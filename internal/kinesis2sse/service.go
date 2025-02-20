package kinesis2sse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/embano1/memlog"
	cfg "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	wk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/worker"
)

const (
	DefaultServicePort = 4444
	DefaultCapacity    = 100_000
	DefaultHost        = ""
)

type ServiceOptions struct {
	// Port is the HTTP port to listen on. Defaults to 4444. Set this to -1 to choose a random port.
	Port int

	// Routes is the set of routes to serve.
	Routes []RouteOptions

	// Logger is the logger to use.
	Logger *slog.Logger // required

	// disableKCL allows disabling the KCL worker, and callers must update the memlog.Log themselves. Only for testing.
	disableKCL bool
}

type RouteOptions struct {
	// Pattern is the pattern to pass to http.ServeMux.HandleFunc.
	Pattern string

	// Capacity is the number of events that will be kept in memory. Defaults to 100,000.
	Capacity int

	// KCLConfig is the Kinesis Client Library (KCL) configuration to use.
	KCLConfig *cfg.KinesisClientLibConfiguration
}

type Service struct {
	cancel func()
	port   int
	routes map[string]route
	logger *slog.Logger // required
	srv    *http.Server
	l      net.Listener
	cond   *sync.Cond
}

type route struct {
	ml   *memlog.Log
	t2o  *Timestamp2Offset
	wrkr *wk.Worker
}

// NewService returns a new Service using the specified KCL configuration.
func NewService(options ServiceOptions) (*Service, error) {
	p := options.Port
	if p == 0 {
		p = DefaultServicePort
	} else if p == -1 {
		p = 0
	}

	handler := http.NewServeMux()

	ctx, cancel := context.WithCancel(context.Background())

	s := &Service{
		cancel: cancel,
		port:   p,
		routes: make(map[string]route),
		logger: options.Logger,
		srv:    &http.Server{ReadHeaderTimeout: 2 * time.Second, Handler: handler},
		l:      nil,
		cond:   &sync.Cond{L: &sync.Mutex{}},
	}

	handler.HandleFunc("/health", func(resp http.ResponseWriter, _ *http.Request) {
		resp.WriteHeader(200)
	})

	for _, routeOptions := range options.Routes {
		capacity := routeOptions.Capacity
		if capacity < 0 {
			return nil, errors.New("capacity must be non-negative")
		}
		if capacity == 0 {
			capacity = DefaultCapacity
		}

		ml, err := memlog.New(ctx, memlog.WithMaxSegmentSize(capacity))
		if err != nil {
			return nil, err
		}

		t2o, err := NewTimestamp2Offset(capacity)
		if err != nil {
			return nil, err
		}

		var wrkr *wk.Worker
		if !options.disableKCL {
			// NOTE(mroberts): We don't support checkpointing. Everything is resumed from `start`.
			kclConfig := routeOptions.KCLConfig.WithLeaseStealing(false)
			wrkr = wk.NewWorker(recordProcessorFactory(ml, t2o, s.logger), kclConfig).
				WithCheckpointer(NewInMemoryCheckpointer(kclConfig.WorkerID, s.logger))
		}

		handler.HandleFunc(routeOptions.Pattern, func(w http.ResponseWriter, r *http.Request) {
			s.handleFunc(ml, t2o, w, r)
		})

		s.routes[routeOptions.Pattern] = route{
			ml:   ml,
			t2o:  t2o,
			wrkr: wrkr,
		}
	}

	return s, nil
}

// Start starts the KCL workers and HTTP server. Only call this method once.
func (s *Service) Start() error {
	// 1. Start all the KCLs workers.
	started := make([]*wk.Worker, 0, len(s.routes))
	for _, r := range s.routes {
		if r.wrkr == nil {
			continue
		}

		if err := r.wrkr.Start(); err != nil {
			// If one of them fails, shut them all down.
			for _, wrkr := range started {
				wrkr.Shutdown()
			}
			return err
		}

		started = append(started, r.wrkr)
	}

	// 2. Acquire a port and broadcast the condition variable.
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", DefaultHost, s.port))
	if err != nil {
		// If this fails, also shutdown the KCL workers.
		for _, wrkr := range started {
			wrkr.Shutdown()
		}
		return err
	}

	s.cond.L.Lock()
	s.l = l
	s.cond.L.Unlock()
	s.cond.Broadcast()

	// 3. Start serving.
	if err := s.srv.Serve(l); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

// Addr blocks until the listener has acquired its port and address.
func (s *Service) Addr() (*net.TCPAddr, error) {
	s.cond.L.Lock()
	for s.l == nil {
		s.cond.Wait()
	}
	defer s.cond.L.Unlock()

	addr, ok := s.l.Addr().(*net.TCPAddr)
	if !ok {
		return nil, errors.New("failed to get *net.TCPAddr")
	}

	return addr, nil
}

// Stop stops the KCL workers and HTTP server. Only call this method once.
func (s *Service) Stop(ctx context.Context) error {
	s.cancel()

	var wait sync.WaitGroup

	// Shutdown KCL workers.
	for _, r := range s.routes {
		wrkr := r.wrkr
		if wrkr != nil {
			wait.Add(1)
			go func() {
				defer wait.Done()
				wrkr.Shutdown()
			}()
		}
	}

	// Shutdown HTTP server.
	err := s.srv.Shutdown(ctx)

	wait.Wait()
	return err
}

func (s *Service) handleFunc(ml *memlog.Log, t2o *Timestamp2Offset, w http.ResponseWriter, r *http.Request) {
	// 1. Ensure we can cast to http.Flusher. Some http.ResponseWriter wrappers can break this functionality.
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("SSE not supported")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// 2. Check the "since" query parameter.
	var timestamp *time.Time
	since := r.URL.Query().Get("since")
	if since != "" {
		// 2.1. First try RFC3339.
		ts, err := time.Parse(time.RFC3339, since)
		if err != nil {
			// 2.2. Then try duration.
			d, err := time.ParseDuration(since)
			if err != nil {
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}
			ts = time.Now().Add(-1 * d)
		}
		timestamp = &ts
	}

	// 3. Start sending SSEs.
	w.Header().Set("Content-Type", "text/event-stream")

	if _, err := fmt.Fprint(w, ":ok\n\n"); err != nil {
		return
	}

	flusher.Flush()

	// Initialize off to the latest offset in the log.
	_, off := ml.Range(r.Context())
	if off < 0 {
		off = 0
	}

	// If "since" was provided, look up an offset by timestamp.
	if timestamp != nil {
		if nearestOff, ok := t2o.NearestOffset(*timestamp); ok {
			off = memlog.Offset(nearestOff)
		}
	}

	stream := ml.Stream(r.Context(), off)

	for {
		if cloudEvent, ok := stream.Next(); ok {
			ssEvent := fmt.Sprintf("data: %s\n\n", string(cloudEvent.Data))

			if _, err := fmt.Fprint(w, ssEvent); err != nil {
				break
			}

			flusher.Flush()
			continue
		}

		break
	}
}

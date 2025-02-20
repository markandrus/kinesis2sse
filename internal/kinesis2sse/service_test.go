package kinesis2sse

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alevinval/sse/pkg/eventsource"
	"github.com/stretchr/testify/require"
)

func TestServiceOneRoute(t *testing.T) {
	r := require.New(t)

	s, err := NewService(ServiceOptions{
		Port: -1,
		Routes: []RouteOptions{
			{
				Pattern: "/",
			},
		},
		disableKCL: true,
		Logger:     slog.New(slog.DiscardHandler),
	})
	r.NoError(err)

	go func() {
		r.NoError(s.Start())
	}()

	addr, err := s.Addr()
	r.NoError(err)
	fmt.Printf("Listening at %s…\n", addr.String())

	var wait sync.WaitGroup
	wait.Add(2)
	events := []string{}

	var es *eventsource.EventSource
	go func() {
		var err error
		es, err = eventsource.New(fmt.Sprintf("http://%s?since=1970-01-01T00%%3A00%%3A00.000Z", addr.String()))
		r.NoError(err)

	Loop:
		for {
			select {
			case event := <-es.MessageEvents():
				if event == nil {
					fmt.Print("Got a nil event…")
					events = append(events, "")
				} else {
					fmt.Printf("[Event] ID: %s\n Name: %s\n Data: %s\n\n", event.ID, event.Name, event.Data)
					events = append(events, event.Data)
				}
				wait.Done()
			case state := <-es.ReadyState():
				fmt.Printf("[ReadyState] %s (err=%v)\n", state.ReadyState, state.Err)
				if state.ReadyState == eventsource.Closed {
					break Loop
				}
			}
		}
	}()

	err = s.routes["/"].t2o.Add(0, time.UnixMilli(0))
	r.NoError(err)
	_, err = s.routes["/"].ml.Write(context.Background(), []byte(`{"hello":"world"}`))
	r.NoError(err)

	err = s.routes["/"].t2o.Add(1, time.UnixMilli(0))
	r.NoError(err)
	_, err = s.routes["/"].ml.Write(context.Background(), []byte(`{"goodbye":"world"}`))
	r.NoError(err)

	wait.Wait()

	fmt.Println("Closing EventSource…")
	es.Close()

	r.Equal([]string{
		`{"hello":"world"}`,
		`{"goodbye":"world"}`,
	}, events)

	fmt.Println("Stopping service…")
	err = s.Stop(context.Background())
	r.NoError(err)
}

func TestServiceTwoRoutes(t *testing.T) {
	r := require.New(t)

	s, err := NewService(ServiceOptions{
		Port: -1,
		Routes: []RouteOptions{
			{
				Pattern: "/foo",
			},
			{
				Pattern: "/bar",
			},
		},
		disableKCL: true,
		Logger:     slog.New(slog.DiscardHandler),
	})
	r.NoError(err)

	go func() {
		r.NoError(s.Start())
	}()

	addr, err := s.Addr()
	r.NoError(err)
	fmt.Printf("Listening at %s…\n", addr.String())

	var wait sync.WaitGroup
	wait.Add(2)
	events := make([][]string, 2)

	ess := make([]*eventsource.EventSource, 2)
	for i, path := range []string{"/foo", "/bar"} {
		i := i
		path := path
		go func() {
			var err error
			es, err := eventsource.New(fmt.Sprintf("http://%s%s?since=1970-01-01T00%%3A00%%3A00.000Z", addr.String(), path))
			r.NoError(err)
			ess[i] = es

		Loop:
			for {
				select {
				case event := <-es.MessageEvents():
					if event == nil {
						fmt.Print("Got a nil event…")
						events[i] = append(events[i], "")
					} else {
						fmt.Printf("%d [Event] ID: %s\n Name: %s\n Data: %s\n\n", i, event.ID, event.Name, event.Data)
						events[i] = append(events[i], event.Data)
					}
					wait.Done()
				case state := <-es.ReadyState():
					fmt.Printf("%d [ReadyState] %s (err=%v)\n", i, state.ReadyState, state.Err)
					if state.ReadyState == eventsource.Closed {
						break Loop
					}
				}
			}
		}()
	}

	err = s.routes["/foo"].t2o.Add(0, time.UnixMilli(0))
	r.NoError(err)
	_, err = s.routes["/foo"].ml.Write(context.Background(), []byte(`{"foo":true}`))
	r.NoError(err)

	err = s.routes["/bar"].t2o.Add(0, time.UnixMilli(0))
	r.NoError(err)
	_, err = s.routes["/bar"].ml.Write(context.Background(), []byte(`{"bar":false}`))
	r.NoError(err)

	wait.Wait()

	fmt.Println("Closing EventSources…")
	for _, es := range ess {
		es.Close()
	}

	r.Equal([][]string{
		{`{"foo":true}`},
		{`{"bar":false}`},
	}, events)

	fmt.Println("Stopping service…")
	err = s.Stop(context.Background())
	r.NoError(err)
}

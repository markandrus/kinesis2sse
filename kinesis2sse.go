package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	cfg "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"

	kinesis2sse "github.com/markandrus/kinesis2sse/internal/kinesis2sse"
)

const (
	defaultPort                    = 4444
	defaultAppNamePrefix           = "kinesis2sse"
	defaultShardSyncIntervalMillis = 1_000
	defaultFailoverTimeMillis      = 300_000
)

var (
	port                    int
	appNamePrefix           string
	shardSyncIntervalMillis int
	failoverTimeMillis      int
	region                  string
	unparsedRoutes          string
	debug                   bool
)

// RouteOptionsCLI are the RouteOptions that can be passed via CLI.
type RouteOptionsCLI struct {
	// Stream is the name of the Kinesis Stream to expose.
	Stream string `json:"stream"`

	// Path is the path to expose the Kinesis Stream at.
	Path string `json:"path"`

	// Capacity is the number of Kinesis Stream events to store in memory.
	Capacity int `json:"capacity"`

	// Start is the position to start reading from the Kinesis Stream. It can be
	//
	// - an ISO 8601 timestamp, like "1970-01-01T00:00:00.000Z".
	// - a duration which will be subtracted from the current timestamp, like "1h" ("1 hour ago").
	// - "TRIM_HORIZON"
	// - "LATEST"
	//
	// Definitions of these can be found in the Amazon Kinesis documentation. Defaults to "LATEST".
	Start string `json:"start"`
}

var rootCmd = &cobra.Command{
	Use: `
  kinesis2sse [flags]`,
	Short: "Expose Kinesis Streams as Server-Sent Events (SSE)",
	Example: `
  kinesis2sse --route {"stream":"my-event-stream","path":"my-events","start":"1h"}`,
	Args: cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, _ []string) error {
		if region == "" {
			return errors.New("region must be specified with the --region flag or AWS_REGION environment variable and cannot be empty")
		}

		if len(unparsedRoutes) == 0 {
			return errors.New("at least one route must be specified with the --route flag")
		}

		if appNamePrefix == "" {
			return errors.New("app name prefix must be specified with the --app-name-prefix flag and cannot be empty")
		}
		appName := appNamePrefix + "-" + uuid.New().String()

		var programLevel = new(slog.LevelVar)
		logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: programLevel}))
		if debug {
			programLevel.Set(slog.LevelDebug)
		}

		logger = logger.With(
			slog.String("service", appNamePrefix),
			slog.String("app", appName))

		kclLogger := kinesis2sse.NewKCLLogger(logger)

		var parsedRoutes []RouteOptionsCLI
		if err := json.Unmarshal([]byte(unparsedRoutes), &parsedRoutes); err != nil {
			return fmt.Errorf("unable to parse routes: %w", err)
		}

		routes := make([]kinesis2sse.RouteOptions, len(parsedRoutes))

		for i, parsedRoute := range parsedRoutes {
			if parsedRoute.Path == "" {
				return fmt.Errorf(`route at index %d has an empty "path"`, i)
			}

			if parsedRoute.Stream == "" {
				return fmt.Errorf(`route at index %d has an empty "stream"`, i)
			}

			// NOTE(mroberts): We should not have such big streams we are subscribed to such that this is a problem.
			maxLeasesForWorker := 100_000
			kclConfig := cfg.NewKinesisClientLibConfig(appName, parsedRoute.Stream, region, appName).
				WithMaxLeasesForWorker(maxLeasesForWorker).
				WithShardSyncIntervalMillis(shardSyncIntervalMillis).
				WithFailoverTimeMillis(failoverTimeMillis).
				WithLogger(kclLogger)

			if parsedRoute.Start == "" || parsedRoute.Start == "LATEST" {
				kclConfig = kclConfig.WithInitialPositionInStream(cfg.LATEST)
			} else if parsedRoute.Start == "TRIM_HORIZON" {
				kclConfig = kclConfig.WithInitialPositionInStream(cfg.TRIM_HORIZON)
			} else if ts, err := time.Parse(time.RFC3339, parsedRoute.Start); err == nil {
				kclConfig = kclConfig.WithTimestampAtInitialPositionInStream(&ts)
			} else if d, err := time.ParseDuration(parsedRoute.Start); err != nil {
				ts := time.Now().Add(-1 * d)
				kclConfig = kclConfig.WithTimestampAtInitialPositionInStream(&ts)
			}

			routes[i] = kinesis2sse.RouteOptions{
				Pattern:   parsedRoute.Path,
				Capacity:  parsedRoute.Capacity,
				KCLConfig: kclConfig,
			}
		}

		s, err := kinesis2sse.NewService(kinesis2sse.ServiceOptions{
			Port:   port,
			Logger: logger,
			Routes: routes,
		})
		if err != nil {
			return err
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		// Signal processing.
		go func() {
			sig := <-sigs
			logger.Info(fmt.Sprintf("Received signal %s. Exiting…\n", sig))
			// NOTE(mroberts): We don't give a timeout here, for simplicity. If stopping takes to long, the user can
			// issue a SIGKILL. This is what Fargate does. By avoiding choosing a timeout, we keep things simple.
			_ = s.Stop(context.Background())
			os.Exit(0)
		}()

		go func() {
			if addr, err := s.Addr(); err == nil {
				logger.Info(fmt.Sprintf("Listening at %s…", addr.String()))
			}
		}()

		return s.Start()
	},
}

func init() {
	rootCmd.PersistentFlags().IntVar(&port, "port", defaultPort, "set the port")
	rootCmd.PersistentFlags().StringVar(&appNamePrefix, "app-name-prefix", defaultAppNamePrefix, "set the app name prefix to which a random suffix will be appended")
	rootCmd.PersistentFlags().IntVar(&shardSyncIntervalMillis, "shard-sync-interval-millis", defaultShardSyncIntervalMillis, "set the shard sync interval in milliseconds, shared by all routes")
	rootCmd.PersistentFlags().IntVar(&failoverTimeMillis, "failover-time-millis", defaultFailoverTimeMillis, "set the failover time in milliseconds, shared by all routes")
	rootCmd.PersistentFlags().StringVar(&region, "region", os.Getenv("AWS_REGION"), "set the region, if not already set by the AWS_REGION environment variable")
	rootCmd.PersistentFlags().StringVar(&unparsedRoutes, "routes", "[]", "set an array of JSON routes")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "enable debug logging")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, err := fmt.Fprintln(os.Stderr, err)
		if err != nil {
			panic(err)
		}
		os.Exit(1)
	}
	os.Exit(0)
}

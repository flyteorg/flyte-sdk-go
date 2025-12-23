package flyte

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	// globalClient is the singleton client instance
	globalClient *client

	// initOnce ensures Initialize is called only once
	initOnce sync.Once

	// initErr stores any initialization error
	initErr error
)

// Initialize sets up the global Flyte client with the provided configuration.
// This function must be called before using Execute() or ExecuteWithContext().
//
// Initialize is thread-safe and will only execute once, even if called multiple times.
// Subsequent calls will return the same error (if any) from the first initialization.
//
// Parameters:
//   - ctx: Context for the initialization (currently unused, reserved for future use)
//   - config: Client configuration including endpoint, credentials, and defaults
//
// Returns:
//   - error if initialization fails (e.g., invalid config, connection failure)
//
// Example:
//
//	config := &flyte.Config{
//	    Endpoint:     "dns:///flyte.example.com:443",
//	    Project:      "my-project",
//	    Domain:       "development",
//	    ClientID:     "your-client-id",
//	    ClientSecret: "your-client-secret",
//	    TokenURL:     "https://auth.example.com/oauth/token",
//	}
//
//	if err := flyte.Initialize(ctx, config); err != nil {
//	    log.Fatalf("Failed to initialize Flyte client: %v", err)
//	}
//
// For local development with insecure connections:
//
//	config := &flyte.Config{
//	    Endpoint: "dns:///localhost:8089",
//	    Project:  "flytesnacks",
//	    Domain:   "development",
//	    Insecure: true,
//	}
//
//	flyte.Initialize(ctx, config)
func Initialize(ctx context.Context, config *Config) error {
	initOnce.Do(func() {
		// Validate config
		if err := config.Validate(); err != nil {
			initErr = fmt.Errorf("invalid config: %w", err)
			return
		}

		// Create gRPC connection options
		var opts []grpc.DialOption

		// Setup TLS or insecure connection
		if config.Insecure {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		} else {
			tlsConfig := &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		}

		// Setup auth interceptor if OAuth credentials are provided
		authInterceptor, err := newAuthInterceptor(config)
		if err != nil {
			initErr = fmt.Errorf("failed to create auth interceptor: %w", err)
			return
		}

		if authInterceptor != nil {
			opts = append(opts,
				grpc.WithUnaryInterceptor(authInterceptor.Unary()),
				grpc.WithStreamInterceptor(authInterceptor.Stream()),
			)
		}

		// Set max message size (100MB for large inputs/outputs)
		// This allows handling of large datasets and models
		opts = append(opts,
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB receive
				grpc.MaxCallSendMsgSize(100*1024*1024), // 100MB send
			),
		)

		// Dial the Flyte server
		conn, err := grpc.Dial(config.Endpoint, opts...)
		if err != nil {
			initErr = fmt.Errorf("failed to connect to %s: %w", config.Endpoint, err)
			return
		}

		// Create client
		globalClient = newClient(conn, config)
	})

	return initErr
}

// getClient returns the global client instance.
// Returns an error if Initialize hasn't been called.
//
// This is an internal function used by Execute() and ExecuteWithContext().
func getClient() (*client, error) {
	if globalClient == nil {
		return nil, fmt.Errorf("flyte client not initialized - call Initialize() first")
	}
	return globalClient, nil
}

// Close closes the global client connection.
// This should be called when the application is shutting down.
//
// Example:
//
//	defer flyte.Close()
//
// Note: After calling Close(), you cannot execute new tasks until
// Initialize() is called again.
func Close() error {
	if globalClient != nil {
		return globalClient.Close()
	}
	return nil
}

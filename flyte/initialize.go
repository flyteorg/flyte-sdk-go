package flyte

import (
	"context"
	"fmt"
	"os"
	"sync"

	client "github.com/unionai/flyte-sdk-go/flyte/client"
	"github.com/unionai/flyte-sdk-go/flyte/client/cache"
)

var (
	mu              sync.RWMutex
	globalClientset *client.RunClientset
	globalConfig    Config
)

// Init connects the SDK to a Flyte control plane. It must be called
// before GetTask or Run. Calling Init again replaces the previous client, so
// embedded applications can re-configure at runtime. Note that re-Init (like
// Close) closes the previous connection, invalidating any RunHandles obtained
// before it — let in-flight Wait/Watch/Outputs calls finish first.
//
// Example:
//
//	err := flyte.Init(ctx, flyte.Config{
//	    Endpoint: "acme.example.com",
//	    Project:  "my-project",
//	    Domain:   "development",
//	})
//
// Authentication defaults to the browser-based PKCE flow. Set APIKey or
// ClientID plus a ClientSecret* field for headless auth, or AuthType to pick
// another flow explicitly.
func Init(ctx context.Context, cfg Config) error {
	resolved, clientCfg, err := cfg.resolve()
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Interactive logins (PKCE, device flow) are persisted in the OS keyring
	// (same layout as the Python SDK, so the two share logins) unless disabled.
	// Headless flows (api-key, client secret, external command) mint tokens on
	// demand and stay in memory, so services embedding the SDK never touch the
	// keyring.
	var tokenCache cache.TokenCache = cache.NewTokenCacheInMemoryProvider()
	interactive := resolved.AuthType == AuthTypePkce || resolved.AuthType == AuthTypeDeviceFlow
	if interactive && !resolved.DisableKeyring {
		tokenCache = cache.NewTokenCacheKeyringProvider(endpointHost(resolved.Endpoint))
	}

	clientset, err := client.NewRunClientsetBuilder().
		WithConfig(clientCfg).
		WithTokenCache(tokenCache).
		Build(ctx)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if globalClientset != nil {
		_ = globalClientset.Close()
	}
	globalClientset = clientset
	globalConfig = resolved
	return nil
}

// InitFromConfig initializes the SDK from a flytectl/uctl-style YAML config
// file. When path is empty, the standard locations are searched (see
// FindConfigPath). This mirrors the Python SDK's flyte.init_from_config().
func InitFromConfig(ctx context.Context, path string) error {
	if path == "" {
		path = FindConfigPath()
		if path == "" {
			return fmt.Errorf("no config file found; searched ./config.yaml, ./.flyte/, git root, $UCTL_CONFIG, $FLYTECTL_CONFIG, ~/.union/, ~/.flyte/")
		}
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		return err
	}
	return Init(ctx, cfg)
}

// InitFromAPIKey initializes the SDK from a platform API key. When apiKey is
// empty, the FLYTE_API_KEY environment variable is used. This mirrors the
// Python SDK's flyte.init_from_api_key().
func InitFromAPIKey(ctx context.Context, apiKey string) error {
	if apiKey == "" {
		apiKey = os.Getenv(EnvAPIKey)
		if apiKey == "" {
			return fmt.Errorf("no api key provided and %s is not set", EnvAPIKey)
		}
	}
	return Init(ctx, Config{APIKey: apiKey})
}

// Close releases the underlying connection. Call it on application shutdown;
// Init may be called again afterwards.
func Close() error {
	mu.Lock()
	defer mu.Unlock()
	if globalClientset == nil {
		return nil
	}
	err := globalClientset.Close()
	globalClientset = nil
	return err
}

// getClientset returns the initialized clientset and resolved config.
func getClientset() (*client.RunClientset, Config, error) {
	mu.RLock()
	defer mu.RUnlock()
	if globalClientset == nil {
		return nil, Config{}, fmt.Errorf("flyte client not initialized - call flyte.Init() first")
	}
	return globalClientset, globalConfig, nil
}

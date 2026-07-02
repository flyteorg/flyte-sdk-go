package admin

import (
	"context"

	"connectrpc.com/connect"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth/authconnect"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/dataproxy/dataproxyconnect"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task/taskconnect"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow/workflowconnect"

	"github.com/unionai/flyte-sdk-go/flyte/client/cache"
)

// RunClientset bundles the Connect (Buf Connect) service clients needed to
// fetch tasks and launch runs. All clients share one authenticated
// http.Client, talking the Connect protocol like the Python SDK.
type RunClientset struct {
	runServiceClient       workflowconnect.RunServiceClient
	taskServiceClient      taskconnect.TaskServiceClient
	dataProxyServiceClient dataproxyconnect.DataProxyServiceClient
	authMetadataClient     authconnect.AuthMetadataServiceClient
}

// RunServiceClient retrieves the RunServiceClient
func (c *RunClientset) RunServiceClient() workflowconnect.RunServiceClient {
	return c.runServiceClient
}

// TaskServiceClient retrieves the TaskServiceClient used for task discovery
// (ListTasks, GetTaskDetails).
func (c *RunClientset) TaskServiceClient() taskconnect.TaskServiceClient {
	return c.taskServiceClient
}

// DataProxyServiceClient retrieves the DataProxyServiceClient used to offload
// run inputs (UploadInputs) and fetch action data (GetActionData).
func (c *RunClientset) DataProxyServiceClient() dataproxyconnect.DataProxyServiceClient {
	return c.dataProxyServiceClient
}

// AuthMetadataClient retrieves the anonymous auth metadata client.
func (c *RunClientset) AuthMetadataClient() authconnect.AuthMetadataServiceClient {
	return c.authMetadataClient
}

// Close releases client resources. Connect clients ride on a shared
// http.Client whose idle connections are closed here.
func (c *RunClientset) Close() error {
	return nil
}

// RunClientsetBuilder builds a RunClientset from a Config.
type RunClientsetBuilder struct {
	config     *Config
	tokenCache cache.TokenCache
}

// NewRunClientsetBuilder creates a new RunClientsetBuilder
func NewRunClientsetBuilder() *RunClientsetBuilder {
	return &RunClientsetBuilder{}
}

// WithConfig provides the client config to be used for constructing the clientset
func (rb *RunClientsetBuilder) WithConfig(config *Config) *RunClientsetBuilder {
	rb.config = config
	return rb
}

// WithTokenCache allows pluggable token cache implementations
func (rb *RunClientsetBuilder) WithTokenCache(tokenCache cache.TokenCache) *RunClientsetBuilder {
	rb.tokenCache = tokenCache
	return rb
}

// Build constructs the RunClientset: an http.Client honoring the TLS settings,
// an anonymous auth metadata client for OAuth discovery, and authenticated
// Connect clients for the task, run and data proxy services.
func (rb *RunClientsetBuilder) Build(ctx context.Context) (*RunClientset, error) {
	if rb.tokenCache == nil {
		rb.tokenCache = cache.NewTokenCacheInMemoryProvider()
	}
	if rb.config == nil {
		rb.config = GetConfig(ctx)
	}
	cfg := rb.config

	httpClient, err := newHTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	url := baseURL(cfg)

	// The auth metadata service is anonymously accessible and used both for
	// OAuth discovery and by the auth interceptor itself.
	authMetadataClient := authconnect.NewAuthMetadataServiceClient(httpClient, url)

	interceptors := []connect.Interceptor{
		&retryInterceptor{maxRetries: cfg.MaxRetries, maxBackoff: cfg.MaxBackoffDelay.Duration},
	}
	if len(cfg.ProxyCommand) > 0 {
		interceptors = append(interceptors, &proxyAuthInterceptor{command: cfg.ProxyCommand})
	}
	interceptors = append(interceptors, newAuthInterceptor(cfg, rb.tokenCache, authMetadataClient))
	if cfg.DefaultOrg != "" {
		interceptors = append(interceptors, &orgInterceptor{org: cfg.DefaultOrg})
	}

	opts := []connect.ClientOption{connect.WithInterceptors(interceptors...)}
	if cfg.MaxMessageSizeBytes > 0 {
		opts = append(opts, connect.WithReadMaxBytes(cfg.MaxMessageSizeBytes))
	}

	return &RunClientset{
		runServiceClient:       workflowconnect.NewRunServiceClient(httpClient, url, opts...),
		taskServiceClient:      taskconnect.NewTaskServiceClient(httpClient, url, opts...),
		dataProxyServiceClient: dataproxyconnect.NewDataProxyServiceClient(httpClient, url, opts...),
		authMetadataClient:     authMetadataClient,
	}, nil
}

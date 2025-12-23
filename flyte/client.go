package flyte

import (
	"context"
	"fmt"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
	"google.golang.org/grpc"
)

// client is the internal implementation of the Client interface.
// It wraps the gRPC connection and service clients.
//
// Users should not create client instances directly.
// Use Initialize() to set up the global client.
type client struct {
	conn       *grpc.ClientConn
	runService workflow.RunServiceClient
	config     *Config
}

// newClient creates a new Flyte client
func newClient(conn *grpc.ClientConn, config *Config) *client {
	return &client{
		conn:       conn,
		runService: workflow.NewRunServiceClient(conn),
		config:     config,
	}
}

// Execute implements Client interface
func (c *client) Execute(ctx context.Context, task Task, inputs map[string]interface{}) (Run, error) {
	return c.createRun(ctx, task, inputs, nil)
}

// ExecuteWithContext implements Client interface
func (c *client) ExecuteWithContext(runCtx RunContextBuilder, task Task, inputs map[string]interface{}) (Run, error) {
	return c.createRun(runCtx.GetContext(), task, inputs, runCtx)
}

// GetConfig implements Client interface
func (c *client) GetConfig() *Config {
	return c.config
}

// Close implements Client interface
// It closes the underlying gRPC connection
func (c *client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// buildConsoleURL constructs the Flyte console URL for a run.
// This is a best-effort construction and may need adjustment based on
// the actual Flyte deployment configuration.
func buildConsoleURL(config *Config, run *workflow.Run) string {
	if run == nil || run.Action == nil || run.Action.Id == nil || run.Action.Id.Run == nil {
		return ""
	}

	runID := run.Action.Id.Run

	// Extract host from endpoint
	// For endpoints like "dns:///localhost:8089" or "localhost:8089"
	endpoint := config.Endpoint

	// Remove "dns:///" prefix if present
	if len(endpoint) > 7 && endpoint[:7] == "dns:///" {
		endpoint = endpoint[7:]
	}

	// Remove port if present (simplified parsing)
	// In production, this would use proper URL parsing
	host := endpoint
	if colonIdx := len(endpoint) - 1; colonIdx > 0 {
		for i := len(endpoint) - 1; i >= 0; i-- {
			if endpoint[i] == ':' {
				host = endpoint[:i]
				break
			}
		}
	}

	// Construct console URL
	// Format: https://{host}/console/projects/{project}/domains/{domain}/executions/{run-name}
	return fmt.Sprintf("https://%s/console/projects/%s/domains/%s/executions/%s",
		host, runID.Project, runID.Domain, runID.Name)
}

// Compile-time check that client implements Client interface
var _ Client = (*client)(nil)

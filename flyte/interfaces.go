package flyte

import (
	"context"
	"time"
)

// Task represents any executable task in Flyte.
// This interface allows for different task implementations (remote tasks, local tasks, etc.)
// to be used interchangeably throughout the SDK.
//
// Example implementations:
//   - remote.TaskRef: References a pre-deployed task on a Flyte cluster
//   - (Future) local.Task: A locally defined task to be executed or deployed
type Task interface {
	// GetName returns the task's name
	GetName() string

	// GetVersion returns the task's version
	GetVersion() string

	// GetProject returns the project containing this task
	GetProject() string

	// GetDomain returns the domain (e.g., "development", "production")
	GetDomain() string
}

// Run represents an execution of a task.
// A Run provides methods to monitor progress, wait for completion, and retrieve results.
//
// Example usage:
//
//	run, err := flyte.Execute(ctx, task, inputs)
//	if err != nil {
//	    return err
//	}
//
//	// Monitor progress
//	fmt.Printf("Run URL: %s\n", run.GetURL())
//
//	// Wait for completion
//	if err := run.Wait(ctx); err != nil {
//	    return err
//	}
//
//	// Get outputs
//	outputs, err := run.GetOutputs(ctx)
type Run interface {
	// GetName returns the unique name/ID of this run
	GetName() string

	// GetURL returns the web console URL for this run
	// Returns empty string for local runs
	GetURL() string

	// GetPhase returns the current execution phase
	// Possible values: QUEUED, RUNNING, SUCCEEDED, FAILED, etc.
	GetPhase() string

	// Wait blocks until the run reaches a terminal state (success or failure)
	// Returns an error if the run fails or is aborted
	Wait(ctx context.Context) error

	// Watch returns a channel that streams run updates
	// The channel is closed when the run completes
	Watch(ctx context.Context) (<-chan *RunUpdate, error)

	// GetOutputs retrieves the run's output values
	// Waits for completion if the run is still running
	GetOutputs(ctx context.Context) (map[string]interface{}, error)
}

// RunUpdate represents a status update from a running execution
type RunUpdate struct {
	// Phase is the current execution phase
	Phase string

	// Timestamp is when this update occurred
	Timestamp time.Time

	// Error contains error information if the run failed
	Error string
}

// RunContextBuilder provides a fluent interface for configuring run execution.
// This pattern allows for clean, readable configuration and easy extension in future phases.
//
// Example usage:
//
//	runCtx := flyte.NewRunContext(ctx).
//	    WithProject("my-project").
//	    WithDomain("staging").
//	    WithRunName("custom-run-001").
//	    WithLabel("team", "data-eng").
//	    WithOverwriteCache(true)
//
//	run, err := flyte.ExecuteWithContext(runCtx, task, inputs)
type RunContextBuilder interface {
	// WithProject overrides the project for this run
	WithProject(project string) RunContextBuilder

	// WithDomain overrides the domain for this run
	WithDomain(domain string) RunContextBuilder

	// WithRunName sets an explicit run name (otherwise auto-generated)
	WithRunName(name string) RunContextBuilder

	// WithLabel adds a label (key-value pair) to the run
	WithLabel(key, value string) RunContextBuilder

	// WithAnnotation adds an annotation to the run
	WithAnnotation(key, value string) RunContextBuilder

	// WithEnvVar adds an environment variable for task execution
	WithEnvVar(key, value string) RunContextBuilder

	// WithOverwriteCache forces re-execution even if cached results exist
	WithOverwriteCache(overwrite bool) RunContextBuilder

	// GetContext returns the underlying context.Context
	GetContext() context.Context

	// GetProject returns the project override (empty if not set)
	GetProject() string

	// GetDomain returns the domain override (empty if not set)
	GetDomain() string

	// GetRunName returns the explicit run name (empty if auto-generated)
	GetRunName() string

	// GetLabels returns all labels
	GetLabels() map[string]string

	// GetAnnotations returns all annotations
	GetAnnotations() map[string]string

	// GetEnvVars returns all environment variables
	GetEnvVars() map[string]string

	// GetOverwriteCache returns whether cache should be overwritten
	GetOverwriteCache() bool
}

// Client represents a connection to a Flyte cluster.
// This interface abstracts the gRPC client, making the code more testable
// and allowing for mock implementations in tests.
//
// Internal interface - users don't need to interact with this directly.
// Use Initialize() and the Execute() functions instead.
type Client interface {
	// Execute creates a run for the given task
	Execute(ctx context.Context, task Task, inputs map[string]interface{}) (Run, error)

	// ExecuteWithContext creates a run with custom configuration
	ExecuteWithContext(runCtx RunContextBuilder, task Task, inputs map[string]interface{}) (Run, error)

	// GetConfig returns the client configuration
	GetConfig() *Config

	// Close closes the client connection
	Close() error
}

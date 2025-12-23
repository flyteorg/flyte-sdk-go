package flyte

import "context"

// RunContext implements RunContextBuilder and provides configuration for run execution.
// It uses the builder pattern for clean, readable configuration.
//
// Example usage:
//
//	runCtx := flyte.NewRunContext(ctx).
//	    WithProject("analytics").
//	    WithDomain("staging").
//	    WithRunName("daily-processing-001").
//	    WithLabel("team", "data-eng").
//	    WithLabel("priority", "high").
//	    WithAnnotation("owner", "alice@example.com").
//	    WithEnvVar("LOG_LEVEL", "DEBUG").
//	    WithOverwriteCache(true)
//
//	run, err := flyte.ExecuteWithContext(runCtx, task, inputs)
//
// Thread-safety: RunContext is not thread-safe. Create separate instances
// for concurrent executions.
type RunContext struct {
	ctx context.Context

	// Project override (if empty, uses Task's project)
	project string

	// Domain override (if empty, uses Task's domain)
	domain string

	// RunName is the explicit run name (if empty, auto-generated)
	runName string

	// Labels to attach to the run
	labels map[string]string

	// Annotations to attach to the run
	annotations map[string]string

	// EnvVars are environment variables to set
	envVars map[string]string

	// OverwriteCache forces re-execution even if cached results exist
	overwriteCache bool
}

// NewRunContext creates a new RunContext with default values.
// The provided context is used for cancellation and timeouts.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
//	defer cancel()
//	runCtx := flyte.NewRunContext(ctx)
func NewRunContext(ctx context.Context) *RunContext {
	return &RunContext{
		ctx:         ctx,
		labels:      make(map[string]string),
		annotations: make(map[string]string),
		envVars:     make(map[string]string),
	}
}

// WithProject sets the project override for this run.
// If not set, the task's project will be used.
func (r *RunContext) WithProject(project string) RunContextBuilder {
	r.project = project
	return r
}

// WithDomain sets the domain override for this run.
// If not set, the task's domain will be used.
//
// Common domains: "development", "staging", "production"
func (r *RunContext) WithDomain(domain string) RunContextBuilder {
	r.domain = domain
	return r
}

// WithRunName sets an explicit run name.
// If not set, a unique name will be auto-generated.
//
// Note: Run names must be unique within a project/domain.
// If a run with this name already exists, execution will fail.
func (r *RunContext) WithRunName(name string) RunContextBuilder {
	r.runName = name
	return r
}

// WithLabel adds a label (key-value pair) to the run.
// Labels can be used for filtering and organizing runs.
//
// Example:
//
//	runCtx.WithLabel("environment", "staging").
//	        WithLabel("team", "data-eng")
func (r *RunContext) WithLabel(key, value string) RunContextBuilder {
	r.labels[key] = value
	return r
}

// WithAnnotation adds an annotation to the run.
// Annotations are similar to labels but are typically used for
// longer-form metadata (e.g., git commit, user email).
//
// Example:
//
//	runCtx.WithAnnotation("git-commit", "abc123def").
//	        WithAnnotation("triggered-by", "alice@example.com")
func (r *RunContext) WithAnnotation(key, value string) RunContextBuilder {
	r.annotations[key] = value
	return r
}

// WithEnvVar adds an environment variable for task execution.
// These variables will be available to the task at runtime.
//
// Example:
//
//	runCtx.WithEnvVar("LOG_LEVEL", "DEBUG").
//	        WithEnvVar("API_ENDPOINT", "https://api.example.com")
func (r *RunContext) WithEnvVar(key, value string) RunContextBuilder {
	r.envVars[key] = value
	return r
}

// WithOverwriteCache controls whether to force re-execution.
// When true, cached results are ignored and the task is re-executed.
//
// Use cases:
//   - Forcing fresh data processing
//   - Debugging cached task behavior
//   - Invalidating stale cache
func (r *RunContext) WithOverwriteCache(overwrite bool) RunContextBuilder {
	r.overwriteCache = overwrite
	return r
}

// GetContext returns the underlying context.Context
func (r *RunContext) GetContext() context.Context {
	return r.ctx
}

// GetProject returns the project override (empty if not set)
func (r *RunContext) GetProject() string {
	return r.project
}

// GetDomain returns the domain override (empty if not set)
func (r *RunContext) GetDomain() string {
	return r.domain
}

// GetRunName returns the explicit run name (empty if auto-generated)
func (r *RunContext) GetRunName() string {
	return r.runName
}

// GetLabels returns all labels
func (r *RunContext) GetLabels() map[string]string {
	return r.labels
}

// GetAnnotations returns all annotations
func (r *RunContext) GetAnnotations() map[string]string {
	return r.annotations
}

// GetEnvVars returns all environment variables
func (r *RunContext) GetEnvVars() map[string]string {
	return r.envVars
}

// GetOverwriteCache returns whether cache should be overwritten
func (r *RunContext) GetOverwriteCache() bool {
	return r.overwriteCache
}

// Compile-time check that RunContext implements RunContextBuilder
var _ RunContextBuilder = (*RunContext)(nil)

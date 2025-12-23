# Flyte Minimal Go SDK

A minimal, interface-based Go SDK for Flyte 2.0 focused on remote task execution. This SDK allows you to reference and execute tasks deployed to a Flyte cluster, monitor progress, and retrieve outputs - all through clean, extensible interfaces.

## Features

- **Interface-Based Architecture**: Clean abstractions (`Task`, `Run`, `RunContextBuilder`) for extensibility
- **Remote Task Execution**: Execute tasks deployed on Flyte without importing their code
- **Real-Time Monitoring**: Stream run updates or block until completion
- **Output Retrieval**: Fetch task outputs with automatic type conversion
- **OAuth Authentication**: Built-in OAuth2 ClientSecret flow
- **Custom Run Configuration**: Builder pattern for labels, annotations, environment variables, etc.
- **Thread-Safe**: Safe for concurrent execution
- **Future-Proof**: Designed to evolve towards full SDK (see [CLAUDE.md](CLAUDE.md))

## Installation

```bash
go get flyte-go-sdk
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "flyte-go-sdk/flyte"
    "flyte-go-sdk/flyte/remote"
)

func main() {
    ctx := context.Background()

    // Initialize Flyte client
    config := &flyte.Config{
        Endpoint: "dns:///localhost:8089",
        Insecure: true,  // For local development
        Project:  "flytesnacks",
        Domain:   "development",
    }

    if err := flyte.Initialize(ctx, config); err != nil {
        log.Fatal(err)
    }
    defer flyte.Close()

    // Reference a remote task (implements flyte.Task interface)
    task := &remote.TaskRef{
        Name:    "my_task",
        Version: "v1.0",
        Project: "flytesnacks",
        Domain:  "development",
    }

    // Execute the task (returns flyte.Run interface)
    run, err := flyte.Execute(ctx, task, map[string]interface{}{
        "input_a": 42,
        "input_b": "hello world",
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Started run: %s\n", run.GetName())
    fmt.Printf("Track at: %s\n", run.GetURL())

    // Wait for completion
    if err := run.Wait(ctx); err != nil {
        log.Fatalf("Run failed: %v", err)
    }

    // Get outputs
    outputs, err := run.GetOutputs(ctx)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Outputs: %v\n", outputs)
}
```

## Core Interfaces

The SDK is built around these key interfaces for maximum extensibility:

### Task Interface

Represents any executable task (remote or local):

```go
type Task interface {
    GetName() string
    GetVersion() string
    GetProject() string
    GetDomain() string
}
```

**Implementations:**
- `remote.TaskRef` - References a deployed task

### Run Interface

Represents an execution with monitoring capabilities:

```go
type Run interface {
    GetName() string
    GetURL() string
    GetPhase() string
    Wait(ctx context.Context) error
    Watch(ctx context.Context) (<-chan *RunUpdate, error)
    GetOutputs(ctx context.Context) (map[string]interface{}, error)
}
```

### RunContextBuilder Interface

Fluent API for configuring run execution:

```go
type RunContextBuilder interface {
    WithProject(string) RunContextBuilder
    WithDomain(string) RunContextBuilder
    WithRunName(string) RunContextBuilder
    WithLabel(key, value string) RunContextBuilder
    WithAnnotation(key, value string) RunContextBuilder
    WithEnvVar(key, value string) RunContextBuilder
    WithOverwriteCache(bool) RunContextBuilder
    // ... getters
}
```

## Usage Examples

### Basic Execution

```go
// Create task reference
task := &remote.TaskRef{
    Name:    "data_processing",
    Version: "v1.0.0",
    Project: "analytics",
    Domain:  "production",
}

// Execute and wait
run, err := flyte.Execute(ctx, task, map[string]interface{}{
    "input_path": "/data/input.csv",
    "threshold":  0.95,
})
if err != nil {
    return err
}

if err := run.Wait(ctx); err != nil {
    return fmt.Errorf("run failed: %w", err)
}

outputs, _ := run.GetOutputs(ctx)
fmt.Printf("Result: %s\n", outputs["result_path"])
```

### Custom Configuration

Use the builder pattern for advanced configuration:

```go
runCtx := flyte.NewRunContext(ctx).
    WithProject("my-project").
    WithDomain("staging").
    WithRunName("daily-processing-001").
    WithLabel("team", "data-eng").
    WithLabel("priority", "high").
    WithAnnotation("owner", "alice@example.com").
    WithEnvVar("LOG_LEVEL", "DEBUG").
    WithOverwriteCache(true)

run, err := flyte.ExecuteWithContext(runCtx, task, inputs)
```

### Streaming Updates

Monitor progress in real-time:

```go
run, _ := flyte.Execute(ctx, task, inputs)

updateChan, err := run.Watch(ctx)
if err != nil {
    return err
}

for update := range updateChan {
    if update.Error != "" {
        log.Printf("Error: %s", update.Error)
        break
    }
    log.Printf("[%v] Phase: %s", update.Timestamp, update.Phase)
}

// Channel closes automatically when run completes
outputs, _ := run.GetOutputs(ctx)
```

### Checking Phase

```go
phase := run.GetPhase()
fmt.Printf("Current phase: %s\n", phase)

// Common phases:
// - ACTION_PHASE_QUEUED
// - ACTION_PHASE_RUNNING
// - ACTION_PHASE_SUCCEEDED
// - ACTION_PHASE_FAILED
```

## Configuration

### flyte.Config

```go
type Config struct {
    // Connection
    Endpoint string   // gRPC endpoint (e.g., "dns:///flyte.example.com:443")
    Insecure bool     // Use insecure connection (local dev only)
    Org      string   // Organization name

    // Defaults
    Project string    // Default project
    Domain  string    // Default domain (e.g., "development", "production")

    // OAuth2 ClientSecret Authentication
    ClientID     string   // OAuth client ID
    ClientSecret string   // OAuth client secret
    TokenURL     string   // Token endpoint URL
    Scopes       []string // OAuth scopes
}
```

### Example Configurations

**Local Development:**

```go
config := &flyte.Config{
    Endpoint: "dns:///localhost:8089",
    Insecure: true,
    Project:  "flytesnacks",
    Domain:   "development",
}
```

**Production with OAuth:**

```go
config := &flyte.Config{
    Endpoint:     "dns:///flyte.example.com:443",
    Project:      "production-analytics",
    Domain:       "production",
    ClientID:     os.Getenv("FLYTE_CLIENT_ID"),
    ClientSecret: os.Getenv("FLYTE_CLIENT_SECRET"),
    TokenURL:     "https://auth.example.com/oauth/token",
}
```

## API Reference

### Main Functions

#### Initialize

```go
func Initialize(ctx context.Context, config *Config) error
```

Sets up the global Flyte client. Must be called before executing tasks.
Thread-safe (uses `sync.Once`).

#### Execute

```go
func Execute(ctx context.Context, task Task, inputs map[string]interface{}) (Run, error)
```

Executes a task with default configuration. Returns a `Run` interface for monitoring.

#### ExecuteWithContext

```go
func ExecuteWithContext(runCtx RunContextBuilder, task Task, inputs map[string]interface{}) (Run, error)
```

Executes a task with custom configuration (project override, labels, etc.).

#### Close

```go
func Close() error
```

Closes the global client connection. Should be called when shutting down.

### RunContext Methods

#### NewRunContext

```go
func NewRunContext(ctx context.Context) *RunContext
```

Creates a new RunContext builder with default values.

#### Builder Methods

All builder methods return `RunContextBuilder` for chaining:

- `WithProject(project string)` - Override project
- `WithDomain(domain string)` - Override domain
- `WithRunName(name string)` - Set explicit run name
- `WithLabel(key, value string)` - Add label
- `WithAnnotation(key, value string)` - Add annotation
- `WithEnvVar(key, value string)` - Add environment variable
- `WithOverwriteCache(overwrite bool)` - Force re-execution

### Run Methods

Methods on the `Run` interface:

- `GetName() string` - Get run name/ID
- `GetURL() string` - Get web console URL
- `GetPhase() string` - Get current execution phase
- `Wait(ctx) error` - Block until completion
- `Watch(ctx) (<-chan *RunUpdate, error)` - Stream updates
- `GetOutputs(ctx) (map[string]interface{}, error)` - Retrieve outputs

### remote.TaskRef

```go
type TaskRef struct {
    Name    string  // Task name
    Version string  // Task version
    Project string  // Project containing task
    Domain  string  // Domain (e.g., "development", "production")
}
```

Implements the `flyte.Task` interface for referencing deployed tasks.

## Type Support

Currently supports basic Go types for inputs/outputs:

- `int`, `int32`, `int64`
- `float32`, `float64`
- `string`
- `bool`

**Future phases** will add support for:
- Structs
- Maps
- Slices
- Custom type plugins

## Thread Safety

The SDK is designed for concurrent use:

- ✅ `Initialize()` is thread-safe (uses `sync.Once`)
- ✅ Multiple `Execute()` calls can run concurrently
- ✅ `Run` methods are safe for concurrent access
- ⚠️ `RunContext` instances should not be shared across goroutines

## Error Handling

Errors follow Go conventions:

```go
// Initialization errors
if err := flyte.Initialize(ctx, config); err != nil {
    log.Fatalf("Failed to initialize: %v", err)
}

// Execution errors
run, err := flyte.Execute(ctx, task, inputs)
if err != nil {
    return fmt.Errorf("failed to create run: %w", err)
}

// Completion errors (failed/aborted/timed out)
if err := run.Wait(ctx); err != nil {
    return fmt.Errorf("run failed: %w", err)
}

// Output retrieval errors
outputs, err := run.GetOutputs(ctx)
if err != nil {
    return fmt.Errorf("failed to get outputs: %w", err)
}
```

## Examples

See [example/main.go](example/main.go) for complete working examples:

1. **Example 1**: Simple execution with Wait
2. **Example 2**: Streaming updates with Watch
3. **Example 3**: Custom configuration with RunContext

Run examples:

```bash
# Set environment variables
export FLYTE_ENDPOINT="dns:///localhost:8089"
export FLYTE_PROJECT="flytesnacks"
export FLYTE_DOMAIN="development"
export EXAMPLE_TASK_NAME="your_task"
export EXAMPLE_TASK_VERSION="v1.0"

# Run examples
go run example/main.go
```

## Architecture

The SDK uses an interface-based architecture for extensibility:

```
┌─────────────────────────────────────┐
│          User Code                  │
│  (Execute, ExecuteWithContext)      │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│     Core Interfaces                 │
│  • Task                             │
│  • Run                              │
│  • RunContextBuilder                │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│    Implementations                  │
│  • remote.TaskRef (Task)            │
│  • runRef (Run)                     │
│  • RunContext (RunContextBuilder)   │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│      Internal Client                │
│  • gRPC communication               │
│  • OAuth2 authentication            │
│  • Type serialization               │
└─────────────────────────────────────┘
```

This design allows for:
- Easy mocking in tests
- Future local task implementations
- Workflow composition (Phase 3)
- Deployment capabilities (Phase 4)

## Roadmap

This minimal SDK will evolve through multiple phases:

- ✅ **Phase 0** (Current): Remote task execution
- 🔄 **Phase 1**: Task details, auto-versioning, resource overrides
- 📅 **Phase 2**: Local task authoring and registration
- 📅 **Phase 3**: Workflow composition with futures
- 📅 **Phase 4**: Task/workflow deployment
- 📅 **Phase 5**: Launch plans, schedules, advanced features

See [CLAUDE.md](CLAUDE.md) for the complete implementation blueprint.

## Contributing

This SDK follows the architecture outlined in CLAUDE.md. When adding features:

1. Define interfaces in `flyte/interfaces.go`
2. Implement interfaces with proper documentation
3. Add examples demonstrating usage
4. Maintain backward compatibility

## License

[Add your license here]

## Resources

- [Flyte Documentation](https://docs.flyte.org)
- [Implementation Blueprint](CLAUDE.md)
- [Examples](example/main.go)

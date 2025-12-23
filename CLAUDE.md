# Flyte SDK Go - Complete Implementation Blueprint

This document outlines the complete evolution path for flyte-sdk-go-min from its current minimal remote execution capability to a full-featured Flyte SDK matching the Python SDK (flyte-sdk) and full Go SDK (flyte-sdk-go).

---

## Table of Contents
1. [Current State](#current-state)
2. [Architecture Overview](#architecture-overview)
3. [Implementation Phases](#implementation-phases)
4. [Phase 0: Current Implementation](#phase-0-current-implementation-complete)
5. [Phase 1: Enhanced Remote Task Features](#phase-1-enhanced-remote-task-features)
6. [Phase 2: Local Task Authoring](#phase-2-local-task-authoring)
7. [Phase 3: Workflow Composition](#phase-3-workflow-composition)
8. [Phase 4: Apps and Deployment](#phase-4-apps-and-deployment)
9. [Phase 5: Advanced Features](#phase-5-advanced-features)
10. [API Design Principles](#api-design-principles)
11. [Reference Implementations](#reference-implementations)

---

## Current State

### What's Implemented (Phase 0)
flyte-sdk-go-min currently provides minimal remote task execution:
- Remote task references via `remote.TaskRef`
- Run creation with `flyte.Run()` and `flyte.RunWithContext()`
- Client initialization with `flyte.Initialize()`
- RunContext for customizing execution (project, domain, labels, etc.)
- Real-time progress monitoring via `Wait()` and `Watch()`
- Output retrieval via `Outputs()`
- OAuth2 client credentials authentication
- gRPC client with TLS/insecure modes
- Basic type serialization (int, float, string, bool)

### Package Structure (Current)
```
flyte-sdk-go-min/
├── flyte/
│   ├── types.go          # Config, type conversions
│   ├── initialize.go     # Client initialization
│   ├── auth.go           # OAuth2 authentication
│   ├── client.go         # gRPC client wrapper
│   ├── context.go        # RunContext
│   ├── run.go            # RunRef and run operations
│   └── remote/
│       └── taskref.go    # Remote task reference
└── example/
    └── main.go           # Usage examples
```

---

## Architecture Overview

The Flyte SDK architecture follows a layered design inspired by both the Python SDK and the full Go SDK:

### Layer 1: Client & Communication
- **gRPC Client**: Low-level communication with Flyte control plane
- **Authentication**: OAuth2, API key, device flow, PKCE
- **Services**: RunService, TaskService, StateService, QueueService

### Layer 2: Remote Execution
- **TaskRef**: Reference to pre-deployed tasks
- **Run/RunContext**: Remote execution orchestration
- **Monitoring**: Watch, Wait, stream updates

### Layer 3: Local Task Authoring
- **Task Registration**: Decorator/function-based task definition
- **Task Environments**: Container images, resources
- **Type System**: Input/output serialization
- **Local Execution**: In-process task testing

### Layer 4: Workflow Composition
- **Workflow DSL**: Composing tasks into workflows
- **Futures**: Async task-to-task communication
- **Control Flow**: Conditionals, loops, dynamic tasks
- **Data Flow**: Input/output passing between tasks

### Layer 5: Deployment & Apps
- **Serialization**: Task/workflow to protobuf
- **Registration**: Upload to Flyte cluster
- **Versioning**: Code bundle management
- **Apps**: Complete application definition

---

## Implementation Phases

### Phase 0: Current Implementation ✅ COMPLETE
- [x] Remote task references
- [x] Basic run creation and monitoring
- [x] OAuth2 authentication
- [x] RunContext for configuration
- [x] Watch and Wait APIs
- [x] Output retrieval
- [x] Basic type serialization

### Phase 1: Enhanced Remote Task Features
- [ ] TaskDetails fetching (get task metadata)
- [ ] Auto-versioning (latest, current)
- [ ] Task overrides (resources, retries, cache)
- [ ] Advanced type support (structs, maps, lists)
- [ ] Run listing and filtering
- [ ] Run abortion
- [ ] Logs streaming

### Phase 2: Local Task Authoring
- [ ] Task registration API
- [ ] Task decorators/functions
- [ ] TaskEnvironment definition
- [ ] Local task execution
- [ ] Type inference and validation
- [ ] Resource specifications
- [ ] Retry and timeout policies
- [ ] Caching configuration

### Phase 3: Workflow Composition
- [ ] Workflow registration
- [ ] Task-to-task invocation (Futures)
- [ ] Conditional execution
- [ ] Map/dynamic tasks
- [ ] Subworkflows
- [ ] Error handling
- [ ] Data dependency management

### Phase 4: Apps and Deployment
- [ ] Task/workflow serialization
- [ ] Code bundle creation
- [ ] Image building
- [ ] Registration to cluster
- [ ] Version management
- [ ] App composition

### Phase 5: Advanced Features
- [ ] Launch plans
- [ ] Schedules and triggers
- [ ] Secrets management
- [ ] Custom type plugins
- [ ] Agents and sensors
- [ ] Notifications

---

## Phase 0: Current Implementation (COMPLETE)

### API Surface

#### Initialization
```go
import "github.com/flyteorg/flyte-sdk-go-min/flyte"

config := &flyte.Config{
    Endpoint:     "dns:///flyte.example.com:443",
    Project:      "my-project",
    Domain:       "development",
    ClientID:     "client-id",
    ClientSecret: "client-secret",
    TokenURL:     "https://auth.example.com/oauth/token",
}

ctx := context.Background()
err := flyte.Initialize(ctx, config)
```

#### Remote Task Execution
```go
import "github.com/flyteorg/flyte-sdk-go-min/flyte/remote"

// Create task reference
task := &remote.TaskRef{
    Name:    "data_processing",
    Version: "v1.0.0",
    Project: "analytics",
    Domain:  "production",
}

// Simple run
run, err := flyte.Run(ctx, task, map[string]interface{}{
    "input_path": "/data/input.csv",
    "threshold":  0.95,
})

// Wait for completion
err = run.Wait(ctx)
fmt.Println("Run URL:", run.URL)

// Get outputs
outputs, err := run.Outputs(ctx)
```

#### RunContext Customization
```go
runCtx := flyte.NewRunContext(ctx).
    WithProject("my-project").
    WithDomain("staging").
    WithRunName("my-custom-run-001").
    WithLabel("team", "data-eng").
    WithAnnotation("owner", "alice@example.com").
    WithEnvVar("LOG_LEVEL", "DEBUG").
    WithOverwriteCache(true)

run, err := flyte.RunWithContext(runCtx, task, inputs)
```

#### Progress Monitoring
```go
// Blocking wait
err := run.Wait(ctx)

// Streaming updates
updateChan := run.Watch(ctx)
for update := range updateChan {
    fmt.Printf("Phase: %s, Timestamp: %v\n", update.Phase, update.Timestamp)
    if update.Error != "" {
        fmt.Printf("Error: %s\n", update.Error)
    }
}
```

### File Structure
See "Package Structure (Current)" section above.

### Key Types
- `Config`: Client configuration
- `TaskRef`: Remote task reference
- `RunContext`: Execution context with overrides
- `RunRef`: Handle to running execution
- `RunUpdate`: Progress update event

---

## Phase 1: Enhanced Remote Task Features

### Goals
Enhance remote task execution to match Python SDK's `TaskDetails` and `Task.get()` functionality.

### New API Surface

#### Task Discovery
```go
import "github.com/flyteorg/flyte-sdk-go-min/flyte/remote"

// Get task with auto-versioning
task, err := remote.GetTask(ctx, remote.TaskRef{
    Name:    "data_processing",
    Project: "analytics",
    Domain:  "production",
}, remote.WithAutoVersion("latest")) // or "current"

// Get task details
details, err := task.Details(ctx)
fmt.Printf("Interface: %+v\n", details.Interface)
fmt.Printf("Resources: %+v\n", details.Resources)
fmt.Printf("Cache: %+v\n", details.Cache)
```

#### Task Overrides
```go
// Override resources
overriddenTask := task.Override(
    remote.WithResources(flyte.Resources{
        CPU:    flyte.NewCPU("4"),
        Memory: flyte.NewMemory("8Gi"),
    }),
    remote.WithRetries(3),
    remote.WithTimeout("30m"),
    remote.WithCache(flyte.Cache{
        Behavior: "override",
        Version:  "v2",
    }),
)

run, err := flyte.Run(ctx, overriddenTask, inputs)
```

#### Run Management
```go
// List runs
runs, err := flyte.ListRuns(ctx, flyte.ListOptions{
    Project:   "my-project",
    Domain:    "production",
    TaskName:  "data_processing",
    InPhase:   []string{"RUNNING", "QUEUED"},
    Limit:     100,
})

// Get existing run
run, err := flyte.GetRun(ctx, "run-name-001")

// Abort run
err = run.Abort(ctx)

// Stream logs
logChan := run.StreamLogs(ctx)
for log := range logChan {
    fmt.Println(log.Message)
}
```

#### Advanced Types
```go
// Support for complex types
type Input struct {
    Paths     []string          `json:"paths"`
    Config    map[string]string `json:"config"`
    Threshold float64           `json:"threshold"`
}

type Output struct {
    ResultPath string   `json:"result_path"`
    Metrics    []Metric `json:"metrics"`
}

inputs := &Input{
    Paths:     []string{"/data/1.csv", "/data/2.csv"},
    Config:    map[string]string{"format": "csv"},
    Threshold: 0.95,
}

run, err := flyte.Run(ctx, task, inputs)
var output Output
err = run.Outputs(ctx, &output)
```

### New Files
```
flyte/remote/
├── task.go           # Task and TaskDetails
├── override.go       # Task override options
├── list.go           # List operations
└── types.go          # Type conversions
```

### Reference Implementation
- Python: `/Users/ketanumare/src/flyte-sdk/src/flyte/remote/_task.py`
- Python: `/Users/ketanumare/src/flyte-sdk/src/flyte/remote/_run.py`

---

## Phase 2: Local Task Authoring

### Goals
Enable users to author, register, and execute tasks locally, then deploy to Flyte cluster.

### API Surface

#### Task Registration (Decorator Style)
```go
import "github.com/flyteorg/flyte-sdk-go-min/flyte"

// Define task environment
var env = flyte.NewTaskEnvironment(
    "my_env",
    "python:3.9-slim",
    flyte.Resources{
        CPU:    flyte.NewCPU("500m"),
        Memory: flyte.NewMemory("1Gi"),
    },
)

// Register task function
func ProcessData(ctx flyte.Context, inputPath string, threshold float64) (string, error) {
    // Task implementation
    outputPath := "/tmp/output.csv"
    // ... processing logic
    return outputPath, nil
}

func init() {
    flyte.RegisterTask("process_data", ProcessData, env)
}
```

#### Task Configuration
```go
// With advanced configuration
var heavyTask = flyte.NewTaskEnvironment(
    "gpu_env",
    "nvidia/cuda:12.0-base",
    flyte.Resources{
        CPU:    flyte.NewCPURange("1", "4"),
        Memory: flyte.NewMemoryRange("2Gi", "16Gi"),
        GPU:    flyte.NewGPU("1", flyte.NVIDIA_A100),
    },
    flyte.WithRetries(3),
    flyte.WithTimeout("1h"),
    flyte.WithCache(flyte.Cache{
        Behavior: "auto",
        Serialize: true,
    }),
    flyte.WithSecrets([]flyte.Secret{
        {MountPath: "/secrets/api-key", Key: "api-key"},
    }),
)
```

#### Local Execution
```go
// Run locally
ctx := context.Background()
run := flyte.Run(ctx, ProcessData, "/data/input.csv", 0.95)
result, err := run.Wait()
fmt.Println("Output:", result)
```

#### Type System
```go
// Custom types with validation
type DataConfig struct {
    Format    string   `json:"format" flyte:"required"`
    Delimiter string   `json:"delimiter" flyte:"default=,"`
    Columns   []string `json:"columns"`
}

func AnalyzeData(ctx flyte.Context, config DataConfig, files []string) (map[string]float64, error) {
    metrics := make(map[string]float64)
    // ... analysis logic
    return metrics, nil
}
```

### Architecture

#### Controller Pattern (from flyte-sdk-go)
```go
// Controller interface for task execution
type Controller interface {
    submitTask(ctx Context, task Task, args ...interface{}) (any, error)
}

// Local controller for in-process execution
type LocalController struct {
    registry *TaskRegistry
}

// Remote controller for cluster execution
type RemoteController struct {
    queueClient  QueueServiceClient
    stateClient  StateServiceClient
    serializer   Serializer
}
```

#### Future Pattern
```go
// Future for async results
type Future[T any] struct {
    result T
    err    error
    done   chan struct{}
}

func (f *Future[T]) Get() T {
    <-f.done
    return f.result
}

func (f *Future[T]) Error() error {
    <-f.done
    return f.err
}
```

### New Files
```
flyte/
├── task.go           # Task definition and registration
├── environment.go    # TaskEnvironment
├── registry.go       # Global task registry
├── controller.go     # Controller interface
├── local_controller.go  # Local execution
├── remote_controller.go # Remote execution
├── futures.go        # Future types
├── resources.go      # Resource specifications
├── cache.go          # Cache configuration
└── secrets.go        # Secret management
```

### Reference Implementation
- Go: `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/flyte.go`
- Go: `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/types.go`
- Python: `/Users/ketanumare/src/flyte-sdk/src/flyte/_task.py`

---

## Phase 3: Workflow Composition

### Goals
Enable task composition into workflows with data flow, control flow, and dynamic execution.

### API Surface

#### Basic Workflow
```go
import "github.com/flyteorg/flyte-sdk-go-min/flyte"

func DataPipeline(ctx flyte.Context, inputPath string) (string, error) {
    // Call preprocessing task
    cleanedFut := flyte.RunTask[string](ctx, CleanData, inputPath)
    if cleanedFut.Error() != nil {
        return "", cleanedFut.Error()
    }
    cleanedPath := cleanedFut.Get()

    // Call transformation task
    transformedFut := flyte.RunTask[string](ctx, TransformData, cleanedPath)
    if transformedFut.Error() != nil {
        return "", transformedFut.Error()
    }

    // Call analysis task
    resultFut := flyte.RunTask[string](ctx, AnalyzeData, transformedFut.Get())
    return resultFut.Get(), resultFut.Error()
}

func init() {
    // Register as workflow
    flyte.RegisterWorkflow("data_pipeline", DataPipeline, env)
}
```

#### Parallel Execution
```go
func ParallelProcessing(ctx flyte.Context, paths []string) ([]Result, error) {
    // Launch tasks in parallel
    futures := make([]*flyte.Future[Result], len(paths))
    for i, path := range paths {
        futures[i] = flyte.RunTask[Result](ctx, ProcessFile, path)
    }

    // Collect results
    results := make([]Result, len(futures))
    for i, fut := range futures {
        if fut.Error() != nil {
            return nil, fut.Error()
        }
        results[i] = fut.Get()
    }

    return results, nil
}
```

#### Conditional Execution
```go
func ConditionalWorkflow(ctx flyte.Context, threshold float64) (string, error) {
    // Get metric
    metricFut := flyte.RunTask[float64](ctx, ComputeMetric)
    if metricFut.Error() != nil {
        return "", metricFut.Error()
    }

    // Conditional logic
    if metricFut.Get() > threshold {
        fut := flyte.RunTask[string](ctx, HighMetricPath)
        return fut.Get(), fut.Error()
    } else {
        fut := flyte.RunTask[string](ctx, LowMetricPath)
        return fut.Get(), fut.Error()
    }
}
```

#### Dynamic Tasks
```go
func DynamicWorkflow(ctx flyte.Context, config Config) ([]Result, error) {
    // Determine tasks to run dynamically
    tasksFut := flyte.RunTask[[]TaskSpec](ctx, DetermineTasks, config)
    if tasksFut.Error() != nil {
        return nil, tasksFut.Error()
    }

    tasks := tasksFut.Get()
    futures := make([]*flyte.Future[Result], len(tasks))

    // Run dynamic set of tasks
    for i, taskSpec := range tasks {
        // Get task by name and override
        task, _ := flyte.GetTask(taskSpec.Name)
        modifiedTask := task.Override(
            flyte.WithResources(taskSpec.Resources),
        )
        futures[i] = flyte.RunTask[Result](ctx, modifiedTask, taskSpec.Inputs)
    }

    // Collect results
    results := make([]Result, len(futures))
    for i, fut := range futures {
        results[i] = fut.Get()
    }

    return results, nil
}
```

#### Subworkflows
```go
func ParentWorkflow(ctx flyte.Context, input string) (string, error) {
    // Call subworkflow
    subFut := flyte.RunTask[string](ctx, ChildWorkflow, input)
    return subFut.Get(), subFut.Error()
}

func ChildWorkflow(ctx flyte.Context, input string) (string, error) {
    fut1 := flyte.RunTask[string](ctx, Step1, input)
    fut2 := flyte.RunTask[string](ctx, Step2, fut1.Get())
    return fut2.Get(), fut2.Error()
}
```

### Architecture

#### Workflow DSL
- Tasks compose into workflows
- Workflows are themselves tasks
- Futures represent async computation
- Automatic data dependency tracking

#### Execution Modes
- **Local**: In-process execution for testing
- **Remote**: Cluster execution via RunService
- **Hybrid**: Parent local, children remote

### New Files
```
flyte/
├── workflow.go       # Workflow registration
├── dynamic.go        # Dynamic task support
├── map.go           # Map task utilities
└── control_flow.go  # Conditional helpers
```

### Reference Implementation
- Go: `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/flyte.go` (RunTask, Futures)
- Python: `/Users/ketanumare/src/flyte-sdk/src/flyte/_task.py`
- Python: `/Users/ketanumare/src/flyte-sdk/src/flyte/_workflow.py`

---

## Phase 4: Apps and Deployment

### Goals
Package tasks/workflows into deployable apps and register them to Flyte clusters.

### API Surface

#### App Definition
```go
import "github.com/flyteorg/flyte-sdk-go-min/flyte/app"

func main() {
    // Create app
    myApp := app.New("data-analytics", "v1.0.0")

    // Add tasks
    myApp.AddTask("clean_data", CleanData, cleanEnv)
    myApp.AddTask("transform", TransformData, transformEnv)
    myApp.AddTask("analyze", AnalyzeData, gpuEnv)

    // Add workflows
    myApp.AddWorkflow("pipeline", DataPipeline, pipelineEnv)

    // Configure
    myApp.WithProject("analytics")
    myApp.WithDomain("production")
    myApp.WithImage("my-org/analytics:v1.0.0")

    // Deploy
    if err := myApp.Deploy(ctx); err != nil {
        log.Fatal(err)
    }
}
```

#### Serialization
```go
// Serialize to protobuf
taskSpec, err := app.SerializeTask(CleanData, cleanEnv)
workflowSpec, err := app.SerializeWorkflow(DataPipeline, pipelineEnv)

// Create code bundle
bundle, err := app.CreateCodeBundle("/path/to/project", app.CopyStyle{
    Style: "all",
    Exclude: []string{"*.pyc", "__pycache__"},
})

// Build image
imageRef, err := app.BuildImage(ctx, app.ImageConfig{
    BaseImage:  "python:3.9-slim",
    Dockerfile: "Dockerfile",
    Tags:       []string{"v1.0.0", "latest"},
})
```

#### Registration
```go
// Register individual task
err := app.RegisterTask(ctx, config, taskSpec)

// Register workflow
err := app.RegisterWorkflow(ctx, config, workflowSpec)

// Bulk registration
err := myApp.Register(ctx, config)
```

#### Versioning
```go
// Auto-versioning from git
version, err := app.GitVersion()

// Manual version
myApp.WithVersion("v2.1.3")

// Version override
myApp.WithVersionOverride("dev-snapshot")
```

### Architecture

#### Serialization Pipeline
1. Task/workflow definition (Go code)
2. Type inference and validation
3. Protobuf translation
4. Code bundle creation
5. Image reference resolution
6. Upload to control plane

#### Code Bundle Strategies
- **all**: Copy entire project directory
- **loaded_modules**: Only files in sys.path
- **pkl**: Pickle serialization for notebooks

### New Files
```
flyte/app/
├── app.go           # App definition
├── serialize.go     # Task/workflow serialization
├── bundle.go        # Code bundle creation
├── image.go         # Image building
├── register.go      # Registration API
└── version.go       # Version management
```

### Reference Implementation
- Python: `/Users/ketanumare/src/flyte-sdk/src/flyte/_deploy.py`
- Python: `/Users/ketanumare/src/flyte-sdk/src/flyte/_code_bundle.py`
- Go: `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/serializer_internal.go`

---

## Phase 5: Advanced Features

### Goals
Complete feature parity with Python SDK and enterprise-ready capabilities.

### Features

#### Launch Plans
```go
// Create launch plan
plan := flyte.NewLaunchPlan("daily_pipeline", DataPipeline)
plan.WithSchedule("0 0 * * *") // Daily at midnight
plan.WithDefaultInputs(map[string]interface{}{
    "threshold": 0.95,
})
plan.WithNotifications([]flyte.Notification{
    {
        Type:      "email",
        Phases:    []string{"FAILED"},
        Recipients: []string{"team@example.com"},
    },
})

err := plan.Deploy(ctx, config)
```

#### Schedules
```go
// Cron schedule
schedule := flyte.CronSchedule("0 */6 * * *") // Every 6 hours

// Fixed rate
schedule := flyte.FixedRate("30m")

// Event-based trigger
trigger := flyte.OnEvent("s3://bucket/path", "ObjectCreated")
```

#### Secrets
```go
func SecureTask(ctx flyte.Context) error {
    // Access mounted secret
    apiKey := flyte.ReadSecret(ctx, "api-key")

    // Use in task
    client := NewClient(apiKey)
    return client.DoWork()
}

env := flyte.NewTaskEnvironment("secure_env", "app:v1",
    flyte.Resources{...},
    flyte.WithSecrets([]flyte.Secret{
        {Key: "api-key", MountPath: "/secrets/api-key"},
        {Key: "db-password", Group: "postgres"},
    }),
)
```

#### Custom Types
```go
// Register custom type transformer
flyte.RegisterTypeTransformer(
    reflect.TypeOf(MyCustomType{}),
    &MyCustomTypeTransformer{},
)

type MyCustomTypeTransformer struct{}

func (t *MyCustomTypeTransformer) ToLiteral(v interface{}) (*core.Literal, error) {
    // Convert Go type to protobuf literal
}

func (t *MyCustomTypeTransformer) FromLiteral(lit *core.Literal) (interface{}, error) {
    // Convert protobuf literal to Go type
}
```

#### Agents
```go
// Define agent for custom task type
type MyAgent struct{}

func (a *MyAgent) Execute(ctx Context, task Task) (Result, error) {
    // Custom execution logic
}

flyte.RegisterAgent("my-custom-type", &MyAgent{})
```

### New Files
```
flyte/
├── launch_plan.go    # Launch plan API
├── schedule.go       # Schedule types
├── notifications.go  # Notification config
├── plugins/          # Plugin system
│   ├── types.go      # Type transformers
│   └── agents.go     # Agent framework
└── advanced/
    ├── checkpoints.go
    ├── signals.go
    └── reporting.go
```

---

## API Design Principles

### 1. Python SDK Compatibility
- Mirror Python SDK API where Go idioms allow
- Use similar naming conventions (snake_case → CamelCase)
- Match functional patterns (decorators → registration)

### 2. Go Idioms
- Use interfaces for extensibility
- Prefer composition over inheritance
- Use functional options for configuration
- Return errors explicitly, not exceptions
- Use generics for type-safe Futures

### 3. Type Safety
- Leverage Go's type system
- Generic Futures for compile-time type checking
- Interface types for flexibility
- Struct tags for metadata

### 4. Developer Experience
- Simple API for common cases
- Builder pattern for complex configuration
- Clear error messages
- Comprehensive examples

### 5. Backward Compatibility
- Never break Phase 0 API
- Deprecate with clear migration paths
- Semantic versioning

---

## Reference Implementations

### Python SDK Structure
```
flyte-sdk/src/flyte/
├── _initialize.py       # Client initialization
├── _run.py             # Run and RunContext
├── _task.py            # Task definition
├── _workflow.py        # Workflow composition
├── remote/
│   ├── _task.py        # Remote task (TaskDetails, LazyEntity)
│   ├── _run.py         # Remote run (Run, RunDetails)
│   └── _client/        # Client implementation
├── _deploy.py          # Deployment and registration
├── _code_bundle.py     # Code bundling
└── types/              # Type system
```

### Go SDK Structure
```
flyte-sdk-go/pkg/flyte/
├── flyte.go             # Main API (RegisterTask, RunTask, Run)
├── interfaces.go        # Core interfaces
├── types.go             # Type definitions
├── futures.go           # Future implementation
├── run.go               # RunHandle
├── run_context.go       # RunContext
├── controller_local.go  # Local execution
├── controller_remote.go # Remote execution
├── serializer_internal.go # Serialization
└── config.go            # Configuration
```

### Key Files for Reference

#### Python SDK
- `/Users/ketanumare/src/flyte-sdk/src/flyte/_run.py` - Run and RunContext patterns
- `/Users/ketanumare/src/flyte-sdk/src/flyte/remote/_task.py` - TaskDetails, LazyEntity, auto-versioning
- `/Users/ketanumare/src/flyte-sdk/src/flyte/remote/_run.py` - Run operations (wait, watch, outputs)
- `/Users/ketanumare/src/flyte-sdk/src/flyte/_initialize.py` - Initialization patterns
- `/Users/ketanumare/src/flyte-sdk/src/flyte/_task.py` - Task authoring

#### Go SDK
- `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/flyte.go` - Main API design
- `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/interfaces.go` - Interface patterns
- `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/types.go` - Type system
- `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/futures.go` - Future pattern
- `/Users/ketanumare/src/flyte-sdk-go/pkg/flyte/run.go` - RunHandle

#### Protocol Buffers
- `/Users/ketanumare/src/flyte/gen/go/flyteidl2/workflow/run_service.pb.go` - RunService protos
- Available at: `https://github.com/flyteorg/flyte/blob/v2/gen/go/flyteidl2/workflow/run_service.pb.go`

#### Authentication
- `https://github.com/flyteorg/flyte/tree/master/flyteidl/clients/go/admin` - OAuth implementation

---

## Developer Experience Examples

### Phase 0: Remote Execution (Current)
```go
// Initialize once
flyte.Initialize(ctx, config)

// Execute remote task
task := &remote.TaskRef{Name: "preprocess", Version: "v1", Project: "ml", Domain: "prod"}
run, _ := flyte.Run(ctx, task, map[string]interface{}{"path": "/data/input.csv"})
run.Wait(ctx)
fmt.Println(run.URL)
```

### Phase 2: Local Authoring
```go
// Define task
func Train(ctx flyte.Context, data string, epochs int) (string, error) {
    // ML training code
    return "/models/model.pkl", nil
}

// Register
flyte.RegisterTask("train", Train, gpuEnv)

// Run locally
run := flyte.Run(ctx, Train, "/data/train.csv", 100)
modelPath, _ := run.Wait()
```

### Phase 3: Workflows
```go
func MLPipeline(ctx flyte.Context, rawData string) (string, error) {
    // Preprocess
    cleanFut := flyte.RunTask[string](ctx, Preprocess, rawData)

    // Train
    modelFut := flyte.RunTask[string](ctx, Train, cleanFut.Get(), 100)

    // Evaluate
    metricsFut := flyte.RunTask[Metrics](ctx, Evaluate, modelFut.Get())

    return modelFut.Get(), metricsFut.Error()
}

flyte.RegisterWorkflow("ml_pipeline", MLPipeline, env)
```

### Phase 4: Deployment
```go
app := app.New("ml-platform", "v2.0.0")
app.AddTask("preprocess", Preprocess, env)
app.AddTask("train", Train, gpuEnv)
app.AddWorkflow("pipeline", MLPipeline, env)
app.Deploy(ctx)
```

---

## Implementation Strategy

### Incremental Development
1. **Phase 0** ✅ Complete - Basic remote execution
2. **Phase 1** - Enhanced remote features (task details, overrides, listing)
3. **Phase 2** - Local authoring (registration, execution, types)
4. **Phase 3** - Workflows (composition, futures, control flow)
5. **Phase 4** - Deployment (serialization, registration, apps)
6. **Phase 5** - Advanced (launch plans, agents, plugins)

### Testing Strategy
- Unit tests for each component
- Integration tests against local Flyte cluster
- Example programs for each phase
- Compatibility tests with Python SDK outputs

### Documentation
- API documentation (godoc)
- User guide with examples
- Migration guide from Phase N to N+1
- Best practices guide

---

## Success Criteria

### Phase 0 (Current) ✅
- [x] Can execute remote tasks
- [x] Can monitor execution progress
- [x] Can retrieve outputs
- [x] OAuth2 authentication works

### Phase 1
- [ ] Can fetch task metadata
- [ ] Can override task configuration
- [ ] Can list and filter runs
- [ ] Advanced types work

### Phase 2
- [ ] Can define tasks in Go
- [ ] Can execute tasks locally
- [ ] Can specify resources
- [ ] Type inference works

### Phase 3
- [ ] Can compose workflows
- [ ] Futures work correctly
- [ ] Can express control flow
- [ ] Dynamic tasks work

### Phase 4
- [ ] Can serialize to protobuf
- [ ] Can register to cluster
- [ ] Code bundling works
- [ ] Apps deploy successfully

### Phase 5
- [ ] Launch plans work
- [ ] Schedules trigger correctly
- [ ] Secrets are accessible
- [ ] Custom types integrate

---

## Conclusion

This blueprint provides a complete roadmap for evolving flyte-sdk-go-min from its current minimal state to a full-featured SDK matching the Python SDK. Each phase builds on the previous, maintaining backward compatibility while adding powerful new capabilities. The design follows Go idioms while preserving the developer experience of the Python SDK, making Flyte accessible to Go developers for building data pipelines, ML workflows, and distributed applications.

// Package flyte provides a minimal SDK for executing tasks on Flyte clusters.
//
// The Flyte SDK allows you to programmatically execute pre-deployed tasks
// and monitor their progress from Go applications. This minimal SDK focuses
// on remote task execution, with plans to expand to local task authoring,
// workflow composition, and deployment in future phases.
//
// # Quick Start
//
// Initialize the Flyte client:
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
//	    log.Fatal(err)
//	}
//	defer flyte.Close()
//
// Execute a remote task:
//
//	import "github.com/flyteorg/flyte-sdk-go-min/flyte/remote"
//
//	task := &remote.TaskRef{
//	    Name:    "data_processing",
//	    Version: "v1.0.0",
//	    Project: "analytics",
//	    Domain:  "production",
//	}
//
//	run, err := flyte.Execute(ctx, task, map[string]interface{}{
//	    "input_path": "/data/input.csv",
//	    "threshold":  0.95,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Wait for completion
//	if err := run.Wait(ctx); err != nil {
//	    log.Fatalf("Run failed: %v", err)
//	}
//
//	// Get outputs
//	outputs, err := run.GetOutputs(ctx)
//	fmt.Printf("Result: %s\n", outputs["result_path"])
//
// # Custom Run Configuration
//
// Use RunContext for advanced configuration:
//
//	runCtx := flyte.NewRunContext(ctx).
//	    WithProject("my-project").
//	    WithDomain("staging").
//	    WithRunName("daily-processing-001").
//	    WithLabel("team", "data-eng").
//	    WithAnnotation("owner", "alice@example.com").
//	    WithEnvVar("LOG_LEVEL", "DEBUG").
//	    WithOverwriteCache(true)
//
//	run, err := flyte.ExecuteWithContext(runCtx, task, inputs)
//
// # Monitoring Progress
//
// Stream run updates:
//
//	updateChan, err := run.Watch(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for update := range updateChan {
//	    if update.Error != "" {
//	        log.Printf("Error: %s", update.Error)
//	        break
//	    }
//	    log.Printf("Phase: %s at %v", update.Phase, update.Timestamp)
//	}
//
// Check current phase:
//
//	phase := run.GetPhase()
//	fmt.Printf("Current phase: %s\n", phase)
//
// # Core Interfaces
//
// The SDK is designed around these key interfaces:
//
//   - Task: Represents an executable task (remote or local)
//   - Run: Represents an execution with monitoring capabilities
//   - RunContextBuilder: Fluent API for configuring run execution
//   - Client: Connection to Flyte cluster (internal)
//
// # Thread Safety
//
// The SDK is thread-safe for concurrent execution:
//   - Initialize() uses sync.Once and can be safely called from multiple goroutines
//   - Multiple Execute() calls can run concurrently
//   - Run instances are safe for concurrent method calls
//   - RunContext instances should not be shared across goroutines
//
// # Error Handling
//
// Errors are returned following Go conventions:
//   - Initialize() returns error if configuration or connection fails
//   - Execute() returns error if run creation fails
//   - Run.Wait() returns error if the run fails or is aborted
//   - Run.GetOutputs() returns error if outputs cannot be retrieved
//
// # Local Development
//
// For local Flyte clusters (e.g., sandbox):
//
//	config := &flyte.Config{
//	    Endpoint: "dns:///localhost:8089",
//	    Project:  "flytesnacks",
//	    Domain:   "development",
//	    Insecure: true, // Disable TLS for local dev
//	}
//
// # Future Phases
//
// This minimal SDK will be extended with:
//   - Phase 1: Task details, auto-versioning, resource overrides
//   - Phase 2: Local task authoring and registration
//   - Phase 3: Workflow composition with futures
//   - Phase 4: Task/workflow deployment
//   - Phase 5: Launch plans, schedules, and advanced features
//
// See CLAUDE.md for the complete implementation roadmap.
package flyte

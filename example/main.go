package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"flyte-go-sdk/flyte"
	"flyte-go-sdk/flyte/remote"
)

func main() {
	ctx := context.Background()

	// Initialize Flyte client
	config := &flyte.Config{
		Endpoint: getEnv("FLYTE_ENDPOINT", "dns:///demo.hosted.unionai.cloud"),
		Insecure: true,
		Project:  getEnv("FLYTE_PROJECT", "ketan"),
		Domain:   getEnv("FLYTE_DOMAIN", "development"),
		Org:      getEnv("FLYTE_ORG", "playground"),

		// OAuth ClientSecret authentication (optional)
		// Set these environment variables for authenticated clusters
		ClientID:     getEnv("FLYTE_CLIENT_ID", ""),
		ClientSecret: getEnv("FLYTE_CLIENT_SECRET", ""),
		TokenURL:     getEnv("FLYTE_TOKEN_URL", ""),
		Scopes:       []string{},
	}

	if err := flyte.Initialize(ctx, config); err != nil {
		log.Fatalf("Failed to initialize Flyte: %v", err)
	}
	defer flyte.Close() // Clean up on exit

	fmt.Println("=== Flyte Minimal Go SDK Examples ===")
	fmt.Println("Using interface-based architecture for extensibility\n")

	// Example 1: Simple execution with Wait
	example1()

	// Example 2: Execution with Watch (streaming updates)
	example2()

	// Example 3: Execution with custom RunContext
	example3()

	fmt.Println("All examples completed!")
}

// Example 1: Simple execution with Wait
// Demonstrates the basic Execute() -> Wait() -> GetOutputs() pattern
func example1() {
	fmt.Println("Example 1: Simple Execution with Wait")
	fmt.Println("--------------------------------------")

	ctx := context.Background()

	// Create a reference to a remote task
	// TaskRef implements the flyte.Task interface
	task := &remote.TaskRef{
		Name:    getEnv("EXAMPLE_TASK_NAME", "my_task"),
		Version: getEnv("EXAMPLE_TASK_VERSION", "v1.0"),
		Project: getEnv("FLYTE_PROJECT", "flytesnacks"),
		Domain:  getEnv("FLYTE_DOMAIN", "development"),
	}

	// Execute the task - returns a flyte.Run interface
	run, err := flyte.Execute(ctx, task, map[string]interface{}{
		"input_a": 42,
		"input_b": "hello world",
	})
	if err != nil {
		log.Printf("Failed to start run: %v", err)
		return
	}

	// Use Run interface methods
	fmt.Printf("Started run: %s\n", run.GetName())
	fmt.Printf("Track at: %s\n", run.GetURL())
	fmt.Printf("Initial phase: %s\n", run.GetPhase())

	// Wait for completion (blocks until terminal state)
	fmt.Println("Waiting for completion...")
	if err := run.Wait(ctx); err != nil {
		log.Printf("Run failed: %v", err)
		return
	}

	fmt.Printf("Final phase: %s\n", run.GetPhase())

	// Get outputs
	outputs, err := run.GetOutputs(ctx)
	if err != nil {
		log.Printf("Failed to get outputs: %v", err)
		return
	}

	fmt.Printf("Outputs: %v\n\n", outputs)
}

// Example 2: Execution with Watch (streaming updates)
// Demonstrates real-time progress monitoring via channels
func example2() {
	fmt.Println("Example 2: Execution with Watch (Streaming)")
	fmt.Println("--------------------------------------------")

	ctx := context.Background()

	task := &remote.TaskRef{
		Name:    getEnv("EXAMPLE_TASK_NAME", "my_task"),
		Version: getEnv("EXAMPLE_TASK_VERSION", "v1.0"),
		Project: getEnv("FLYTE_PROJECT", "flytesnacks"),
		Domain:  getEnv("FLYTE_DOMAIN", "development"),
	}

	// Execute the task
	run, err := flyte.Execute(ctx, task, map[string]interface{}{
		"input_a": 100,
		"input_b": "streaming example",
	})
	if err != nil {
		log.Printf("Failed to start run: %v", err)
		return
	}

	fmt.Printf("Started run: %s\n", run.GetName())
	fmt.Printf("Track at: %s\n", run.GetURL())

	// Watch for updates via channel
	updateChan, err := run.Watch(ctx)
	if err != nil {
		log.Printf("Failed to watch run: %v", err)
		return
	}

	fmt.Println("Watching run progress...")
	// Channel closes automatically when run completes
	for update := range updateChan {
		if update.Error != "" {
			fmt.Printf("❌ Error: %s\n", update.Error)
			break
		}
		fmt.Printf("⏱  [%s] Phase: %s\n",
			update.Timestamp.Format("15:04:05"),
			update.Phase)
	}

	// Get final outputs
	outputs, err := run.GetOutputs(ctx)
	if err != nil {
		log.Printf("Failed to get outputs: %v", err)
		return
	}

	fmt.Printf("✅ Final outputs: %v\n\n", outputs)
}

// Example 3: Execution with custom RunContext
// Demonstrates the builder pattern for advanced configuration
func example3() {
	fmt.Println("Example 3: Execution with Custom RunContext")
	fmt.Println("--------------------------------------------")

	ctx := context.Background()

	task := &remote.TaskRef{
		Name:    getEnv("EXAMPLE_TASK_NAME", "my_task"),
		Version: getEnv("EXAMPLE_TASK_VERSION", "v1.0"),
		Project: getEnv("FLYTE_PROJECT", "flytesnacks"),
		Domain:  getEnv("FLYTE_DOMAIN", "development"),
	}

	// Create a RunContext with custom configuration
	// RunContext implements RunContextBuilder interface
	runCtx := flyte.NewRunContext(ctx).
		WithProject("myproject"). // Override project
		WithDomain("production"). // Override domain
		WithRunName("demo-run-"+time.Now().Format("20060102-150405")). // Custom run name
		WithLabel("team", "ml-platform"). // Add labels
		WithLabel("environment", "production").
		WithLabel("initiated-by", "example-code").
		WithAnnotation("owner", "data-science-team"). // Add annotations
		WithAnnotation("description", "SDK refactoring demo").
		WithEnvVar("LOG_LEVEL", "DEBUG"). // Set env vars
		WithEnvVar("MAX_RETRIES", "5").
		WithOverwriteCache(true) // Force re-execution

	fmt.Println("Custom RunContext configured:")
	fmt.Printf("  Project: %s\n", runCtx.GetProject())
	fmt.Printf("  Domain: %s\n", runCtx.GetDomain())
	fmt.Printf("  Run Name: %s\n", runCtx.GetRunName())
	fmt.Printf("  Labels: %v\n", runCtx.GetLabels())
	fmt.Printf("  Annotations: %v\n", runCtx.GetAnnotations())
	fmt.Printf("  Env Vars: %v\n", runCtx.GetEnvVars())
	fmt.Printf("  Overwrite Cache: %v\n", runCtx.GetOverwriteCache())
	fmt.Println()

	// Execute with custom context
	run, err := flyte.ExecuteWithContext(runCtx, task, map[string]interface{}{
		"input_a": 200,
		"input_b": "custom context example",
	})
	if err != nil {
		log.Printf("Failed to start run: %v", err)
		return
	}

	fmt.Printf("Started custom run: %s\n", run.GetName())
	fmt.Printf("Track at: %s\n", run.GetURL())

	// Wait for completion
	fmt.Println("Waiting for completion...")
	if err := run.Wait(ctx); err != nil {
		log.Printf("Run failed: %v", err)
		return
	}

	fmt.Printf("Final phase: %s\n", run.GetPhase())

	// Get outputs
	outputs, err := run.GetOutputs(ctx)
	if err != nil {
		log.Printf("Failed to get outputs: %v", err)
		return
	}

	fmt.Printf("Outputs: %v\n\n", outputs)
}

// getEnv retrieves environment variable with fallback to default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

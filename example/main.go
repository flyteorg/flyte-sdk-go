// Command example launches tasks registered on a Flyte control plane using
// the flyte Go SDK.
//
// Configure via env vars:
//
//	FLYTE_ENDPOINT      control plane endpoint, e.g. my-org.example.com (required)
//	FLYTE_PROJECT       project the runs are created in (required)
//	FLYTE_DOMAIN        domain (default development)
//	FLYTE_TASK          task to run (default hello_world.square)
//	FLYTE_TASK_VERSION  task version (default: latest)
//	FLYTE_API_KEY       API key -> headless ClientSecret auth
//	FLYTE_AUTH_COMMAND  external command that prints a bearer token
//
// Without FLYTE_API_KEY or FLYTE_AUTH_COMMAND the browser-based PKCE flow is
// used.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/unionai/flyte-sdk-go/flyte"
)

func main() {
	ctx := context.Background()

	cfg := flyte.Config{
		Endpoint: os.Getenv("FLYTE_ENDPOINT"),
		Project:  os.Getenv("FLYTE_PROJECT"),
		Domain:   getEnv("FLYTE_DOMAIN", "development"),
		APIKey:   os.Getenv("FLYTE_API_KEY"),
	}
	if (cfg.Endpoint == "" && cfg.APIKey == "") || cfg.Project == "" {
		log.Fatal("set FLYTE_ENDPOINT (or FLYTE_API_KEY) and FLYTE_PROJECT, e.g.\n" +
			"  FLYTE_ENDPOINT=my-org.example.com FLYTE_PROJECT=my-project go run ./example")
	}
	if cmd := os.Getenv("FLYTE_AUTH_COMMAND"); cmd != "" {
		cfg.AuthType = flyte.AuthTypeExternalCommand
		cfg.Command = strings.Fields(cmd)
	}

	if err := flyte.Init(ctx, cfg); err != nil {
		log.Fatalf("failed to initialize: %v", err)
	}
	defer flyte.Close()

	runLatestTask(ctx)
	runWithWatch(ctx)
}

// runLatestTask fetches a task and runs it with typed inputs, then waits and
// prints the outputs. FLYTE_TASK / FLYTE_TASK_VERSION override which task is
// run; an empty version resolves to the latest deployed one.
func runLatestTask(ctx context.Context) {
	name := getEnv("FLYTE_TASK", "hello_world.square")
	fmt.Printf("=== %s (Wait) ===\n", name)

	task, err := flyte.GetTask(ctx, flyte.TaskRef{
		Name:    name,
		Version: os.Getenv("FLYTE_TASK_VERSION"),
	})
	if err != nil {
		log.Fatalf("failed to fetch task: %v", err)
	}
	fmt.Printf("resolved %s @ version %s\n", task.Name(), task.Version())

	run, err := flyte.Run(ctx, task, flyte.Inputs{"i": 7},
		flyte.WithEnvVar("LOG_LEVEL", "INFO"),
	)
	if err != nil {
		log.Fatalf("failed to launch run: %v", err)
	}
	fmt.Printf("launched run %s\n%s\n", run.Name(), run.URL())

	if err := run.Wait(ctx); err != nil {
		log.Fatalf("run failed: %v", err)
	}
	outputs, err := run.Outputs(ctx)
	if err != nil {
		log.Fatalf("failed to fetch outputs: %v", err)
	}
	fmt.Printf("phase=%s outputs=%v\n\n", run.Phase(), outputs)
}

// runWithWatch launches a task by reference (fetched automatically) with no
// inputs — the task's registered default (i=3) is applied — and streams phase
// updates while it executes.
func runWithWatch(ctx context.Context) {
	name := getEnv("FLYTE_TASK", "hello_world.square")
	fmt.Printf("=== %s (default inputs, streaming Watch) ===\n", name)

	run, err := flyte.Run(ctx,
		flyte.TaskRef{Name: name, Version: os.Getenv("FLYTE_TASK_VERSION")},
		nil, // no inputs: the registered default is used
	)
	if err != nil {
		log.Fatalf("failed to launch run: %v", err)
	}
	fmt.Printf("launched run %s\n%s\n", run.Name(), run.URL())

	updates, err := run.Watch(ctx)
	if err != nil {
		log.Fatalf("failed to watch run: %v", err)
	}
	for update := range updates {
		fmt.Printf("  [%s] %s\n", update.Timestamp.Format(time.TimeOnly), update.Phase)
		if update.Error != "" {
			log.Fatalf("run failed: %s", update.Error)
		}
	}

	outputs, err := run.Outputs(ctx)
	if err != nil {
		log.Fatalf("failed to fetch outputs: %v", err)
	}
	fmt.Printf("phase=%s outputs=%v\n", run.Phase(), outputs)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

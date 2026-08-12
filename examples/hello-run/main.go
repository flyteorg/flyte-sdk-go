// Command hello-run launches a deployed task by reference and reads back its
// outputs — the remote-control counterpart of flyte-sdk-rs's hello-trace
// example.
//
// The Rust example authors the task (three traced steps composed into
// my_task(x, label) -> string); this program is the other side of the
// boundary: it launches that already-deployed task on the control plane,
// waits for it, and prints the outputs. Deploy the Rust task first:
//
//	cd flyte-sdk-rs/examples/hello-trace
//	flyte run task.py my_task --x 21 --label demo   # deploys and runs once
//
// Then launch it from Go:
//
//	FLYTE_ENDPOINT=my-org.example.com FLYTE_PROJECT=my-project \
//	go run ./examples/hello-run -x 21 -label demo
//
// Any deployed task works — point -task at yours. Auth follows the same
// contract as examples/basic: FLYTE_API_KEY for headless, FLYTE_AUTH_COMMAND
// for an external token command, browser PKCE otherwise.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/unionai/flyte-sdk-go/flyte"
)

func main() {
	taskName := flag.String("task", "hello_trace_env.my_task", "deployed task to run (env.task)")
	version := flag.String("version", "", "task version (default: latest deployed)")
	x := flag.Int("x", 21, "value for the task's x input")
	label := flag.String("label", "demo", "value for the task's label input")
	flag.Parse()

	ctx := context.Background()
	mustInit(ctx)
	defer flyte.Close()

	// Resolve the task reference first to fail fast on typos; Run also accepts
	// the TaskRef directly and resolves it internally.
	task, err := flyte.GetTask(ctx, flyte.TaskRef{Name: *taskName, Version: *version})
	if err != nil {
		log.Fatalf("failed to fetch task: %v", err)
	}
	fmt.Printf("resolved %s @ version %s\n", task.Name(), task.Version())

	run, err := flyte.Run(ctx, task, flyte.Inputs{"x": *x, "label": *label})
	if err != nil {
		log.Fatalf("failed to launch run: %v", err)
	}
	fmt.Printf("launched run %s\n%s\n", run.Name(), run.URL())

	// Outputs waits for the run to finish and converts the outputs to native
	// Go values keyed by output name.
	outputs, err := run.Outputs(ctx)
	if err != nil {
		log.Fatalf("run did not produce outputs: %v", err)
	}
	fmt.Printf("phase=%s outputs=%v\n", run.Phase(), outputs)
}

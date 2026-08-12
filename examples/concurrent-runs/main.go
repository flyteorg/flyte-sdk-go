// Command concurrent-runs launches many runs of one task at once and collects
// their outputs — the remote-control counterpart of flyte-sdk-rs's
// concurrent-traces example.
//
// The Rust example fans out many traced steps *inside* one task; this program
// fans out at the level above: N independent runs of a deployed task in
// flight at the same time, each waited on concurrently, outputs collected at
// the end. RunHandle is safe for concurrent use, so plain goroutines are all
// it takes.
//
//	FLYTE_ENDPOINT=my-org.example.com FLYTE_PROJECT=my-project \
//	go run ./examples/concurrent-runs -task hello_trace_env.my_task -n 5
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/unionai/flyte-sdk-go/flyte"
)

func main() {
	taskName := flag.String("task", "hello_trace_env.my_task", "deployed task to run (env.task)")
	n := flag.Int("n", 5, "number of runs to launch concurrently")
	flag.Parse()

	ctx := context.Background()
	mustInit(ctx)
	defer flyte.Close()

	// Fetch the task once; the resolved version is shared by every run.
	task, err := flyte.GetTask(ctx, flyte.TaskRef{Name: *taskName})
	if err != nil {
		log.Fatalf("failed to fetch task: %v", err)
	}

	type result struct {
		run     string
		outputs map[string]any
		err     error
	}
	results := make([]result, *n)

	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			run, err := flyte.Run(ctx, task, flyte.Inputs{"x": i, "label": fmt.Sprintf("run-%d", i)})
			if err != nil {
				results[i] = result{err: err}
				return
			}
			fmt.Printf("launched %s (x=%d)\n", run.Name(), i)
			outputs, err := run.Outputs(ctx) // waits for the run to finish
			results[i] = result{run: run.Name(), outputs: outputs, err: err}
		}(i)
	}
	wg.Wait()

	failed := 0
	for i, r := range results {
		if r.err != nil {
			failed++
			fmt.Printf("x=%d: FAILED: %v\n", i, r.err)
			continue
		}
		fmt.Printf("x=%d: %s -> %v\n", i, r.run, r.outputs)
	}
	if failed > 0 {
		log.Fatalf("%d of %d runs failed", failed, *n)
	}
}

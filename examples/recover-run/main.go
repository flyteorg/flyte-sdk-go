// Command recover-run recovers a failed run, reusing everything that already
// succeeded — the remote-control counterpart of flyte-sdk-rs's retry-replay
// example.
//
// The Rust example shows replay *inside* one task: a recorded trace step is
// not re-run on retry. Recovery is the same idea one level up: a new run is
// created with WithRecover, actions that succeeded in the source run are not
// re-executed — they land in ACTION_PHASE_RECOVERED with their outputs reused
// — and only the failed part of the tree runs again.
//
// Take any failed (or aborted) run, then:
//
//	FLYTE_ENDPOINT=my-org.example.com FLYTE_PROJECT=my-project \
//	go run ./examples/recover-run -run <failed-run-name> -task <env.task>
//
// The recovery run re-passes the source run's task and inputs; the link to
// the source is recorded on the run (relation RECOVER) and per reused action
// (RecoveredFrom). WithForceRerunActions can pin named actions to re-execute
// even though they succeeded.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/unionai/flyte-sdk-go/flyte"
)

func main() {
	sourceName := flag.String("run", "", "failed run to recover (required)")
	taskName := flag.String("task", "hello_trace_env.my_task", "the source run's task (env.task)")
	x := flag.Int("x", 21, "the source run's x input")
	label := flag.String("label", "demo", "the source run's label input")
	flag.Parse()
	if *sourceName == "" {
		log.Fatal("-run is required: the name of the failed run to recover")
	}

	ctx := context.Background()
	mustInit(ctx)
	defer flyte.Close()

	source, err := flyte.GetRun(ctx, *sourceName)
	if err != nil {
		log.Fatalf("failed to fetch source run: %v", err)
	}
	fmt.Printf("recovering %s (%s)\n", source.Name(), source.Phase())

	recovery, err := flyte.Run(ctx, flyte.TaskRef{Name: *taskName},
		flyte.Inputs{"x": *x, "label": *label},
		flyte.WithRecover(source.Name()))
	if err != nil {
		log.Fatalf("failed to create recovery run: %v", err)
	}
	fmt.Printf("launched recovery run %s\n%s\n", recovery.Name(), recovery.URL())

	if err := recovery.Wait(ctx); err != nil {
		log.Fatalf("recovery run failed: %v", err)
	}

	// Reused actions are terminal in RECOVERED and point back at the source
	// run's action they were recovered from.
	actions, err := recovery.ListActions(ctx)
	if err != nil {
		log.Fatalf("failed to list actions: %v", err)
	}
	for _, a := range actions {
		line := fmt.Sprintf("action %-12s %s", a.Name(), a.Phase())
		if rf := a.RecoveredFrom(); rf != nil {
			line += fmt.Sprintf(" (reused from %s/%s)", rf.GetRun().GetName(), rf.GetName())
		}
		fmt.Println(line)
	}

	// Outputs works for recovered runs too: RECOVERED is a successful terminal
	// phase, and the outputs are served from the reused result.
	outputs, err := recovery.Outputs(ctx)
	if err != nil {
		log.Fatalf("failed to fetch outputs: %v", err)
	}
	fmt.Printf("phase=%s outputs=%v\n", recovery.Phase(), outputs)
}

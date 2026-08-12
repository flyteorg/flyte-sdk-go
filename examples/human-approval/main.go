// Command human-approval answers the approval conditions a paused run is
// waiting on — the reviewer's side of flyte-sdk-rs's human-approval example.
//
// The Rust task (gated_deploy) raises two conditions up front and then blocks
// until both are answered: "security-review" wants a bool, "release-ticket"
// wants a string. Its README answers them with the flyte CLI; this program is
// the same act through the Go SDK — attach to the run, discover the paused
// conditions, signal each with a typed value, and watch the run finish.
//
// Start the Rust task, note its run name, then:
//
//	FLYTE_ENDPOINT=my-org.example.com FLYTE_PROJECT=my-project \
//	go run ./examples/human-approval -run <run-name> -approve -ticket REL-123
//
// The server validates every signal against the condition's declared type and
// rejects mismatches and double signals with typed errors — nothing here
// needs to pre-validate.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/unionai/flyte-sdk-go/flyte"
)

func main() {
	runName := flag.String("run", "", "run that is paused on conditions (required)")
	approve := flag.Bool("approve", true, "answer for the security-review condition")
	ticket := flag.String("ticket", "REL-123", "answer for the release-ticket condition")
	flag.Parse()
	if *runName == "" {
		log.Fatal("-run is required: the name of the run waiting for approval")
	}

	ctx := context.Background()
	mustInit(ctx)
	defer flyte.Close()

	run, err := flyte.GetRun(ctx, *runName)
	if err != nil {
		log.Fatalf("failed to attach to run: %v", err)
	}
	fmt.Printf("attached to %s (%s)\n%s\n", run.Name(), run.Phase(), run.URL())

	// The conditions appear as PAUSED child actions once the task raises them;
	// poll until both are visible.
	conditions := awaitConditions(ctx, run, 2)
	for _, c := range conditions {
		fmt.Printf("condition %s [%s]: %s\n", c.Name(), c.Phase(), c.Prompt())
		if c.SignalInfo() != nil {
			fmt.Printf("  already answered, skipping\n")
			continue
		}

		// The condition's *action* name is generated; which question it is
		// lives in the prompt. Pick the answer type by matching on it.
		var value any
		switch {
		case strings.Contains(strings.ToLower(c.Prompt()), "security"):
			value = *approve // bool condition
		default:
			value = *ticket // string condition
		}
		if err := c.Signal(ctx, value); err != nil {
			log.Fatalf("failed to signal %s: %v", c.Name(), err)
		}
		fmt.Printf("  answered with %v\n", value)
	}

	// With both answers delivered the task resumes; a rejection is a normal
	// run failure (the Rust task returns a user error), not an SDK error.
	if err := run.Wait(ctx); err != nil {
		log.Fatalf("run did not succeed: %v", err)
	}
	outputs, err := run.Outputs(ctx)
	if err != nil {
		log.Fatalf("failed to fetch outputs: %v", err)
	}
	fmt.Printf("phase=%s outputs=%v\n", run.Phase(), outputs)
}

// awaitConditions polls until the run exposes at least want condition actions.
func awaitConditions(ctx context.Context, run *flyte.RunHandle, want int) []*flyte.Condition {
	for {
		conditions, err := run.ListConditions(ctx)
		if err != nil {
			log.Fatalf("failed to list conditions: %v", err)
		}
		if len(conditions) >= want {
			return conditions
		}
		fmt.Printf("waiting for conditions (%d/%d)...\n", len(conditions), want)
		select {
		case <-ctx.Done():
			log.Fatalf("context cancelled: %v", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

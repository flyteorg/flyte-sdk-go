package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// WantsInterface reports whether argv asks for the interface descriptor rather
// than a run. Answered before anything else touches env or the network, so
// `docker run --rm <image> describe-interface` works with no setup at all.
//
// Safe to key off argv[1]: the backend's first token is always an action name
// ("a0", or a base36 hash) and can never be a hyphenated literal.
func WantsInterface() bool {
	return len(os.Args) > 1 && os.Args[1] == "describe-interface"
}

// PrintInterfaces prints one descriptor JSON line per registered task — the
// output of `<binary> describe-interface`, consumed by flyteplugins-go.
func PrintInterfaces() {
	for _, name := range ListTasks() {
		t, _ := GetTask(name)
		fmt.Println(t.Interface().DescriptorJSON(name))
	}
}

// Main is the one-shot worker entrypoint: call it from main() after tasks are
// registered. It honors the backend's container arg/env contract, runs the
// selected task, and uploads outputs.pb or error.pb.
//
// Task failure travels via error.pb — the process exits 0 either way, matching
// the Python and Rust runtimes. A nonzero exit means the worker could not even
// start (e.g. run from a shell with no configuration).
func Main() {
	os.Exit(runMain())
}

func runMain() int {
	if WantsInterface() {
		PrintInterfaces()
		return 0
	}

	cfg, err := ResolveConfig(ParseArgs(os.Args[1:]))
	if err != nil {
		printConfigHelp(err)
		return 1
	}
	task, err := resolveTask(cfg.TaskName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flyte worker: %v\n", err)
		return 1
	}

	ctx := context.Background()
	store := NewStorage()
	// The task's own failure is reported via error.pb inside Execute; the
	// container itself exits 0.
	_ = Execute(ctx, task, store, cfg, IsRetryAttempt(envNonEmpty))
	return 0
}

func printConfigHelp(err error) {
	bin := "<binary>"
	if len(os.Args) > 0 {
		bin = filepath.Base(os.Args[0])
	}
	fmt.Fprintf(os.Stderr, "flyte worker configuration error: %v\n\n", err)
	fmt.Fprintf(os.Stderr,
		"'%[1]s' is a task's in-container worker: the Flyte backend launches it and\n"+
			"supplies this configuration through container args and environment variables,\n"+
			"so running the binary from a shell is expected to stop here. To use the tasks\n"+
			"directly:\n"+
			"  - run them in-process, no backend needed:  `go test`, or call them as functions\n"+
			"  - print their interfaces:                  `%[1]s describe-interface`\n"+
			"If this IS a backend-launched task container, the values listed above are\n"+
			"missing from the pod: check the container's env and args in the pod spec.\n", bin)
}

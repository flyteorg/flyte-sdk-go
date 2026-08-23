// The basics of authoring a Flyte task in Go: plain functions registered in
// init(), a shared TaskEnvironment, a struct traveling between steps as
// msgpack, and one self-describing binary.
//
//	go build -o bin/hello .        # local build for interface discovery
//	./bin/hello describe-interface # what the launcher reads
//
// Deploy and run with flyteplugins-go (see task.py in this directory).
package main

import (
	"fmt"
	"strings"

	flyteruntime "github.com/unionai/flyte-sdk-go/flyte/runtime"
)

var env = &flyteruntime.TaskEnvironment{
	Name: "hello_env",
	Resources: flyteruntime.Resources{
		CPU:    flyteruntime.NewCPU("500m"),
		Memory: flyteruntime.NewMemory("512Mi"),
	},
}

// Stats travels between Go and Python as msgpack; the tags are the
// cross-language field names.
type Stats struct {
	Mean  float64 `msgpack:"mean"`
	Count int64   `msgpack:"count"`
	Label string  `msgpack:"label"`
}

func myTask(ctx flyteruntime.Context, x int64, label string) (string, error) {
	doubled := x * 2
	stats := Stats{Mean: float64(doubled), Count: 1, Label: label}
	return describe(stats), nil
}

func describe(s Stats) string {
	return fmt.Sprintf("%s: mean=%.1f over %d values", strings.ToUpper(s.Label), s.Mean, s.Count)
}

func init() {
	flyteruntime.RegisterTask("my_task", myTask, env, flyteruntime.WithInputNames("x", "label"))
}

func main() { flyteruntime.Main() }

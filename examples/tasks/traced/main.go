// The point of traces: a recorded step is not re-run on retry. Mirrors
// flyte-sdk-rust's examples/retry-replay.
//
// slowStep sleeps two seconds and is traced; the task fails on purpose on its
// first attempt (FLYTE_ATTEMPT_NUMBER == 0). The retry replays slowStep's
// recording instantly and then succeeds — watch the logs for "replaying
// recorded trace". Deploy with retries enabled (see task.py).
package main

import (
	"fmt"
	"os"
	"time"

	flyteruntime "github.com/unionai/flyte-sdk-go/flyte/runtime"
)

var env = &flyteruntime.TaskEnvironment{Name: "traced_env"}

func slowStep(ctx flyteruntime.Context, seed int64) (int64, error) {
	time.Sleep(2 * time.Second)
	return seed * seed, nil
}

func flaky(ctx flyteruntime.Context, seed int64) (string, error) {
	squared, err := flyteruntime.Trace[int64](ctx, slowStep, seed).Get()
	if err != nil {
		return "", err
	}
	if os.Getenv("FLYTE_ATTEMPT_NUMBER") == "" || os.Getenv("FLYTE_ATTEMPT_NUMBER") == "0" {
		return "", flyteruntime.UserErrorf("FlakyError",
			"failing on purpose so the retry can replay the recorded step")
	}
	return fmt.Sprintf("seed %d squared is %d (slow step replayed, not re-run)", seed, squared), nil
}

func init() {
	flyteruntime.RegisterTask("flaky", flaky, env, flyteruntime.WithInputNames("seed"))
}

func main() { flyteruntime.Main() }

# flyteplugins-go

Launch Go Flyte tasks from Python — the deploy-time companion to
[flyte-sdk-go](https://github.com/unionai/flyte-sdk-go)'s `flyte/runtime`
worker package, mirroring
[flyteplugins-rs](https://github.com/flyteorg/flyte-sdk-rs/tree/main/python/flyteplugins-rs).

A Go task container runs a compiled binary and no Python. This package reads
the worker binary's self-described interface (`<binary> describe-interface`,
one JSON line per registered task), declares the matching Flyte task, and
builds the worker image from Go sources:

```python
from pathlib import Path
import flyteplugins_go as fgo

my_task, go_env = fgo.go_task(
    module_dir=Path(__file__).parent,
    binary="hello",
)
```

Then `flyte run task.py my_task --x 21`.

Pass `reuse=flyte.ReusePolicy(...)` to run the task in a warm-container pool —
the Go side opts in with `reuse.Main()` from
`github.com/unionai/union-reuse-go`.

Local dev convention: `go build -o bin/<binary> .` in the module directory so
the launcher can read the live descriptor; the committed
`_generated_interface.py` fallback covers containers with no Go toolchain.

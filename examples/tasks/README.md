# Authoring tasks in Go

Each folder is a Go module (or `main` package) whose binary **is** the task
container, built on `flyte/runtime` — the worker-side half of this SDK. Deploy
and launch with the [`flyteplugins-go`](../../python/flyteplugins-go) Python
companion: the binary describes its own interface
(`<binary> describe-interface`), so the Go signature is the only place inputs
and outputs are written down.

| Example | Shows | flyte-sdk-rs counterpart |
|---|---|---|
| [`hello`](hello) | The basics: `RegisterTask` in `init()`, a shared `TaskEnvironment`, a struct traveling as msgpack, `flyteruntime.Main()`. | [`hello-trace`](https://github.com/flyteorg/flyte-sdk-rs/tree/main/examples/hello-trace) |
| [`traced`](traced) | Traces: a slow step recorded on attempt 1 and replayed (not re-run) on the retry. | [`retry-replay`](https://github.com/flyteorg/flyte-sdk-rs/tree/main/examples/retry-replay) |

Warm containers (`reuse.Main()` + `ReusePolicy`) live with the reuse module in
[unionai/union-reuse-go](https://github.com/unionai/union-reuse-go) (see its
`examples/reusable`), the same split as Rust's
[union-reuse](https://github.com/unionai/union-reuse).

## Running one

```bash
cd examples/tasks/hello
go build -o bin/hello .            # local build: flyteplugins-go reads its descriptor
flyte run task.py my_task --x 21 --label demo
```

`task.py` needs `flyteplugins-go` installed (`pip install -e python/flyteplugins-go`
from the repo root) plus a configured `flyte` CLI. The committed
`_generated_interface.py` beside each example lets the task be declared where
no Go toolchain exists (inside a container); any launch with a built binary
refreshes it.

For the devbox/minio setup, export `AWS_ENDPOINT_URL` so the worker's S3
client targets minio (path-style addressing is applied automatically).

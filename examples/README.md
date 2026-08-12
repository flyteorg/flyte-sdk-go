# Examples

Each folder is a self-contained `main` package showing one way to drive a
Flyte control plane with the Go SDK. The SDK is a **remote-control SDK**:
tasks are authored and deployed with the
[Python SDK](https://github.com/flyteorg/flyte-sdk) or
[Rust SDK](https://github.com/flyteorg/flyte-sdk-rs); Go launches, monitors,
signals, and recovers runs of tasks already deployed.

Four of the examples deliberately mirror the
[flyte-sdk-rs examples](https://github.com/flyteorg/flyte-sdk-rs/tree/main/examples):
the Rust crate authors the task, the Go program here is the other side of that
boundary. Deploy the matching Rust task once and the default `-task` flag
values line up.

| Example | Shows | flyte-sdk-rs counterpart |
|---|---|---|
| [`basic`](basic) | The quickstart: `Init`, `GetTask`, `Run` with typed inputs, `Wait`, `Outputs`, and a streaming `Watch`. | — |
| [`hello-run`](hello-run) | Launching a deployed task by reference and reading outputs back as native Go values. | [`hello-trace`](https://github.com/flyteorg/flyte-sdk-rs/tree/main/examples/hello-trace) — the Rust side authors the task this launches. |
| [`concurrent-runs`](concurrent-runs) | N runs in flight at once: `RunHandle` is safe for concurrent use, so fan-out is plain goroutines. | [`concurrent-traces`](https://github.com/flyteorg/flyte-sdk-rs/tree/main/examples/concurrent-traces) — same fan-out, one level up. |
| [`human-approval`](human-approval) | The reviewer's side of a paused run: `ListConditions`, read the prompts, `Signal` typed answers, watch the task resume. | [`human-approval`](https://github.com/flyteorg/flyte-sdk-rs/tree/main/examples/human-approval) — the Rust task raises the questions; this program answers them. |
| [`recover-run`](recover-run) | Recovering a failed run with `WithRecover`: succeeded actions land `RECOVERED` with outputs reused, only the failed part re-executes. | [`retry-replay`](https://github.com/flyteorg/flyte-sdk-rs/tree/main/examples/retry-replay) — replay within a task; recovery is the same idea across runs. |

Not mirrored: `custom-image` — image building is task authoring, which stays
in the Python/Rust SDKs by design.

## Running any of them

```bash
FLYTE_ENDPOINT=my-org.example.com \
FLYTE_PROJECT=my-project \
FLYTE_DOMAIN=development \
go run ./examples/<name> [flags]
```

Auth: set `FLYTE_API_KEY` for headless client-secret auth, or
`FLYTE_AUTH_COMMAND` for an external command that prints a bearer token;
with neither, the browser PKCE flow opens on first use. Run
`go run ./examples/<name> -h` for the example's flags.

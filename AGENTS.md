# Agent guide: flyte-sdk-go

Go SDK for Flyte v2: launch, monitor, signal, and recover runs of tasks
deployed on a Flyte control plane. It is a **remote-control SDK, not a task
runtime** — task authoring and execution stay in the
[Python SDK](https://github.com/flyteorg/flyte-sdk); Go launches deployed
tasks by reference (`GetTask` + `Run`). Keep that boundary: do not add task
authoring, registration, or deployment APIs.

## Commands

```bash
go build ./...    # build
go test ./...     # unit tests only; no live cluster needed
gofmt -l .        # must print nothing
go vet ./...
```

Note: `flyte/client` tests take ~1 min (auth flow timeouts); scope test runs
to `./flyte/` when iterating on the public API.

## Layout

```
flyte/            Public SDK, one file per concern:
                  initialize.go (Init/Close, global clientset)
                  config.go     (Config, YAML config-file loading)
                  task.go       (TaskRef/TaskDetails, GetTask)
                  run.go        (Run, GetRun, RunHandle, watch loop)
                  options.go    (RunOption builders + RelationType)
                  action.go     (Action handle: list/get/refresh/watch/abort)
                  condition.go  (Condition handle + Signal)
                  types.go      (Go ⇄ Flyte literal conversion)
flyte/client/     Connect clientset builder, interceptors, auth flows (PKCE,
                  device flow, client credentials, API key, external command),
                  token caching (keyring/in-memory)
examples/         Runnable examples against a live cluster, one directory per
                  scenario (several mirror the flyte-sdk-rs examples from the
                  launching side; see examples/README.md)
```

## Design rules

- **Mirror the Python SDK.** Public API names and semantics follow the Python
  `flyte` package (`Run` ⇔ `with_runcontext(...).run`, `GetRun` ⇔
  `Run.get`, options ⇔ `flyte.rerun` parameters). When adding surface, find
  the Python equivalent first and match its behavior; diverge only to stay
  idiomatic Go.
- **Thin wrappers only.** Everything is a typed veneer over the flyteidl2
  Connect services (`RunService`, `TaskService`, `DataProxyService`). No
  client-side state machines or caching beyond the token cache.
- **Don't pre-validate what the server validates.** Callers (notably
  functional test suites) rely on observing the server's typed Connect codes
  (`InvalidArgument`, `FailedPrecondition`, `NotFound`). E.g. `Signal` sends
  any supported payload type and lets the server reject mismatches.
- **Degrade gracefully on older control planes.** Data-proxy RPCs fall back
  on `connect.CodeUnimplemented` (see `Run` input offload and
  `RunHandle.Outputs`); follow that pattern for new data-path RPCs.
- **Wrap errors with context** (`fmt.Errorf("failed to ...: %w", err)`), and
  translate well-known codes into friendly messages where the caller can act
  (see `CodeAlreadyExists` in `Run`, `CodeNotFound` in `GetTask`/`GetRun`).

## Proto dependency

Generated flyteidl2 protos come from `github.com/flyteorg/flyte/v2`. Its
version is **pinned to match the Python SDK release**: the flyteidl2 Go module
version tracks the `flyteidl2==X.Y.Z` pin in
[flyteorg/flyte-sdk's pyproject.toml](https://github.com/flyteorg/flyte-sdk/blob/main/pyproject.toml).
When bumping, check that pin for the targeted release, then `go get
github.com/flyteorg/flyte/v2@vX.Y.Z && go mod tidy` and fix any renamed
fields.

## Proto/API gotchas

- Full action details (`ErrorInfo`/`AbortInfo`/`SignalInfo` oneof, condition
  spec, per-attempt records) exist **only** on `GetActionDetails` responses.
  `ListActions` returns lightweight actions — hence `Action.Refresh` and the
  per-condition fetch in `ListConditions`.
- `ACTION_PHASE_RECOVERED` is a terminal, successful phase; keep it in
  `isTerminalPhase` and the `Wait` success cases.
- A recovery run's source reference lives only in `RunSpec.relation.related_to`
  (`relation_type = RECOVER`); the `Recover` message carries only
  `force_rerun_actions`. `WithRecover` sets both — keep them consistent.
- `SignalEventRequest` needs the condition's **parent action name**
  (`ActionMetadata.parent`) alongside the action id.
- Watch streams drop on idle/rollouts; reuse `watchActionPhases` (reconnect
  with linear backoff) rather than hand-rolling stream loops.

## Conventions

- Tests: `testify` (`assert`/`require`), table-driven, colocated
  `*_test.go`; pure unit tests — construct protos directly, no network.
- Options follow the functional-options pattern in `options.go`; repeated
  options accumulate (labels, env vars, force-rerun names).
- Doc comments on every exported symbol; reference the Python SDK equivalent
  where one exists.
- Sign off commits (`git commit -s`).

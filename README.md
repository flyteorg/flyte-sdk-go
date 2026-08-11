# Flyte Go SDK

**Launch and monitor Flyte task runs from Go — control-plane parity with the
[Python `flyte` SDK](https://github.com/flyteorg/flyte-sdk).**

[![Go Reference](https://pkg.go.dev/badge/github.com/unionai/flyte-sdk-go.svg)](https://pkg.go.dev/github.com/unionai/flyte-sdk-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/unionai/flyte-sdk-go)](https://goreportcard.com/report/github.com/unionai/flyte-sdk-go)
[![Go Version](https://img.shields.io/github/go-mod/go-version/unionai/flyte-sdk-go)](go.mod)
[![License](https://img.shields.io/badge/license-Apache%202.0-orange)](LICENSE)

It mirrors the remote-execution surface of the Python SDK while staying
idiomatic Go, and is built to be embedded in services: import, `Init` once,
then `Run` tasks.

## Install

```bash
go get github.com/unionai/flyte-sdk-go
```

## Quick start

```go
package main

import (
	"context"
	"fmt"

	"github.com/unionai/flyte-sdk-go/flyte"
)

func main() {
	ctx := context.Background()

	flyte.Init(ctx, flyte.Config{Endpoint: "my-org.example.com", Project: "my-project", Domain: "development"})
	defer flyte.Close()

	task, _ := flyte.GetTask(ctx, flyte.TaskRef{Name: "my_env.my_task"}) // latest version
	run, _ := flyte.Run(ctx, task, flyte.Inputs{"x": 5})
	run.Wait(ctx)

	outputs, _ := run.Outputs(ctx)
	fmt.Println(outputs)
}
```

[`example/main.go`](example/main.go) is a fuller, runnable version against a
live cluster:

```bash
FLYTE_ENDPOINT=my-org.example.com \
FLYTE_PROJECT=my-project FLYTE_DOMAIN=development \
go run ./example
```

Set `FLYTE_API_KEY` for headless auth, or `FLYTE_AUTH_COMMAND` to supply a
token-printing command; otherwise the browser PKCE flow is used.

## Features

- **Remote task execution** — fetch deployed tasks (auto-resolving the latest
  version, like Python's `auto_version="latest"`) and launch runs by reference.
- **Python-SDK-parity launch path** — inputs are validated against the task's
  typed interface, registered defaults are applied, and payloads are offloaded
  through the data proxy before `CreateRun`, exactly like the Python SDK.
- **Typed inputs/outputs** — numbers, strings, bools, slices, maps, structs
  (via JSON), `time.Time` (DATETIME) and `time.Duration` (DURATION), converted
  with the same JSON⇄literal converter the Flyte backend uses.
- **Monitoring** — `Wait` blocks to terminal phase; `Watch` streams updates
  over the same `WatchActionDetails` stream the Python SDK uses; `Abort`
  cancels a run.
- **Existing runs and actions** — `GetRun` attaches to a run launched
  elsewhere; `ListActions`/`GetAction` expose each action (task, condition,
  trace) with typed phase, attempt, and error/abort/signal accessors, plus
  per-action `Watch`, `Wait`, and `Abort`.
- **Conditions** — list a run's approval gates and `Signal` them with a bool,
  string, integer, or float, like the Python SDK's condition workflow.
- **Recovery and reruns** — `WithRecover` reuses succeeded actions from a
  failed run (with `WithForceRerunActions` as the escape hatch);
  `WithRelation` records rerun provenance.
- **All the auth flows** — PKCE (default, browser), API key, OAuth2
  client credentials, device flow, and external token commands.
- **Config-file compatible** — reads flytectl/uctl-style `config.yaml` files
  from the same search paths as the Python SDK.

## Initialization

`Init` takes a plain config struct mirroring the Python SDK's `flyte.init()`
parameters:

```go
err := flyte.Init(ctx, flyte.Config{
    Endpoint: "my-org.example.com", // bare host, https:// URL, or dns:/// target
    Project:  "my-project",
    Domain:   "development",
    // Org defaults to the endpoint's first DNS label ("my-org" here).
})
defer flyte.Close()
```

Or from a config file (searched in `./config.yaml`, `./.flyte/config.yaml`,
`<git root>/.flyte/config.yaml`, `$UCTL_CONFIG`, `$FLYTECTL_CONFIG`,
`~/.union/config.yaml`, `~/.flyte/config.yaml` — same order as the Python SDK):

```go
err := flyte.InitFromConfig(ctx, "") // "" = auto-discover
```

```yaml
admin:
  endpoint: dns:///my-org.example.com
  authType: Pkce
task:
  org: my-org
  project: my-project
  domain: development
```

### Authentication

| Mode | Config | Notes |
|------|--------|-------|
| PKCE (default) | *(nothing)* | Opens a browser to log in. |
| API key | `APIKey: "..."` or `flyte.InitFromAPIKey(ctx, "")` | Reads `FLYTE_API_KEY` when empty. Decodes to endpoint + client credentials, so `Endpoint` may be omitted. |
| Client credentials | `ClientID` + one of `ClientSecret`, `ClientSecretEnvVar`, `ClientSecretLocation` | Headless OAuth2 client-credentials flow. |
| Device flow | `AuthType: flyte.AuthTypeDeviceFlow` | Prints a URL + code; no browser needed on the host. |
| External command | `AuthType: flyte.AuthTypeExternalCommand`, `Command: []string{...}` | The command must print a bearer token. |

Interactive logins (PKCE, device flow) are cached in the OS keyring — the same
keychain entries the Python SDK/CLI uses, so the two share logins and re-runs
don't reopen the browser. Set `DisableKeyring: true` to opt out. Headless
flows never touch the keyring, so services embedding the SDK with an API key
or client secret have no keyring dependency.

```go
// API key (headless)
err := flyte.Init(ctx, flyte.Config{
    APIKey:  os.Getenv("FLYTE_API_KEY"),
    Project: "my-project",
    Domain:  "development",
})

// Client credentials
err := flyte.Init(ctx, flyte.Config{
    Endpoint:           "my-org.example.com",
    Project:            "my-project",
    Domain:             "development",
    ClientID:           "my-app",
    ClientSecretEnvVar: "FLYTE_CLIENT_SECRET",
})
```

## Fetching tasks

```go
// Latest deployed version (Python: Task.get(name, auto_version="latest"))
task, err := flyte.GetTask(ctx, flyte.TaskRef{Name: "my_env.my_task"})

// Pinned version / different project
task, err := flyte.GetTask(ctx, flyte.TaskRef{
    Name:    "my_env.my_task",
    Version: "abc123",
    Project: "other-project",
})

task.Name()      // fully qualified name
task.Version()   // resolved version
task.Interface() // typed input/output interface
```

## Launching runs

`Run` accepts a fetched `*TaskDetails` or a `TaskRef` directly (fetched on
demand). Options mirror Python's `with_runcontext(...)`:

```go
run, err := flyte.Run(ctx, task,
    flyte.Inputs{
        "x":            5,
        "when":         time.Now(),          // DATETIME
        "window":       90 * time.Minute,    // DURATION
        "names":        []string{"a", "b"},  // lists
        "weights":      map[string]float64{"a": 0.9}, // maps
    },
    flyte.WithRunName("my-run-001"),      // omit for a server-generated name
    flyte.WithEnvVar("LOG_LEVEL", "DEBUG"),
    flyte.WithLabels(map[string]string{"team": "data"}),
    flyte.WithAnnotation("owner", "alice@example.com"),
    flyte.WithServiceAccount("runner"),
    flyte.WithInterruptible(true),
    flyte.WithOverwriteCache(true),
    flyte.WithQueue("gpu-queue"),
    flyte.WithMaxActionConcurrency(10),
)
```

Inputs the task declares defaults for may be omitted; missing required inputs
and unknown input names fail before anything hits the cluster.

To derive a run from an existing one in the same project/domain:

```go
// Recovery: actions that succeeded in "failed-run" are skipped and their
// outputs reused; add WithForceRerunActions to re-execute specific ones anyway.
run, err := flyte.Run(ctx, task, inputs,
    flyte.WithRecover("failed-run"),
    flyte.WithForceRerunActions("a1"),
)

// Provenance only: record the source run without recovery semantics.
run, err = flyte.Run(ctx, task, inputs,
    flyte.WithRelation("src-run", flyte.RelationTypeRerun),
)
```

## Monitoring and results

```go
fmt.Println(run.Name(), run.URL())

// Block until terminal phase
if err := run.Wait(ctx); err != nil { ... }

// Or stream updates
updates, _ := run.Watch(ctx)
for u := range updates {
    fmt.Printf("[%s] %s\n", u.Timestamp.Format(time.TimeOnly), u.Phase)
}

// Outputs as native Go values (waits for success first)
outputs, err := run.Outputs(ctx) // map[string]any, e.g. {"o0": 49}

// Abort
err = run.Abort(ctx, "superseded")
```

## Existing runs, actions, and conditions

`GetRun` attaches a handle to a run launched elsewhere; `ListActions` /
`GetAction` expose the run's individual actions (tasks, conditions, traces)
with typed status accessors:

```go
run, err := flyte.GetRun(ctx, "my-run-001")

actions, err := run.ListActions(ctx) // lightweight: identity, metadata, phase
a, err := run.GetAction(ctx, "a1")   // full details: error/abort/signal info, attempts

a.Phase()          // "ACTION_PHASE_RUNNING"
a.Type()           // flyte.ActionTypeTask | ActionTypeCondition | ActionTypeTrace
a.Attempts()       // attempt count so far
a.ErrorInfo()      // failure message + USER/SYSTEM kind, nil unless failed
a.AbortInfo()      // abort reason + principal, nil unless aborted
a.RecoveredFrom()  // source action when recovered, else nil
err = a.Refresh(ctx)            // re-poll a live action
err = a.Wait(ctx)               // block until terminal
err = a.Abort(ctx, "stuck")     // abort this action only
```

Condition actions (created by tasks via `flyte.new_condition` in Python) pause
until signalled:

```go
conds, err := run.ListConditions(ctx)
cond, err := run.GetCondition(ctx, "approval-gate")
fmt.Println(cond.Prompt(), cond.Description())

err = cond.Signal(ctx, true) // bool, string, integer, or float,
                             // validated server-side against the declared type
```

## Package layout

```
flyte/            Public SDK: Init, Config, GetTask, Run, GetRun, RunHandle,
                  Action, Condition
flyte/client/     Connect clientset, auth flows (PKCE, device flow, client
                  credentials, external command), token caching
example/          Runnable end-to-end example
```

The SDK talks to the control plane over the [Connect
protocol](https://connectrpc.com) (like the Python SDK) using the flyteidl2
services: `TaskService` (task discovery), `DataProxyService` (input offload,
action data), `RunService` (create, watch, abort) and `AuthMetadataService`
(anonymous OAuth discovery).

## Learn more

- **[flyte-sdk](https://github.com/flyteorg/flyte-sdk)** — the Python `flyte`
  SDK this package mirrors
- **[flyte](https://github.com/flyteorg/flyte)** — the Flyte project
- **[Documentation](https://www.union.ai/docs/v2/flyte/user-guide/running-locally/)**
- **[Slack](https://slack.flyte.org/)** |
  **[GitHub Discussions](https://github.com/flyteorg/flyte/discussions)** |
  **[Issues](https://github.com/unionai/flyte-sdk-go/issues)**

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for how to
build, test, and submit a PR, or join us on [slack.flyte.org](https://slack.flyte.org).

## License

Apache 2.0 — see [LICENSE](LICENSE).

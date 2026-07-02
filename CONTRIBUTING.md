# Contributing to the Flyte Go SDK

Thanks for your interest in contributing! This project follows the same
spirit as [flyteorg/flyte-sdk](https://github.com/flyteorg/flyte-sdk) — small,
focused PRs are easiest to review and merge.

## Development setup

Requires Go 1.26+ (see [go.mod](go.mod)).

```bash
git clone https://github.com/unionai/flyte-sdk-go.git
cd flyte-sdk-go
go build ./...
```

## Running tests

```bash
go test ./...
```

## Before opening a PR

```bash
gofmt -l .        # should print nothing
go vet ./...
go test ./...
```

## Submitting changes

1. Fork the repo and create a branch off `main`.
2. Make your change, with tests for new behavior.
3. Ensure the checks above pass.
4. Open a PR with a clear description of the change and why it's needed.

For anything beyond a small fix — new public API, behavioral changes, new
auth flows — please open an issue or start a thread in
[Slack](https://slack.flyte.org/) first so we can align on the approach
before you invest time in an implementation.

## Reporting bugs

Open a [GitHub issue](https://github.com/unionai/flyte-sdk-go/issues) with a
minimal repro, the SDK version (`go.mod` / `go list -m github.com/unionai/flyte-sdk-go`),
and the Flyte control plane version if relevant.

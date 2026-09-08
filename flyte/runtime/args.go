package runtime

import (
	"os"
	"strconv"
	"strings"

	commonpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
)

// WorkerConfig is the raw container arg contract, one field per flag. Empty
// string means the flag was absent or its value was an unsubstituted template.
//
// This is a wire contract shared with the Rust SDK's worker (worker.rs
// parse_args) and the Python companion's container_args — change it nowhere
// unilaterally.
type WorkerConfig struct {
	TaskName   string // --task: which registered task to run (Go extension)
	InputsURI  string
	OutputPath string
	RunBaseDir string
	ActionName string
	RunName    string
	Project    string
	Domain     string
	Org        string
}

// realValue filters out values the backend failed to substitute:
// `{{.actionName}}`-style templates and empty strings fall back to env.
func realValue(v string) string {
	if strings.HasPrefix(v, "{{") {
		return ""
	}
	return v
}

// ParseArgs parses the container argv (without the binary path). Mirrors the
// Rust worker's parse_args: unknown tokens (the leading action name "a0",
// anything unrecognized) are skipped, known-but-unused flags swallow their
// value, and --resolver ends parsing (everything after is Python resolver
// business).
//
// Only split tokens ("--inputs", "<uri>") are recognized. The backend always
// emits them that way and the Rust worker parses only that form, so
// "--inputs=<uri>" is deliberately an unknown token: adding it here alone
// would make the Go and Rust workers disagree on the same argv.
func ParseArgs(args []string) WorkerConfig {
	var cfg WorkerConfig
	i := 0
	next := func() string {
		if i < len(args) {
			v := args[i]
			i++
			return realValue(v)
		}
		return ""
	}
	for i < len(args) {
		arg := args[i]
		i++
		switch arg {
		case "--inputs", "-i":
			cfg.InputsURI = next()
		case "--outputs-path", "-o":
			cfg.OutputPath = next()
		case "--run-base-dir":
			cfg.RunBaseDir = next()
		case "--name":
			cfg.ActionName = next()
		case "--run-name":
			cfg.RunName = next()
		case "--project":
			cfg.Project = next()
		case "--domain":
			cfg.Domain = next()
		case "--org":
			cfg.Org = next()
		case "--task":
			cfg.TaskName = next()
		case "--version", "--raw-data-path", "--checkpoint-path", "--prev-checkpoint",
			"--run-start-time", "--image-cache", "--tgz", "--pkl", "--dest", "--interface":
			// Known flags we accept but don't use.
			if i < len(args) {
				i++
			}
		case "--resolver":
			// Resolver args are a Python-ism; everything after is theirs.
			return cfg
		default:
			// Subcommand tokens ("a0") and anything unrecognized: skip.
		}
	}
	return cfg
}

// ResolvedConfig is a WorkerConfig with every gap filled from the environment:
// the complete identity of one action execution.
type ResolvedConfig struct {
	TaskName   string // may be empty: Main falls back to the sole registered task
	InputsURI  string // may be empty: task has no inputs
	OutputPath string
	RunBaseDir string
	ActionName string
	RunID      *commonpb.RunIdentifier
}

// ResolveConfig fills the args the backend did not substitute from the process
// environment. The one-shot container's identity arrives this way: the backend
// injects ACTION_NAME / RUN_NAME / _U_RUN_BASE etc. into the pod.
func ResolveConfig(cfg WorkerConfig) (ResolvedConfig, error) {
	return ResolveConfigWithEnv(cfg, envNonEmpty)
}

// ResolveConfigWithEnv is ResolveConfig reading identity from env instead of
// the process. A reusable container cannot use process env for this: the pod's
// variables are fixed when the replica starts, so they name whichever action
// happened to be first. Per-action identity arrives in-band with each fasttask
// assignment, and this is where it enters.
func ResolveConfigWithEnv(cfg WorkerConfig, env func(string) string) (ResolvedConfig, error) {
	// Collect every gap before failing: a partially configured container (one
	// env var lost) and a bare `./binary` from a shell (all of them absent)
	// look identical when only the first missing key is reported.
	var missing []string
	require := func(value, what string) string {
		if value == "" {
			missing = append(missing, what)
		}
		return value
	}
	firstNonEmpty := func(values ...string) string {
		for _, v := range values {
			if v != "" {
				return v
			}
		}
		return ""
	}
	actionName := require(firstNonEmpty(cfg.ActionName, env("ACTION_NAME")), "ACTION_NAME (or --name)")
	runName := require(firstNonEmpty(cfg.RunName, env("RUN_NAME")), "RUN_NAME (or --run-name)")
	// GetExecutionEnvVars emits both the execution's and the task definition's
	// project/domain; EXECUTION_ normally wins but TASK_ stays as a fallback
	// (worker-v2's executor reads only the TASK_ pair).
	project := require(
		firstNonEmpty(cfg.Project, env("FLYTE_INTERNAL_EXECUTION_PROJECT"), env("FLYTE_INTERNAL_TASK_PROJECT")),
		"FLYTE_INTERNAL_EXECUTION_PROJECT (or --project)")
	domain := require(
		firstNonEmpty(cfg.Domain, env("FLYTE_INTERNAL_EXECUTION_DOMAIN"), env("FLYTE_INTERNAL_TASK_DOMAIN")),
		"FLYTE_INTERNAL_EXECUTION_DOMAIN (or --domain)")
	org := firstNonEmpty(cfg.Org, env("_U_ORG_NAME"))
	runBaseDir := require(firstNonEmpty(cfg.RunBaseDir, env("_U_RUN_BASE")), "_U_RUN_BASE (or --run-base-dir)")
	if len(missing) > 0 {
		return ResolvedConfig{}, SystemErrorf("missing required configuration: %s", strings.Join(missing, ", "))
	}
	outputPath := cfg.OutputPath
	if outputPath == "" {
		outputPath = joinURI(runBaseDir, actionName)
	}
	return ResolvedConfig{
		TaskName:   cfg.TaskName,
		InputsURI:  cfg.InputsURI,
		OutputPath: outputPath,
		RunBaseDir: runBaseDir,
		ActionName: actionName,
		RunID: &commonpb.RunIdentifier{
			Org:     org,
			Project: project,
			Domain:  domain,
			Name:    runName,
		},
	}, nil
}

// IsRetryAttempt reports whether env describes a retry: FLYTE_ATTEMPT_NUMBER is
// 0-based, so >0 means a previous attempt may have recorded traces to replay.
func IsRetryAttempt(env func(string) string) bool {
	n, err := strconv.Atoi(env("FLYTE_ATTEMPT_NUMBER"))
	return err == nil && n > 0
}

func envNonEmpty(key string) string {
	return os.Getenv(key)
}

func joinURI(base, name string) string {
	return strings.TrimRight(base, "/") + "/" + name
}

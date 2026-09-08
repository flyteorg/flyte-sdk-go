package runtime

// Table tests ported from flyte-sdk-rust's worker.rs — the arg/env contract is
// shared wire behavior; keep the cases in sync.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseArgs(t *testing.T) {
	t.Run("backend argv with substituted values", func(t *testing.T) {
		cfg := ParseArgs([]string{
			"a0", "--task", "add",
			"--inputs", "s3://bucket/in/inputs.pb",
			"--outputs-path", "s3://bucket/out",
			"--run-name", "r1", "--name", "a0",
		})
		assert.Equal(t, "add", cfg.TaskName)
		assert.Equal(t, "s3://bucket/in/inputs.pb", cfg.InputsURI)
		assert.Equal(t, "s3://bucket/out", cfg.OutputPath)
		assert.Equal(t, "r1", cfg.RunName)
		assert.Equal(t, "a0", cfg.ActionName)
	})

	t.Run("unsubstituted templates are discarded", func(t *testing.T) {
		cfg := ParseArgs([]string{
			"--inputs", "{{.input}}",
			"--run-name", "{{.runName}}",
			"--name", "{{.actionName}}",
		})
		assert.Equal(t, WorkerConfig{}, cfg)
	})

	t.Run("ignored flags swallow their value", func(t *testing.T) {
		cfg := ParseArgs([]string{
			"--version", "v1", "--raw-data-path", "s3://x", "--checkpoint-path", "s3://c",
			"--image-cache", "abc", "--name", "a0",
		})
		assert.Equal(t, "a0", cfg.ActionName)
		assert.Empty(t, cfg.InputsURI)
	})

	t.Run("resolver stops parsing", func(t *testing.T) {
		cfg := ParseArgs([]string{"--name", "a0", "--resolver", "--inputs", "s3://late"})
		assert.Equal(t, "a0", cfg.ActionName)
		assert.Empty(t, cfg.InputsURI)
	})

	t.Run("unknown tokens are skipped", func(t *testing.T) {
		cfg := ParseArgs([]string{"whatever", "--mystery-flag", "--inputs", "s3://in"})
		assert.Equal(t, "s3://in", cfg.InputsURI)
	})

	t.Run("flag=value form is not part of the contract", func(t *testing.T) {
		// Shared with the Rust worker: split tokens only. Pinned so the form is
		// not added here unilaterally.
		cfg := ParseArgs([]string{"--inputs=s3://in", "--name", "a0"})
		assert.Empty(t, cfg.InputsURI)
		assert.Equal(t, "a0", cfg.ActionName)
	})

	t.Run("short flags", func(t *testing.T) {
		cfg := ParseArgs([]string{"-i", "s3://in", "-o", "s3://out"})
		assert.Equal(t, "s3://in", cfg.InputsURI)
		assert.Equal(t, "s3://out", cfg.OutputPath)
	})
}

func testEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveConfig(t *testing.T) {
	full := map[string]string{
		"ACTION_NAME":                      "a0",
		"RUN_NAME":                         "r1",
		"FLYTE_INTERNAL_EXECUTION_PROJECT": "proj",
		"FLYTE_INTERNAL_EXECUTION_DOMAIN":  "dev",
		"_U_ORG_NAME":                      "acme",
		"_U_RUN_BASE":                      "s3://bucket/runs/r1",
	}

	t.Run("env fills unsubstituted args", func(t *testing.T) {
		cfg, err := ResolveConfigWithEnv(WorkerConfig{}, testEnv(full))
		require.NoError(t, err)
		assert.Equal(t, "a0", cfg.ActionName)
		assert.Equal(t, "s3://bucket/runs/r1", cfg.RunBaseDir)
		assert.Equal(t, "s3://bucket/runs/r1/a0", cfg.OutputPath)
		assert.Equal(t, "proj", cfg.RunID.GetProject())
		assert.Equal(t, "dev", cfg.RunID.GetDomain())
		assert.Equal(t, "acme", cfg.RunID.GetOrg())
		assert.Equal(t, "r1", cfg.RunID.GetName())
	})

	t.Run("args win over env", func(t *testing.T) {
		cfg, err := ResolveConfigWithEnv(WorkerConfig{ActionName: "override", OutputPath: "s3://elsewhere"}, testEnv(full))
		require.NoError(t, err)
		assert.Equal(t, "override", cfg.ActionName)
		assert.Equal(t, "s3://elsewhere", cfg.OutputPath)
	})

	t.Run("TASK_ project/domain fallback", func(t *testing.T) {
		env := map[string]string{
			"ACTION_NAME": "a0", "RUN_NAME": "r1", "_U_RUN_BASE": "/tmp/base",
			"FLYTE_INTERNAL_TASK_PROJECT": "tp", "FLYTE_INTERNAL_TASK_DOMAIN": "td",
		}
		cfg, err := ResolveConfigWithEnv(WorkerConfig{}, testEnv(env))
		require.NoError(t, err)
		assert.Equal(t, "tp", cfg.RunID.GetProject())
		assert.Equal(t, "td", cfg.RunID.GetDomain())
	})

	t.Run("all missing keys reported at once", func(t *testing.T) {
		_, err := ResolveConfigWithEnv(WorkerConfig{}, testEnv(nil))
		require.Error(t, err)
		for _, want := range []string{"ACTION_NAME", "RUN_NAME", "FLYTE_INTERNAL_EXECUTION_PROJECT",
			"FLYTE_INTERNAL_EXECUTION_DOMAIN", "_U_RUN_BASE"} {
			assert.Contains(t, err.Error(), want)
		}
		assert.Equal(t, OriginSystem, OriginOf(err))
	})

	t.Run("org is optional", func(t *testing.T) {
		env := map[string]string{}
		for k, v := range full {
			env[k] = v
		}
		delete(env, "_U_ORG_NAME")
		cfg, err := ResolveConfigWithEnv(WorkerConfig{}, testEnv(env))
		require.NoError(t, err)
		assert.Empty(t, cfg.RunID.GetOrg())
	})
}

func TestIsRetryAttempt(t *testing.T) {
	assert.False(t, IsRetryAttempt(testEnv(nil)))
	assert.False(t, IsRetryAttempt(testEnv(map[string]string{"FLYTE_ATTEMPT_NUMBER": "0"})))
	assert.True(t, IsRetryAttempt(testEnv(map[string]string{"FLYTE_ATTEMPT_NUMBER": "1"})))
	assert.False(t, IsRetryAttempt(testEnv(map[string]string{"FLYTE_ATTEMPT_NUMBER": "junk"})))
}

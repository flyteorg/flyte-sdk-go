package runtime

// Single-shot integration tests over file:// storage: the full
// inputs.pb → Execute → outputs.pb / error.pb path with no network.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	commonpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	corepb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func testConfig(t *testing.T, inputsURI string) ResolvedConfig {
	t.Helper()
	base := t.TempDir()
	return ResolvedConfig{
		InputsURI:  inputsURI,
		OutputPath: filepath.Join(base, "a0"),
		RunBaseDir: base,
		ActionName: "a0",
		RunID:      &commonpb.RunIdentifier{Project: "p", Domain: "d", Name: "r1"},
	}
}

func writeInputs(t *testing.T, dir string, names []string, vals ...any) string {
	t.Helper()
	inputs, err := buildInputs(names, reflectValues(vals...))
	require.NoError(t, err)
	data, err := proto.Marshal(inputs)
	require.NoError(t, err)
	uri := filepath.Join(dir, "inputs.pb")
	require.NoError(t, os.WriteFile(uri, data, 0o644))
	return uri
}

func readErrorDoc(t *testing.T, outputPath string) *corepb.ErrorDocument {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outputPath, "error.pb"))
	require.NoError(t, err)
	doc := &corepb.ErrorDocument{}
	require.NoError(t, proto.Unmarshal(data, doc))
	return doc
}

func TestExecuteSuccessWritesOutputs(t *testing.T) {
	task := RegisterTask("exec_add", func(ctx Context, a, b int64) (int64, error) {
		return a + b, nil
	}, nil, WithInputNames("a", "b"))

	cfg := testConfig(t, "")
	cfg.InputsURI = writeInputs(t, cfg.RunBaseDir, []string{"a", "b"}, int64(19), int64(23))

	require.NoError(t, Execute(context.Background(), task, NewStorage(), cfg, false))

	data, err := os.ReadFile(filepath.Join(cfg.OutputPath, "outputs.pb"))
	require.NoError(t, err)
	outputs := &taskpb.Outputs{}
	require.NoError(t, proto.Unmarshal(data, outputs))
	require.Len(t, outputs.GetLiterals(), 1)
	assert.Equal(t, "o0", outputs.GetLiterals()[0].GetName())
	assert.Equal(t, int64(42), outputs.GetLiterals()[0].GetValue().GetScalar().GetPrimitive().GetInteger())
}

func TestExecuteNoInputsTask(t *testing.T) {
	task := RegisterTask("exec_noin", func(ctx Context) (string, error) { return "hi", nil }, nil)
	cfg := testConfig(t, "")
	require.NoError(t, Execute(context.Background(), task, NewStorage(), cfg, false))
	_, err := os.Stat(filepath.Join(cfg.OutputPath, "outputs.pb"))
	assert.NoError(t, err)
}

func TestExecuteUserFailureWritesErrorPb(t *testing.T) {
	task := RegisterTask("exec_fail", func(ctx Context) error {
		return errors.New("business logic said no")
	}, nil)
	cfg := testConfig(t, "")
	err := Execute(context.Background(), task, NewStorage(), cfg, false)
	require.Error(t, err)
	assert.Equal(t, OriginUser, OriginOf(err))

	doc := readErrorDoc(t, cfg.OutputPath)
	assert.Equal(t, "UserError", doc.GetError().GetCode())
	assert.Equal(t, corepb.ExecutionError_USER, doc.GetError().GetOrigin())
	assert.Equal(t, corepb.ContainerError_RECOVERABLE, doc.GetError().GetKind())
	assert.Contains(t, doc.GetError().GetMessage(), "business logic said no")
}

func TestExecutePanicWritesUserErrorPb(t *testing.T) {
	task := RegisterTask("exec_panic", func(ctx Context) error { panic("splat") }, nil)
	cfg := testConfig(t, "")
	err := Execute(context.Background(), task, NewStorage(), cfg, false)
	require.Error(t, err)
	doc := readErrorDoc(t, cfg.OutputPath)
	assert.Equal(t, "PanicError", doc.GetError().GetCode())
	assert.Equal(t, corepb.ExecutionError_USER, doc.GetError().GetOrigin())
}

func TestExecuteExplicitErrorCodesSurvive(t *testing.T) {
	task := RegisterTask("exec_coded", func(ctx Context) error {
		return UserErrorf("QuotaExceeded", "too many widgets")
	}, nil)
	cfg := testConfig(t, "")
	require.Error(t, Execute(context.Background(), task, NewStorage(), cfg, false))
	doc := readErrorDoc(t, cfg.OutputPath)
	assert.Equal(t, "QuotaExceeded", doc.GetError().GetCode())
}

func TestExecuteMissingInputsIsSystemFailure(t *testing.T) {
	task := RegisterTask("exec_sysfail", func(ctx Context, a int64) error { return nil }, nil)
	cfg := testConfig(t, "") // no inputs URI, but the task needs an input
	err := Execute(context.Background(), task, NewStorage(), cfg, false)
	require.Error(t, err)
	assert.Equal(t, OriginSystem, OriginOf(err))
	doc := readErrorDoc(t, cfg.OutputPath)
	assert.Equal(t, "SystemError", doc.GetError().GetCode())
	assert.Equal(t, corepb.ExecutionError_SYSTEM, doc.GetError().GetOrigin())
}

func TestExecuteOversizedOutputIsRefused(t *testing.T) {
	task := RegisterTask("exec_huge", func(ctx Context) (string, error) {
		return strings.Repeat("x", 11*1024*1024), nil
	}, nil)
	cfg := testConfig(t, "")
	err := Execute(context.Background(), task, NewStorage(), cfg, false)
	require.Error(t, err)
	assert.Equal(t, OriginSystem, OriginOf(err))
	assert.Contains(t, err.Error(), "exceeds")
}

// A directory where a file is expected makes the local-storage write fail —
// the test double for an object-store upload failure.
func blockFile(t *testing.T, outputPath, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(outputPath, name), 0o755))
}

func TestExecuteOutputsUploadFailureReportsSystemError(t *testing.T) {
	task := RegisterTask("exec_nooutputs", func(ctx Context) (int64, error) { return 1, nil }, nil)
	cfg := testConfig(t, "")
	blockFile(t, cfg.OutputPath, "outputs.pb")

	err := Execute(context.Background(), task, NewStorage(), cfg, false)
	require.Error(t, err, "a success the backend cannot see is not a success")
	assert.True(t, errors.Is(err, ErrPublish))
	assert.Equal(t, OriginSystem, OriginOf(err))

	// Best effort: the failure is still reported through error.pb, as a system
	// fault, so the action fails visibly instead of hanging on a missing file.
	doc := readErrorDoc(t, cfg.OutputPath)
	assert.Equal(t, "SystemError", doc.GetError().GetCode())
	assert.Equal(t, corepb.ExecutionError_SYSTEM, doc.GetError().GetOrigin())
	assert.Contains(t, doc.GetError().GetMessage(), "outputs upload failed")
}

func TestExecuteErrorUploadFailureIsPublishFailure(t *testing.T) {
	task := RegisterTask("exec_noerrorpb", func(ctx Context) error { return errors.New("user bug") }, nil)
	cfg := testConfig(t, "")
	blockFile(t, cfg.OutputPath, "error.pb")

	err := Execute(context.Background(), task, NewStorage(), cfg, false)
	require.Error(t, err)
	// Neither artifact exists: system fault, not the user's — but the task's
	// own error stays in the chain for callers that want it.
	assert.True(t, errors.Is(err, ErrPublish))
	assert.Equal(t, OriginSystem, OriginOf(err))
	assert.Contains(t, err.Error(), "user bug")
}

func TestExecuteUserFailureIsNotPublishFailure(t *testing.T) {
	task := RegisterTask("exec_plainfail", func(ctx Context) error { return errors.New("nope") }, nil)
	cfg := testConfig(t, "")
	err := Execute(context.Background(), task, NewStorage(), cfg, false)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrPublish), "a reported task failure exits 0")
}

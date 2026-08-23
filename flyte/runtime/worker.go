package runtime

import (
	"context"
	"log/slog"
	"sync"

	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"google.golang.org/protobuf/proto"

	"github.com/unionai/flyte-sdk-go/flyte/runtime/internal/controller"
	istorage "github.com/unionai/flyte-sdk-go/flyte/runtime/internal/storage"
)

// Storage is the runtime's object-storage handle. One Storage caches one
// client per scheme://authority; share it across executions (a reusable
// container passes the same Storage to every assignment) so credential
// resolution is paid once per process.
type Storage struct{ inner *istorage.Storage }

// NewStorage returns an empty Storage.
func NewStorage() *Storage { return &Storage{inner: istorage.New()} }

// runtimeState is the per-action runtime context: the identity of the running
// action plus the resources trace calls need. It travels in context values
// (see withState), so concurrent actions in a reusable container are isolated
// without process globals.
type runtimeState struct {
	storage    *istorage.Storage
	actionName string
	runBaseDir string
	outputPath string
	runID      *runIdentifier
	isRetry    bool

	seq sequencer

	// The trace controller is built lazily on first Trace call: a task that
	// never traces needs no control-plane connection at all.
	ctrlOnce sync.Once
	ctrl     *controller.Controller
	ctrlErr  error
}

type runIdentifier = controller.RunIdentifier

// sequencer is a deterministic per-key call counter (Python's
// TaskCallSequencer). Keys combine a step's identity with its inputs hash so
// repeated identical calls get distinct deterministic names (first call = 1).
type sequencer struct {
	mu     sync.Mutex
	counts map[string]uint32
}

func (s *sequencer) next(key string) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts == nil {
		s.counts = map[string]uint32{}
	}
	s.counts[key]++
	return s.counts[key]
}

type stateCtxKey struct{}
type inTraceCtxKey struct{}

func withState(ctx context.Context, s *runtimeState) context.Context {
	return context.WithValue(ctx, stateCtxKey{}, s)
}

func stateFrom(ctx context.Context) *runtimeState {
	s, _ := ctx.Value(stateCtxKey{}).(*runtimeState)
	return s
}

func inTrace(ctx context.Context) bool {
	v, _ := ctx.Value(inTraceCtxKey{}).(bool)
	return v
}

func (s *runtimeState) controller(ctx context.Context) (*controller.Controller, error) {
	s.ctrlOnce.Do(func() {
		s.ctrl, s.ctrlErr = controller.New(ctx, s.runID)
	})
	return s.ctrl, s.ctrlErr
}

// Execute runs one action to completion: fetch inputs, run the task, upload
// outputs.pb or error.pb.
//
// The returned error is the task's own result, so a caller that must report a
// phase elsewhere — the reusable container answering the fasttask plugin over
// its heartbeat — can tell success from failure (and user from system fault
// via OriginOf). The one-shot worker ignores it: there the artifacts are the
// report.
func Execute(ctx context.Context, t *Task, store *Storage, cfg ResolvedConfig, isRetry bool) error {
	state := &runtimeState{
		storage:    store.inner,
		actionName: cfg.ActionName,
		runBaseDir: cfg.RunBaseDir,
		outputPath: cfg.OutputPath,
		runID:      cfg.RunID,
		isRetry:    isRetry,
	}
	ctx = withState(ctx, state)

	result, runErr := func() (*taskpb.Outputs, error) {
		inputs := &taskpb.Inputs{}
		if cfg.InputsURI != "" {
			data, err := state.storage.Get(ctx, cfg.InputsURI)
			if err != nil {
				return nil, SystemErrorf("failed to fetch inputs: %w", err)
			}
			if err := proto.Unmarshal(data, inputs); err != nil {
				return nil, SystemErrorf("failed to decode inputs: %w", err)
			}
		}
		return t.run(ctx, inputs)
	}()

	var outcome error
	if runErr == nil {
		uri := istorage.Join(cfg.OutputPath, "outputs.pb")
		data, err := proto.Marshal(result)
		if err == nil {
			err = state.storage.Put(ctx, uri, data)
		}
		if err != nil {
			slog.Error("outputs upload failed", "task", t.name, "error", err)
			outcome = SystemErrorf("outputs upload failed: %w", err)
		} else {
			slog.Info("task succeeded, outputs uploaded", "task", t.name)
		}
	} else {
		slog.Error("task failed", "task", t.name, "error", runErr)
		uri := istorage.Join(cfg.OutputPath, "error.pb")
		data, err := proto.Marshal(errorDocument(runErr))
		if err == nil {
			err = state.storage.Put(ctx, uri, data)
		}
		if err != nil {
			slog.Error("error upload failed", "task", t.name, "error", err)
		}
		outcome = runErr
	}

	if state.ctrl != nil {
		state.ctrl.Finalize(cfg.ActionName)
	}
	return outcome
}

// OriginOf classifies an error returned by Execute: user fault or
// system/infrastructure fault. The reusable container uses it to set the
// fasttask system_failure flag so worker deaths don't spend the user's retry
// budget.
func OriginOf(err error) ErrorOrigin {
	return classify(err).Origin
}

// writeErrorDocument writes err as error.pb under outputPath; used by Main for
// failures that happen outside Execute.
func writeErrorDocument(ctx context.Context, store *Storage, outputPath string, err error) {
	uri := istorage.Join(outputPath, "error.pb")
	data, merr := proto.Marshal(errorDocument(err))
	if merr != nil {
		slog.Error("failed to marshal error document", "error", merr)
		return
	}
	if perr := store.inner.Put(ctx, uri, data); perr != nil {
		slog.Error("error upload failed", "error", perr)
	}
}

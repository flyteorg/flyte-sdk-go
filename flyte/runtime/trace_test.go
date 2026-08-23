package runtime

// Trace protocol integration: record on the first attempt, replay on the
// retry, against an in-process fake ActionsService (the Go analog of the Rust
// smoke test's two-attempt flow).

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	actionspb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/actions"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/actions/actionsconnect"
	commonpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	workflowpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeActions is a minimal ActionsService: Enqueue stores trace actions, and
// WatchForUpdates replays them as ActionUpdates, emits the sentinel, then
// holds the stream open — the contract the informer relies on.
type fakeActions struct {
	actionsconnect.UnimplementedActionsServiceHandler

	mu       sync.Mutex
	enqueued []*actionspb.Action
	enqueues atomic.Int32
	dupCode  connect.Code // returned for duplicate enqueues; 0 = store again
}

func (f *fakeActions) Enqueue(ctx context.Context, req *connect.Request[actionspb.EnqueueRequest]) (*connect.Response[actionspb.EnqueueResponse], error) {
	f.enqueues.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.enqueued {
		if a.GetActionId().GetName() == req.Msg.GetAction().GetActionId().GetName() && f.dupCode != 0 {
			return nil, connect.NewError(f.dupCode, nil)
		}
	}
	f.enqueued = append(f.enqueued, req.Msg.GetAction())
	return connect.NewResponse(&actionspb.EnqueueResponse{}), nil
}

func (f *fakeActions) WatchForUpdates(ctx context.Context, req *connect.Request[actionspb.WatchForUpdatesRequest], stream *connect.ServerStream[actionspb.WatchForUpdatesResponse]) error {
	f.mu.Lock()
	snapshot := append([]*actionspb.Action(nil), f.enqueued...)
	f.mu.Unlock()
	for _, a := range snapshot {
		update := &workflowpb.ActionUpdate{
			ActionId:  a.GetActionId(),
			Phase:     commonpb.ActionPhase_ACTION_PHASE_SUCCEEDED,
			OutputUri: a.GetTrace().GetOutputs().GetOutputUri(),
		}
		if err := stream.Send(&actionspb.WatchForUpdatesResponse{
			Message: &actionspb.WatchForUpdatesResponse_ActionUpdate{ActionUpdate: update},
		}); err != nil {
			return err
		}
	}
	if err := stream.Send(&actionspb.WatchForUpdatesResponse{
		Message: &actionspb.WatchForUpdatesResponse_ControlMessage{
			ControlMessage: &workflowpb.ControlMessage{Sentinel: true},
		},
	}); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

var traceRuns atomic.Int32

func tracedDouble(ctx Context, x int64) (int64, error) {
	traceRuns.Add(1)
	return x * 2, nil
}

func TestTraceRecordThenReplay(t *testing.T) {
	fake := &fakeActions{}
	_, handler := actionsconnect.NewActionsServiceHandler(fake)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("_U_EP_OVERRIDE", server.URL)
	t.Setenv("_UNION_EAGER_API_KEY", "")
	t.Setenv("EAGER_API_KEY", "")

	task := RegisterTask("trace_parent", func(ctx Context, x int64) (int64, error) {
		v, err := Trace[int64](ctx, tracedDouble, x).Get()
		return v, err
	}, nil, WithInputNames("x"))

	traceRuns.Store(0)
	cfg := testConfig(t, "")
	cfg.InputsURI = writeInputs(t, cfg.RunBaseDir, []string{"x"}, int64(21))

	// Attempt 1: the traced step runs and is recorded.
	require.NoError(t, Execute(context.Background(), task, NewStorage(), cfg, false))
	assert.Equal(t, int32(1), traceRuns.Load())
	require.Len(t, fake.enqueued, 1)
	trace := fake.enqueued[0].GetTrace()
	assert.Equal(t, "tracedDouble", trace.GetName())
	assert.Equal(t, commonpb.ActionPhase_ACTION_PHASE_SUCCEEDED, trace.GetPhase())
	assert.NotEmpty(t, trace.GetOutputs().GetOutputUri())
	assert.Equal(t, "a0", fake.enqueued[0].GetParentActionName(), "parent is the running action, not the task name")

	// Attempt 2 (retry): the recording is replayed; the step does not re-run.
	require.NoError(t, Execute(context.Background(), task, NewStorage(), cfg, true))
	assert.Equal(t, int32(1), traceRuns.Load(), "traced step must be replayed, not re-run")
}

func TestTraceRecordTolerAlreadyExists(t *testing.T) {
	fake := &fakeActions{dupCode: connect.CodeAlreadyExists}
	_, handler := actionsconnect.NewActionsServiceHandler(fake)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("_U_EP_OVERRIDE", server.URL)
	t.Setenv("_UNION_EAGER_API_KEY", "")
	t.Setenv("EAGER_API_KEY", "")

	calls := atomic.Int32{}
	step := func(ctx Context, x int64) (int64, error) {
		calls.Add(1)
		return x, nil
	}
	// Two identical calls in one attempt: the sequencer gives them distinct
	// action names; both run (no recording exists mid-attempt in this fake
	// since the informer synced before either was enqueued).
	task := RegisterTask("trace_dup", func(ctx Context, x int64) (int64, error) {
		a, err := Trace[int64](ctx, step, x).Get()
		if err != nil {
			return 0, err
		}
		b, err := Trace[int64](ctx, step, x).Get()
		return a + b, err
	}, nil, WithInputNames("x"))

	cfg := testConfig(t, "")
	cfg.InputsURI = writeInputs(t, cfg.RunBaseDir, []string{"x"}, int64(5))
	require.NoError(t, Execute(context.Background(), task, NewStorage(), cfg, false))
	assert.Equal(t, int32(2), calls.Load())
	assert.Len(t, fake.enqueued, 2, "identical calls get distinct deterministic names")
	assert.NotEqual(t, fake.enqueued[0].GetActionId().GetName(), fake.enqueued[1].GetActionId().GetName())
}

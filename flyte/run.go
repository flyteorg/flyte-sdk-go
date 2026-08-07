package flyte

import (
	"context"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/dataproxy"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"

	client "github.com/unionai/flyte-sdk-go/flyte/client"
)

// Inputs holds the named inputs for a run. Values are converted to Flyte
// literals using the task's typed interface: numbers, strings, bools, slices,
// maps, structs (via JSON), time.Time (DATETIME) and time.Duration (DURATION).
type Inputs = map[string]any

// RunUpdate is a single status update emitted by RunHandle.Watch.
type RunUpdate struct {
	// Phase is the current execution phase, e.g. "ACTION_PHASE_RUNNING".
	Phase string
	// Timestamp is when this update occurred.
	Timestamp time.Time
	// Error carries the failure message when the run failed.
	Error string
}

// RunHandle is a handle to a launched run, equivalent to the Python SDK's
// flyte.remote.Run. It is returned by Run and is safe for concurrent use.
type RunHandle struct {
	pb        *workflow.Run
	details   *TaskDetails
	clientset *client.RunClientset
	config    Config

	mu    sync.RWMutex
	phase common.ActionPhase
}

// Run launches a task on the cluster and returns a handle to the new run.
// It is the Go equivalent of flyte.with_runcontext(...).run(task, **inputs):
//
//	task, _ := flyte.GetTask(ctx, flyte.TaskRef{Name: "my_env.my_task"})
//	run, err := flyte.Run(ctx, task, flyte.Inputs{"x": 5},
//	    flyte.WithEnvVar("LOG_LEVEL", "DEBUG"))
//
// A TaskRef may be passed directly; it is fetched first (resolving the latest
// version when none is pinned). Inputs are validated against the task's
// interface, defaults are applied for omitted parameters, and the input
// payload is offloaded through the data proxy before the run is created,
// matching the Python SDK's launch path.
func Run(ctx context.Context, task Task, inputs Inputs, opts ...RunOption) (*RunHandle, error) {
	clientset, cfg, err := getClientset()
	if err != nil {
		return nil, err
	}

	details, ok := task.(*TaskDetails)
	if !ok {
		details, err = GetTask(ctx, task.taskRef())
		if err != nil {
			return nil, err
		}
	}

	o := newRunOptions(opts)
	project := firstNonEmpty(o.project, cfg.Project)
	domain := firstNonEmpty(o.domain, cfg.Domain)
	if project == "" || domain == "" {
		return nil, fmt.Errorf("project and domain are required (set them in flyte.Init or via WithProject/WithDomain)")
	}

	pbInputs, err := convertInputs(ctx, details, inputs)
	if err != nil {
		return nil, err
	}

	req := &workflow.CreateRunRequest{
		Task:    &workflow.CreateRunRequest_TaskId{TaskId: details.pb.GetTaskId()},
		RunSpec: buildRunSpec(o, cfg.Org, project, domain),
	}
	upload := &dataproxy.UploadInputsRequest{
		Inputs:  pbInputs,
		Task:    &dataproxy.UploadInputsRequest_TaskId{TaskId: details.pb.GetTaskId()},
		BaseDir: o.runBaseDir,
	}
	// A named run targets a specific RunIdentifier; otherwise the server
	// generates the run name for the project/domain.
	if o.name != "" {
		runID := &common.RunIdentifier{Org: cfg.Org, Project: project, Domain: domain, Name: o.name}
		req.Id = &workflow.CreateRunRequest_RunId{RunId: runID}
		upload.Id = &dataproxy.UploadInputsRequest_RunId{RunId: runID}
	} else {
		projectID := &common.ProjectIdentifier{Organization: cfg.Org, Domain: domain, Name: project}
		req.Id = &workflow.CreateRunRequest_ProjectId{ProjectId: projectID}
		upload.Id = &dataproxy.UploadInputsRequest_ProjectId{ProjectId: projectID}
	}

	// Offload inputs via the data proxy (the current Python SDK launch path).
	// Older control planes without the data proxy get inline inputs instead.
	uploadResp, err := clientset.DataProxyServiceClient().UploadInputs(ctx, connect.NewRequest(upload))
	switch {
	case err == nil:
		req.InputWrapper = &workflow.CreateRunRequest_OffloadedInputData{OffloadedInputData: uploadResp.Msg.GetOffloadedInputData()}
	case connect.CodeOf(err) == connect.CodeUnimplemented:
		req.InputWrapper = &workflow.CreateRunRequest_Inputs{Inputs: pbInputs}
	default:
		return nil, fmt.Errorf("failed to upload inputs: %w", err)
	}

	resp, err := clientset.RunServiceClient().CreateRun(ctx, connect.NewRequest(req))
	if err != nil {
		if connect.CodeOf(err) == connect.CodeAlreadyExists {
			return nil, fmt.Errorf("a run named %q already exists in %s/%s: %w", o.name, project, domain, err)
		}
		return nil, fmt.Errorf("failed to create run: %w", err)
	}
	run := resp.Msg.GetRun()
	if run.GetAction().GetId().GetRun() == nil {
		return nil, fmt.Errorf("invalid CreateRun response: missing run identifier")
	}

	return &RunHandle{
		pb:        run,
		details:   details,
		clientset: clientset,
		config:    cfg,
		phase:     run.GetAction().GetStatus().GetPhase(),
	}, nil
}

// buildRunSpec assembles the RunSpec the same way the Python SDK's
// _apply_overrides does for a fresh run. Org/project/domain scope the related
// run reference; relations are always within a single project/domain.
func buildRunSpec(o *runOptions, org, project, domain string) *taskpb.RunSpec {
	spec := &taskpb.RunSpec{
		OverwriteCache:       o.overwriteCache,
		Queue:                o.queue,
		MaxActionConcurrency: o.maxActionConcurrency,
		RunBaseDir:           o.runBaseDir,
		CacheConfig: &taskpb.CacheConfig{
			OverwriteCache: o.overwriteCache,
		},
	}
	if len(o.labels) > 0 {
		spec.Labels = &taskpb.Labels{Values: o.labels}
	}
	if len(o.annotations) > 0 {
		spec.Annotations = &taskpb.Annotations{Values: o.annotations}
	}
	if len(o.envVars) > 0 {
		envs := make([]*core.KeyValuePair, 0, len(o.envVars))
		for k, v := range o.envVars {
			envs = append(envs, &core.KeyValuePair{Key: k, Value: v})
		}
		spec.Envs = &taskpb.Envs{Values: envs}
	}
	if o.interruptible != nil {
		spec.Interruptible = wrapperspb.Bool(*o.interruptible)
	}
	if o.rawDataPath != "" {
		spec.RawDataStorage = &taskpb.RawDataStorage{RawDataPrefix: o.rawDataPath}
	}
	if o.serviceAccount != "" {
		spec.SecurityContext = &core.SecurityContext{
			RunAs: &core.Identity{K8SServiceAccount: o.serviceAccount},
		}
	}
	if o.relatedRun != "" {
		spec.Relation = &common.Relation{
			RelatedTo:    &common.RunIdentifier{Org: org, Project: project, Domain: domain, Name: o.relatedRun},
			RelationType: o.relationType,
		}
	}
	if o.recover || len(o.forceRerunActions) > 0 {
		spec.Recover = &taskpb.Recover{ForceRerunActions: o.forceRerunActions}
	}
	return spec
}

// Name returns the run's unique name.
func (r *RunHandle) Name() string {
	return r.pb.GetAction().GetId().GetRun().GetName()
}

// URL returns the console URL for this run.
func (r *RunHandle) URL() string {
	id := r.pb.GetAction().GetId().GetRun()
	scheme := "https"
	if r.config.Insecure {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s/v2/domain/%s/project/%s/runs/%s",
		scheme, endpointHost(r.config.Endpoint), id.GetDomain(), id.GetProject(), id.GetName())
}

// Phase returns the last observed execution phase, e.g. "ACTION_PHASE_RUNNING".
func (r *RunHandle) Phase() string {
	return r.currentPhase().String()
}

func (r *RunHandle) currentPhase() common.ActionPhase {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.phase
}

func (r *RunHandle) setPhase(p common.ActionPhase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phase = p
}

// Wait blocks until the run reaches a terminal phase. It returns nil on
// success and an error describing the failure otherwise.
func (r *RunHandle) Wait(ctx context.Context) error {
	updates, err := r.Watch(ctx)
	if err != nil {
		return err
	}
	var last *RunUpdate
	for update := range updates {
		u := update
		last = &u
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if last == nil {
		return fmt.Errorf("watch stream for run %s ended without updates", r.Name())
	}
	if last.Error != "" {
		return fmt.Errorf("run %s failed: %s", r.Name(), last.Error)
	}
	switch r.currentPhase() {
	case common.ActionPhase_ACTION_PHASE_SUCCEEDED:
		return nil
	case common.ActionPhase_ACTION_PHASE_ABORTED:
		return fmt.Errorf("run %s was aborted", r.Name())
	case common.ActionPhase_ACTION_PHASE_TIMED_OUT:
		return fmt.Errorf("run %s timed out", r.Name())
	case common.ActionPhase_ACTION_PHASE_FAILED:
		return fmt.Errorf("run %s failed", r.Name())
	default:
		return fmt.Errorf("run %s ended in unexpected phase %s", r.Name(), r.Phase())
	}
}

// watchMaxConsecutiveFailures bounds reconnect attempts that make no progress
// before Watch gives up and surfaces the transport error.
const watchMaxConsecutiveFailures = 5

// Watch streams status updates until the run reaches a terminal phase. The
// returned channel is closed when the run completes or ctx is cancelled;
// cancel ctx to stop watching early, otherwise the stream stays open until the
// run terminates. Like the Python SDK, it watches the run's root action via
// WatchActionDetails. Transient stream drops (idle timeouts, server rollouts)
// are reconnected transparently.
func (r *RunHandle) Watch(ctx context.Context) (<-chan RunUpdate, error) {
	actionID := r.pb.GetAction().GetId()
	if actionID.GetRun() == nil {
		return nil, fmt.Errorf("invalid run: missing action identifier")
	}

	watch := func() (*connect.ServerStreamForClient[workflow.WatchActionDetailsResponse], error) {
		return r.clientset.RunServiceClient().WatchActionDetails(ctx, connect.NewRequest(&workflow.WatchActionDetailsRequest{
			ActionId: actionID,
		}))
	}

	stream, err := watch()
	if err != nil {
		return nil, fmt.Errorf("failed to watch run: %w", err)
	}

	updates := make(chan RunUpdate, 16)
	go func() {
		defer close(updates)
		failures := 0
		for {
			if !stream.Receive() {
				err := stream.Err()
				if err == nil {
					// Normal end of stream: the server closed it. If the run is
					// not terminal yet, treat it like a drop and reconnect.
					err = fmt.Errorf("stream closed before run reached a terminal phase")
				}
				if connect.CodeOf(err) == connect.CodeCanceled || ctx.Err() != nil {
					return
				}
				// The run has not reached a terminal phase, so an EOF or a
				// transport error only means the stream dropped; reconnect
				// with linear backoff until it sticks or we give up.
				for {
					failures++
					if failures > watchMaxConsecutiveFailures {
						select {
						case updates <- RunUpdate{Phase: r.Phase(), Timestamp: time.Now(), Error: fmt.Sprintf("watch stream error: %v", err)}:
						case <-ctx.Done():
						}
						return
					}
					select {
					case <-time.After(time.Duration(failures) * time.Second):
					case <-ctx.Done():
						return
					}
					next, werr := watch()
					if werr == nil {
						stream = next
						break
					}
					err = werr
				}
				continue
			}

			details := stream.Msg().GetDetails()
			if details.GetStatus() == nil {
				continue
			}
			failures = 0
			phase := details.GetStatus().GetPhase()
			r.setPhase(phase)

			update := RunUpdate{Phase: phase.String(), Timestamp: time.Now()}
			if phase == common.ActionPhase_ACTION_PHASE_FAILED {
				update.Error = details.GetErrorInfo().GetMessage()
				if update.Error == "" {
					update.Error = "run failed"
				}
			}

			select {
			case updates <- update:
			case <-ctx.Done():
				return
			}

			if isTerminalPhase(phase) {
				return
			}
		}
	}()

	return updates, nil
}

// Outputs waits for the run to complete successfully and returns its outputs
// as native Go values keyed by output name (e.g. "o0").
func (r *RunHandle) Outputs(ctx context.Context) (map[string]any, error) {
	if !isTerminalPhase(r.currentPhase()) {
		if err := r.Wait(ctx); err != nil {
			return nil, err
		}
	}
	if r.currentPhase() != common.ActionPhase_ACTION_PHASE_SUCCEEDED {
		return nil, fmt.Errorf("run %s did not succeed (phase %s)", r.Name(), r.Phase())
	}

	var outputs *taskpb.Outputs
	resp, err := r.clientset.DataProxyServiceClient().GetActionData(ctx, connect.NewRequest(&dataproxy.GetActionDataRequest{
		ActionId: r.pb.GetAction().GetId(),
	}))
	switch {
	case err == nil:
		outputs = resp.Msg.GetOutputs()
	case connect.CodeOf(err) == connect.CodeUnimplemented:
		// Mirror Run's inline-inputs fallback for control planes without the
		// data proxy.
		legacy, lerr := r.clientset.RunServiceClient().GetActionData(ctx, connect.NewRequest(&workflow.GetActionDataRequest{
			ActionId: r.pb.GetAction().GetId(),
		}))
		if lerr != nil {
			return nil, fmt.Errorf("failed to get run outputs: %w", lerr)
		}
		outputs = legacy.Msg.GetOutputs()
	default:
		return nil, fmt.Errorf("failed to get run outputs: %w", err)
	}

	return convertOutputs(ctx, r.details, outputs)
}

// Abort requests termination of the run. The reason is recorded on the run.
func (r *RunHandle) Abort(ctx context.Context, reason string) error {
	req := &workflow.AbortRunRequest{RunId: r.pb.GetAction().GetId().GetRun()}
	if reason != "" {
		req.Reason = &reason
	}
	_, err := r.clientset.RunServiceClient().AbortRun(ctx, connect.NewRequest(req))
	if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
		return fmt.Errorf("failed to abort run %s: %w", r.Name(), err)
	}
	return nil
}

func isTerminalPhase(phase common.ActionPhase) bool {
	switch phase {
	case common.ActionPhase_ACTION_PHASE_SUCCEEDED,
		common.ActionPhase_ACTION_PHASE_FAILED,
		common.ActionPhase_ACTION_PHASE_ABORTED,
		common.ActionPhase_ACTION_PHASE_TIMED_OUT:
		return true
	default:
		return false
	}
}

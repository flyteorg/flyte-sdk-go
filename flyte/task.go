package flyte

import (
	"context"
	"fmt"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"

	"connectrpc.com/connect"

	client "github.com/unionai/flyte-sdk-go/flyte/client"
)

// Task is anything that can be launched with Run: a TaskRef (fetched on
// demand) or a *TaskDetails returned by GetTask.
type Task interface {
	taskRef() TaskRef
}

// TaskRef references a task deployed on the cluster, equivalent to
// flyte.remote.Task.get(...) arguments in the Python SDK.
//
// Version is optional: when empty, the latest deployed version is resolved
// automatically (Python's auto_version="latest"). Project and Domain default
// to the values passed to Init.
type TaskRef struct {
	// Name is the full task name, e.g. "my_env.my_task".
	Name string
	// Version pins a specific task version. Empty means latest.
	Version string
	// Project and Domain override the Init defaults for this task.
	Project string
	Domain  string
}

func (r TaskRef) taskRef() TaskRef { return r }

// TaskDetails is a fetched task: its identity plus the full registered spec
// (typed interface, default inputs, metadata). It mirrors the Python SDK's
// flyte.remote.TaskDetails.
type TaskDetails struct {
	pb *taskpb.TaskDetails
}

func (t *TaskDetails) taskRef() TaskRef {
	id := t.pb.GetTaskId()
	return TaskRef{Name: id.GetName(), Version: id.GetVersion(), Project: id.GetProject(), Domain: id.GetDomain()}
}

// Name returns the fully qualified task name.
func (t *TaskDetails) Name() string { return t.pb.GetTaskId().GetName() }

// Version returns the resolved task version.
func (t *TaskDetails) Version() string { return t.pb.GetTaskId().GetVersion() }

// Project returns the project the task is registered in.
func (t *TaskDetails) Project() string { return t.pb.GetTaskId().GetProject() }

// Domain returns the domain the task is registered in.
func (t *TaskDetails) Domain() string { return t.pb.GetTaskId().GetDomain() }

// Org returns the organization the task is registered in.
func (t *TaskDetails) Org() string { return t.pb.GetTaskId().GetOrg() }

// Interface returns the task's typed input/output interface.
func (t *TaskDetails) Interface() *core.TypedInterface {
	return t.pb.GetSpec().GetTaskTemplate().GetInterface()
}

// Proto exposes the underlying protobuf for advanced use.
func (t *TaskDetails) Proto() *taskpb.TaskDetails { return t.pb }

// defaultInputs returns the task's default input literals keyed by input name.
func (t *TaskDetails) defaultInputs() map[string]*core.Literal {
	defaults := map[string]*core.Literal{}
	for _, p := range t.pb.GetSpec().GetDefaultInputs() {
		if d := p.GetParameter().GetDefault(); d != nil {
			defaults[p.GetName()] = d
		}
	}
	return defaults
}

// GetTask fetches a deployed task. When ref.Version is empty the latest
// version is resolved first, matching the Python SDK's
// Task.get(name, auto_version="latest"):
//
//	task, err := flyte.GetTask(ctx, flyte.TaskRef{Name: "my_env.my_task"})
func GetTask(ctx context.Context, ref TaskRef) (*TaskDetails, error) {
	clientset, cfg, err := getClientset()
	if err != nil {
		return nil, err
	}

	if ref.Name == "" {
		return nil, fmt.Errorf("task name is required")
	}
	project := firstNonEmpty(ref.Project, cfg.Project)
	domain := firstNonEmpty(ref.Domain, cfg.Domain)
	if project == "" || domain == "" {
		return nil, fmt.Errorf("project and domain are required (set them on the TaskRef or in flyte.Init)")
	}

	version := ref.Version
	if version == "" {
		version, err = latestTaskVersion(ctx, clientset, cfg.Org, project, domain, ref.Name)
		if err != nil {
			return nil, err
		}
	}

	resp, err := clientset.TaskServiceClient().GetTaskDetails(ctx, connect.NewRequest(&taskpb.GetTaskDetailsRequest{
		TaskId: &taskpb.TaskIdentifier{
			Org:     cfg.Org,
			Project: project,
			Domain:  domain,
			Name:    ref.Name,
			Version: version,
		},
	}))
	if err != nil {
		if connect.CodeOf(err) == connect.CodeNotFound {
			return nil, fmt.Errorf("task %s version %s not found in %s/%s", ref.Name, version, project, domain)
		}
		return nil, fmt.Errorf("failed to get task details: %w", err)
	}

	return &TaskDetails{pb: resp.Msg.GetDetails()}, nil
}

// latestTaskVersion returns the most recently created version of the named
// task, using the same query as the Python SDK (ListTasks filtered by name,
// sorted by created_at descending, limit 1).
func latestTaskVersion(ctx context.Context, clientset *client.RunClientset, org, project, domain, name string) (string, error) {
	resp, err := clientset.TaskServiceClient().ListTasks(ctx, connect.NewRequest(&taskpb.ListTasksRequest{
		ScopeBy: &taskpb.ListTasksRequest_ProjectId{
			ProjectId: &common.ProjectIdentifier{
				Organization: org,
				Domain:       domain,
				Name:         project,
			},
		},
		Request: &common.ListRequest{
			Limit: 1,
			SortBy: &common.Sort{
				Key:       "created_at",
				Direction: common.Sort_DESCENDING,
			},
			Filters: []*common.Filter{{
				Function: common.Filter_EQUAL,
				Field:    "name",
				Values:   []string{name},
			}},
		},
	}))
	if err != nil {
		return "", fmt.Errorf("failed to list task versions for %s: %w", name, err)
	}
	if len(resp.Msg.GetTasks()) == 0 {
		return "", fmt.Errorf("no versions found for task %s in %s/%s", name, project, domain)
	}
	return resp.Msg.GetTasks()[0].GetTaskId().GetVersion(), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

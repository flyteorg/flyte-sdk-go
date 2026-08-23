package runtime

import (
	"context"
	"fmt"
	"reflect"
	"regexp"

	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
)

// Context is the first parameter of every task function. It is a plain
// context.Context; per-action runtime state (action identity, storage, the
// trace controller) travels in its values, so always pass it along — a task
// body that swaps in a fresh context.Background() silently loses tracing.
type Context = context.Context

// TaskEnvironment groups tasks that share an execution environment, mirroring
// the prototype Go SDK's TaskEnvironment. In v1 it is declarative: the actual
// image and resources of a deployed task are controlled by the Python
// companion (flyteplugins-go); these values document intent and feed future
// deploy-side integration.
type TaskEnvironment struct {
	Name      string
	Resources Resources
}

// Resources describes compute requests for tasks in an environment.
type Resources struct {
	CPU    CPU
	Memory Memory
	GPU    GPU
}

// CPU is a cpu request/limit pair in Kubernetes quantity syntax ("500m", "2").
type CPU struct{ Request, Limit string }

// NewCPU returns a CPU with request == limit.
func NewCPU(v string) CPU { return CPU{Request: v, Limit: v} }

// NewCPURange returns a CPU with distinct request and limit.
func NewCPURange(request, limit string) CPU { return CPU{Request: request, Limit: limit} }

// Memory is a memory request/limit pair in Kubernetes quantity syntax ("512Mi").
type Memory struct{ Request, Limit string }

// NewMemory returns a Memory with request == limit.
func NewMemory(v string) Memory { return Memory{Request: v, Limit: v} }

// NewMemoryRange returns a Memory with distinct request and limit.
func NewMemoryRange(request, limit string) Memory { return Memory{Request: request, Limit: limit} }

// GPUType names an accelerator class.
type GPUType string

const (
	NVIDIA_TESLA_V100 GPUType = "nvidia-tesla-v100"
	NVIDIA_TESLA_P100 GPUType = "nvidia-tesla-p100"
	NVIDIA_A100       GPUType = "nvidia-a100"
	NVIDIA_H100       GPUType = "nvidia-h100"
)

// GPU describes an accelerator request. The zero value means no GPU.
type GPU struct {
	Count string
	Type  GPUType
}

// NewGPU returns a GPU request for count devices of the given type.
func NewGPU(count string, gpuType GPUType) GPU { return GPU{Count: count, Type: gpuType} }

// Task is a registered task: a plain Go function plus the interface derived
// from its signature. Obtain one from RegisterTask.
type Task struct {
	name       string
	fn         reflect.Value
	inputNames []string
	inputTypes []reflect.Type
	outputType reflect.Type // nil when the fn returns only error
	env        *TaskEnvironment
	iface      *Interface
}

// Name returns the task's registered name.
func (t *Task) Name() string { return t.name }

// Interface returns the task's derived interface.
func (t *Task) Interface() *Interface { return t.iface }

// Environment returns the task's environment.
func (t *Task) Environment() *TaskEnvironment { return t.env }

// TaskOption customizes a task at registration (or via Override).
type TaskOption func(*Task)

// WithInputNames names the task's inputs, in parameter order (the Context
// parameter is not counted). Without it, inputs are named a0, a1, ... —
// functional, but named inputs make `flyte run --a 5` and the launch form read
// well. Reflection cannot recover Go parameter names, hence the option.
func WithInputNames(names ...string) TaskOption {
	return func(t *Task) { t.inputNames = names }
}

// WithEnvironment replaces the task's environment (Override convenience).
func WithEnvironment(env *TaskEnvironment) TaskOption {
	return func(t *Task) { t.env = env }
}

// Override returns a copy of the task with the options applied, leaving the
// registered task untouched — the prototype SDK's per-call tweak pattern.
func (t *Task) Override(opts ...TaskOption) *Task {
	c := *t
	for _, opt := range opts {
		opt(&c)
	}
	return &c
}

var (
	ctxType     = reflect.TypeOf((*context.Context)(nil)).Elem()
	errType     = reflect.TypeOf((*error)(nil)).Elem()
	inputNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// newTask validates fn's signature and derives the task's interface.
func newTask(name string, fn any, env *TaskEnvironment, opts ...TaskOption) (*Task, error) {
	if name == "" || !inputNameRe.MatchString(name) {
		return nil, fmt.Errorf("task name %q must be a plain identifier", name)
	}
	fv := reflect.ValueOf(fn)
	ft := fv.Type()
	if fv.Kind() != reflect.Func {
		return nil, fmt.Errorf("task %q: fn must be a function, got %T", name, fn)
	}
	if ft.NumIn() < 1 || !ft.In(0).Implements(ctxType) && ft.In(0) != ctxType {
		return nil, fmt.Errorf("task %q: first parameter must be flyteruntime.Context (context.Context)", name)
	}
	if ft.IsVariadic() {
		return nil, fmt.Errorf("task %q: variadic functions are not supported", name)
	}
	switch ft.NumOut() {
	case 1, 2:
		if ft.Out(ft.NumOut()-1) != errType {
			return nil, fmt.Errorf("task %q: last return value must be error", name)
		}
	default:
		return nil, fmt.Errorf("task %q: must return (T, error) or error", name)
	}

	t := &Task{name: name, fn: fv, env: env}
	for i := 1; i < ft.NumIn(); i++ {
		t.inputTypes = append(t.inputTypes, ft.In(i))
	}
	if ft.NumOut() == 2 {
		t.outputType = ft.Out(0)
	}
	for _, opt := range opts {
		opt(t)
	}

	if t.inputNames == nil {
		for i := range t.inputTypes {
			t.inputNames = append(t.inputNames, fmt.Sprintf("a%d", i))
		}
	}
	if len(t.inputNames) != len(t.inputTypes) {
		return nil, fmt.Errorf("task %q: WithInputNames gave %d names for %d inputs", name, len(t.inputNames), len(t.inputTypes))
	}
	seen := map[string]bool{}
	for _, n := range t.inputNames {
		if !inputNameRe.MatchString(n) {
			return nil, fmt.Errorf("task %q: input name %q must be a plain identifier", name, n)
		}
		if seen[n] {
			return nil, fmt.Errorf("task %q: duplicate input name %q", name, n)
		}
		seen[n] = true
	}
	if err := t.deriveInterface(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Task) deriveInterface() error {
	iface := &Interface{}
	for i, it := range t.inputTypes {
		lt, err := literalTypeOf(it)
		if err != nil {
			return fmt.Errorf("task %q input %q: %w", t.name, t.inputNames[i], err)
		}
		iface.Inputs = append(iface.Inputs, Variable{Name: t.inputNames[i], LiteralType: lt, Required: true})
	}
	if t.outputType != nil {
		lt, err := literalTypeOf(t.outputType)
		if err != nil {
			return fmt.Errorf("task %q output: %w", t.name, err)
		}
		iface.Outputs = append(iface.Outputs, Variable{Name: "o0", LiteralType: lt, Required: true})
	}
	t.iface = iface
	return nil
}

// run executes the task against a decoded Inputs envelope and returns the
// Outputs envelope. Panics in the task body are recovered as user errors.
func (t *Task) run(ctx context.Context, inputs *taskpb.Inputs) (outputs *taskpb.Outputs, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = UserErrorf("PanicError", "task %q panicked: %v", t.name, r)
		}
	}()

	args := make([]reflect.Value, 0, len(t.inputTypes)+1)
	args = append(args, reflect.ValueOf(ctx))
	for i, it := range t.inputTypes {
		lit, lerr := namedLiteral(inputs.GetLiterals(), t.inputNames[i])
		if lerr != nil {
			return nil, SystemErrorf("task %q: inputs envelope: %w", t.name, lerr)
		}
		v, cerr := fromLiteral(lit, it)
		if cerr != nil {
			return nil, SystemErrorf("task %q input %q: %w", t.name, t.inputNames[i], cerr)
		}
		args = append(args, v)
	}

	results := t.fn.Call(args)
	if ferr, _ := results[len(results)-1].Interface().(error); ferr != nil {
		return nil, ferr
	}
	if t.outputType == nil {
		return &taskpb.Outputs{}, nil
	}
	return buildOutputs([]string{"o0"}, results[:1])
}

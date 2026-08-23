package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	goruntime "runtime"
	"strings"
	"time"

	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"google.golang.org/protobuf/proto"

	"github.com/unionai/flyte-sdk-go/flyte/runtime/internal/controller"
	istorage "github.com/unionai/flyte-sdk-go/flyte/runtime/internal/storage"
)

// Trace runs fn(ctx, args...) as a recorded step: the first successful run
// uploads its result and records a trace action; a retry of the task replays
// the recording instead of re-running the step. The Go answer to Rust's
// #[flyte::trace] and Python's @flyte.trace.
//
// fn must be func(Context, ...inputs) (T, error) with supported types, and
// should be a named top-level function — the trace's identity is derived from
// the function's name (closures get compiler-assigned names like "func1" that
// shift when code moves).
//
// Unlike the Rust macro, Go cannot hash the function body: editing a traced
// function does NOT invalidate its recordings. Use TraceVersioned and bump the
// version when a traced function's behavior changes.
//
// Outside a worker (local runs, tests) or inside another traced call, fn just
// runs — no recording, matching the Rust SDK's local mode.
func Trace[T any](ctx Context, fn any, args ...any) *Future[T] {
	return TraceVersioned[T](ctx, "v1", fn, args...)
}

// TraceVersioned is Trace with an explicit identity version. Recordings are
// looked up by "{function-name}-{version}": bumping the version makes prior
// recordings invisible, forcing a re-run.
func TraceVersioned[T any](ctx Context, version string, fn any, args ...any) *Future[T] {
	fut := newFuture[T]()
	go func() {
		val, err := runTraced[T](ctx, version, fn, args)
		fut.complete(val, err)
	}()
	return fut
}

func runTraced[T any](ctx Context, version string, fn any, args []any) (T, error) {
	var zero T
	tr, err := newTracedCall(fn, args, reflect.TypeOf((*T)(nil)).Elem())
	if err != nil {
		return zero, err
	}

	state := stateFrom(ctx)
	if state == nil || inTrace(ctx) {
		// Local mode, or nested inside another traced call: just run the body.
		out, err := tr.invoke(ctx)
		if err != nil {
			return zero, err
		}
		return castOutput[T](out)
	}

	identity := tr.shortName + "-" + version
	inputs, err := buildInputs(tr.inputNames, tr.argValues)
	if err != nil {
		return zero, fmt.Errorf("trace %s: %w", tr.shortName, err)
	}
	serialized, err := proto.Marshal(inputs)
	if err != nil {
		return zero, SystemErrorf("trace %s: failed to serialize inputs: %w", tr.shortName, err)
	}
	inHash, err := inputsHash(inputs)
	if err != nil {
		return zero, err
	}
	seq := state.seq.next(identity + ":" + inHash)
	actionName := subActionName(state.actionName, inHash, identity, seq)

	// Upload inputs before the replay lookup (matches Python ordering).
	subPath := istorage.Join(state.runBaseDir, actionName)
	inputsURI := istorage.Join(subPath, "inputs.pb")
	if err := state.storage.Put(ctx, inputsURI, serialized); err != nil {
		return zero, SystemErrorf("trace %s: inputs upload failed: %w", tr.shortName, err)
	}

	ctrl, err := state.controller(ctx)
	if err != nil {
		return zero, SystemErrorf("trace %s: controller init failed: %w", tr.shortName, err)
	}

	found, err := ctrl.LookupAction(ctx, actionName, state.actionName)
	if err != nil {
		return zero, SystemErrorf("trace %s: lookup failed: %w", tr.shortName, err)
	}
	// Cold-cache mitigation, retry attempts only: the informer's watch stream
	// may still be syncing actions recorded by a previous attempt. On first
	// attempts a miss is the expected case — don't tax it with sleeps.
	if found == nil && state.isRetry {
		for i := 0; i < 3 && found == nil; i++ {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			found, err = ctrl.LookupAction(ctx, actionName, state.actionName)
			if err != nil {
				return zero, SystemErrorf("trace %s: lookup failed: %w", tr.shortName, err)
			}
		}
	}

	if found != nil {
		if found.Failed {
			slog.Info("trace previously failed; re-running", "action", actionName)
		} else if found.OutputsURI != "" && tr.outputType != nil {
			data, err := state.storage.Get(ctx, found.OutputsURI)
			if err != nil {
				return zero, SystemErrorf("trace %s: recorded outputs fetch failed: %w", tr.shortName, err)
			}
			outputs := &taskpb.Outputs{}
			if err := proto.Unmarshal(data, outputs); err != nil {
				return zero, SystemErrorf("trace %s: recorded outputs decode failed: %w", tr.shortName, err)
			}
			slog.Info("replaying recorded trace", "action", actionName)
			lit, err := namedLiteral(outputs.GetLiterals(), "o0")
			if err != nil {
				return zero, SystemErrorf("trace %s: recorded outputs: %w", tr.shortName, err)
			}
			v, err := fromLiteral(lit, tr.outputType)
			if err != nil {
				return zero, SystemErrorf("trace %s: recorded output: %w", tr.shortName, err)
			}
			return castOutput[T](v)
		}
	}

	// Run the body; nested traced calls run inline.
	start := float64(time.Now().UnixNano()) / 1e9
	out, err := tr.invoke(context.WithValue(ctx, inTraceCtxKey{}, true))
	if err != nil {
		return zero, err
	}
	end := float64(time.Now().UnixNano()) / 1e9

	// Record: upload outputs (if any) and enqueue the trace action. Record
	// failures are logged, not surfaced — the user's step already succeeded;
	// the cost of a lost record is one re-run on retry.
	outputsURI := ""
	record := true
	if tr.outputType != nil {
		outputs, berr := buildOutputs([]string{"o0"}, []reflect.Value{out})
		if berr != nil {
			return zero, fmt.Errorf("trace %s: %w", tr.shortName, berr)
		}
		data, merr := proto.Marshal(outputs)
		if merr != nil {
			return zero, SystemErrorf("trace %s: outputs serialize failed: %w", tr.shortName, merr)
		}
		outputsURI = istorage.Join(subPath, "outputs.pb")
		if perr := state.storage.Put(ctx, outputsURI, data); perr != nil {
			slog.Error("trace outputs upload failed", "action", actionName, "error", perr)
			record = false
		}
	}
	if record {
		rec := controller.TraceRecord{
			ParentActionName: state.actionName,
			ActionName:       actionName,
			FriendlyName:     tr.shortName,
			InputsURI:        inputsURI,
			OutputsURI:       outputsURI,
			Start:            start,
			End:              end,
			RunOutputBase:    state.runBaseDir,
			Interface:        tr.iface.Typed(),
		}
		if rerr := ctrl.RecordTrace(ctx, rec); rerr != nil {
			slog.Error("trace record failed", "action", actionName, "error", rerr)
		}
	}
	return castOutput[T](out)
}

// tracedCall is a validated traced function plus its bound arguments.
type tracedCall struct {
	fn         reflect.Value
	shortName  string
	inputNames []string
	argValues  []reflect.Value
	outputType reflect.Type // nil for func(...) error
	iface      *Interface
}

func newTracedCall(fn any, args []any, want reflect.Type) (*tracedCall, error) {
	fv := reflect.ValueOf(fn)
	if fv.Kind() != reflect.Func {
		return nil, fmt.Errorf("Trace: fn must be a function, got %T", fn)
	}
	ft := fv.Type()
	if ft.NumIn() < 1 || (!ft.In(0).Implements(ctxType) && ft.In(0) != ctxType) {
		return nil, fmt.Errorf("Trace %s: first parameter must be Context", funcName(fv))
	}
	if ft.IsVariadic() {
		return nil, fmt.Errorf("Trace %s: variadic functions are not supported", funcName(fv))
	}
	switch ft.NumOut() {
	case 1, 2:
		if ft.Out(ft.NumOut()-1) != errType {
			return nil, fmt.Errorf("Trace %s: last return value must be error", funcName(fv))
		}
	default:
		return nil, fmt.Errorf("Trace %s: must return (T, error) or error", funcName(fv))
	}
	if ft.NumIn()-1 != len(args) {
		return nil, fmt.Errorf("Trace %s: takes %d inputs, got %d args", funcName(fv), ft.NumIn()-1, len(args))
	}

	tr := &tracedCall{fn: fv, shortName: funcName(fv)}
	iface := &Interface{}
	for i := 0; i < len(args); i++ {
		pt := ft.In(i + 1)
		av := reflect.ValueOf(args[i])
		if !av.IsValid() || !av.Type().AssignableTo(pt) {
			got := "nil"
			if av.IsValid() {
				got = av.Type().String()
			}
			return nil, fmt.Errorf("Trace %s: arg %d must be %s, got %s", tr.shortName, i, pt, got)
		}
		name := fmt.Sprintf("a%d", i)
		lt, err := literalTypeOf(pt)
		if err != nil {
			return nil, fmt.Errorf("Trace %s input %s: %w", tr.shortName, name, err)
		}
		tr.inputNames = append(tr.inputNames, name)
		tr.argValues = append(tr.argValues, av)
		iface.Inputs = append(iface.Inputs, Variable{Name: name, LiteralType: lt, Required: true})
	}
	if ft.NumOut() == 2 {
		tr.outputType = ft.Out(0)
		if !tr.outputType.AssignableTo(want) {
			return nil, fmt.Errorf("Trace %s: fn returns %s, future wants %s", tr.shortName, tr.outputType, want)
		}
		lt, err := literalTypeOf(tr.outputType)
		if err != nil {
			return nil, fmt.Errorf("Trace %s output: %w", tr.shortName, err)
		}
		iface.Outputs = append(iface.Outputs, Variable{Name: "o0", LiteralType: lt, Required: true})
	}
	tr.iface = iface
	return tr, nil
}

// invoke calls the traced fn, recovering panics as user errors. Returns the
// non-error result (invalid reflect.Value for error-only fns).
func (t *tracedCall) invoke(ctx context.Context) (out reflect.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = UserErrorf("PanicError", "traced call %s panicked: %v", t.shortName, r)
		}
	}()
	callArgs := append([]reflect.Value{reflect.ValueOf(ctx)}, t.argValues...)
	results := t.fn.Call(callArgs)
	if ferr, _ := results[len(results)-1].Interface().(error); ferr != nil {
		return reflect.Value{}, ferr
	}
	if t.outputType == nil {
		return reflect.Value{}, nil
	}
	return results[0], nil
}

func castOutput[T any](v reflect.Value) (T, error) {
	var zero T
	if !v.IsValid() {
		return zero, nil // error-only fn: T should be struct{} or ignored
	}
	out, ok := v.Interface().(T)
	if !ok {
		return zero, fmt.Errorf("traced result %s is not assignable to %T", v.Type(), zero)
	}
	return out, nil
}

// funcName returns the bare name of a function value ("expensiveStep"), used
// as the trace's friendly name and identity stem.
func funcName(fv reflect.Value) string {
	full := goruntime.FuncForPC(fv.Pointer()).Name()
	if idx := strings.LastIndex(full, "/"); idx >= 0 {
		full = full[idx+1:]
	}
	if idx := strings.LastIndex(full, "."); idx >= 0 {
		full = full[idx+1:]
	}
	return strings.TrimSuffix(full, "-fm") // method values
}

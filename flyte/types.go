package flyte

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flyteorg/flyte/v2/dataproxy/converter"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"google.golang.org/protobuf/types/known/structpb"
)

// convertInputs turns native Go inputs into the task's wire-format Inputs.
// Conversion is driven by the task's typed interface using the same
// JSON-to-literal converter the Flyte backend uses, so any type the platform
// supports works here. Inputs omitted by the caller fall back to the task's
// registered defaults; missing required inputs and unknown input names are
// errors. Literals are emitted in interface order, matching the Python SDK.
func convertInputs(ctx context.Context, details *TaskDetails, inputs Inputs) (*taskpb.Inputs, error) {
	variables := details.Interface().GetInputs().GetVariables()
	defaults := details.defaultInputs()

	known := make(map[string]bool, len(variables))
	payload := map[string]any{}
	// provided narrows the variable map passed to the converter: caller-supplied
	// inputs (so registered defaults don't need a lossy literal->JSON->literal
	// round trip) plus absent optional inputs without a default, for which the
	// converter emits an explicit NONE literal.
	provided := &core.VariableMap{}
	for _, entry := range variables {
		name := entry.GetKey()
		known[name] = true
		value, ok := inputs[name]
		if !ok {
			if _, hasDefault := defaults[name]; !hasDefault && isOptional(entry.GetValue().GetType()) {
				provided.Variables = append(provided.Variables, entry)
			}
			continue
		}
		jsonValue, err := toJSONValue(value)
		if err != nil {
			return nil, fmt.Errorf("input %q: %w", name, err)
		}
		payload[name] = jsonValue
		provided.Variables = append(provided.Variables, entry)
	}

	for name := range inputs {
		if !known[name] {
			return nil, fmt.Errorf("unknown input %q: task %s accepts %s", name, details.Name(), inputNames(variables))
		}
	}

	var literals []*taskpb.NamedLiteral
	if len(provided.GetVariables()) > 0 {
		payloadStruct, err := structpb.NewStruct(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to encode inputs: %w", err)
		}
		literals, err = converter.JSONValuesToLiterals(ctx, provided, payloadStruct)
		if err != nil {
			return nil, fmt.Errorf("failed to convert inputs: %w", err)
		}
	}

	byName := make(map[string]*taskpb.NamedLiteral, len(literals))
	for _, l := range literals {
		byName[l.GetName()] = l
	}

	// Emit literals in interface order; apply defaults and reject missing
	// required inputs.
	ordered := make([]*taskpb.NamedLiteral, 0, len(variables))
	for _, entry := range variables {
		name := entry.GetKey()
		if l, ok := byName[name]; ok {
			ordered = append(ordered, l)
			continue
		}
		if _, providedByUser := inputs[name]; providedByUser {
			// The converter dropped a null-like value; keep the server default.
			continue
		}
		if def, ok := defaults[name]; ok {
			ordered = append(ordered, &taskpb.NamedLiteral{Name: name, Value: def})
			continue
		}
		if isOptional(entry.GetValue().GetType()) {
			continue
		}
		return nil, fmt.Errorf("missing required input %q for task %s", name, details.Name())
	}

	return &taskpb.Inputs{Literals: ordered}, nil
}

// convertOutputs turns wire-format outputs back into native Go values
// (string, float64, bool, []any, map[string]any, ...) keyed by output name.
func convertOutputs(ctx context.Context, details *TaskDetails, outputs *taskpb.Outputs) (map[string]any, error) {
	if outputs == nil || len(outputs.GetLiterals()) == 0 {
		return map[string]any{}, nil
	}
	form, err := converter.LiteralsToLaunchFormJson(ctx, outputs.GetLiterals(), details.Interface().GetOutputs())
	if err != nil {
		return nil, fmt.Errorf("failed to convert outputs: %w", err)
	}
	return launchFormValues(form), nil
}

// launchFormValues extracts the default values from the JSON-schema-shaped
// struct produced by LiteralsToLaunchFormJson: {properties: {name: {default: v}}}.
func launchFormValues(form *structpb.Struct) map[string]any {
	result := map[string]any{}
	properties, _ := form.AsMap()["properties"].(map[string]any)
	for name, schema := range properties {
		if s, ok := schema.(map[string]any); ok {
			result[name] = s["default"]
		}
	}
	return result
}

// toJSONValue converts a native Go value to its JSON representation as
// expected by the platform's JSON-to-literal converter: time.Time becomes an
// RFC3339 string, time.Duration its string form ("1h30m"), and everything
// else goes through encoding/json (so structs, maps, slices and json.Marshaler
// implementations all work).
func toJSONValue(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case time.Time:
		return t.UTC().Format(time.RFC3339), nil
	case time.Duration:
		return t.String(), nil
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return t, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("value of type %T is not JSON-serializable: %w", v, err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// isOptional reports whether a literal type is a union that admits None.
func isOptional(t *core.LiteralType) bool {
	for _, variant := range t.GetUnionType().GetVariants() {
		if variant.GetSimple() == core.SimpleType_NONE {
			return true
		}
	}
	return false
}

func inputNames(variables []*core.VariableEntry) []string {
	names := make([]string, 0, len(variables))
	for _, v := range variables {
		names = append(names, v.GetKey())
	}
	return names
}

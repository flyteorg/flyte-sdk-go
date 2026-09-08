package runtime

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"

	corepb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"github.com/vmihailenco/msgpack/v5"
)

// Native ⇄ Flyte literal conversion. Mirrors the Rust SDK's types.rs (and the
// Python type engine) for the v1 surface: primitives map to
// Literal.scalar.primitive, structs map to msgpack bytes in
// Literal.scalar.binary{tag:"msgpack"} — string-keyed maps in field declaration
// order, wire-compatible with mashumaro dataclasses and rmp_serde structs.
// Cross-language field names come from `msgpack:"..."` tags; an untagged field
// goes on the wire under its exact Go name.

const msgpackTag = "msgpack"

func simpleLiteralType(t corepb.SimpleType) *corepb.LiteralType {
	return &corepb.LiteralType{Type: &corepb.LiteralType_Simple{Simple: t}}
}

// literalTypeOf maps a Go type to its Flyte literal type. Supported: int/int32/
// int64 → INTEGER; float32/float64 → FLOAT; string; bool; any named struct →
// STRUCT (msgpack binary).
func literalTypeOf(t reflect.Type) (*corepb.LiteralType, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return simpleLiteralType(corepb.SimpleType_INTEGER), nil
	case reflect.Float32, reflect.Float64:
		return simpleLiteralType(corepb.SimpleType_FLOAT), nil
	case reflect.String:
		return simpleLiteralType(corepb.SimpleType_STRING), nil
	case reflect.Bool:
		return simpleLiteralType(corepb.SimpleType_BOOLEAN), nil
	case reflect.Struct:
		return simpleLiteralType(corepb.SimpleType_STRUCT), nil
	default:
		return nil, fmt.Errorf("unsupported task value type %s (supported: int, int32, int64, float32, float64, string, bool, struct)", t)
	}
}

func primitiveLiteral(value *corepb.Primitive) *corepb.Literal {
	return &corepb.Literal{Value: &corepb.Literal_Scalar{Scalar: &corepb.Scalar{
		Value: &corepb.Scalar_Primitive{Primitive: value},
	}}}
}

// toLiteral converts a native Go value to a Flyte literal.
func toLiteral(v reflect.Value) (*corepb.Literal, error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return primitiveLiteral(&corepb.Primitive{Value: &corepb.Primitive_Integer{Integer: v.Int()}}), nil
	case reflect.Float32, reflect.Float64:
		return primitiveLiteral(&corepb.Primitive{Value: &corepb.Primitive_FloatValue{FloatValue: v.Float()}}), nil
	case reflect.String:
		return primitiveLiteral(&corepb.Primitive{Value: &corepb.Primitive_StringValue{StringValue: v.String()}}), nil
	case reflect.Bool:
		return primitiveLiteral(&corepb.Primitive{Value: &corepb.Primitive_Boolean{Boolean: v.Bool()}}), nil
	case reflect.Struct:
		// msgpack with string keys in declaration order: the encoding mashumaro
		// produces for dataclasses, so structs round-trip across the Python SDK.
		// CompactInts matches msgpack-python/rmp_serde (smallest int encoding);
		// floats stay float64, also matching. Pinned by the mashumaro golden test.
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		enc.UseCompactInts(true)
		if err := enc.Encode(v.Interface()); err != nil {
			return nil, fmt.Errorf("msgpack encode of %s failed: %w", v.Type(), err)
		}
		b := buf.Bytes()
		return &corepb.Literal{Value: &corepb.Literal_Scalar{Scalar: &corepb.Scalar{
			Value: &corepb.Scalar_Binary{Binary: &corepb.Binary{Value: b, Tag: msgpackTag}},
		}}}, nil
	default:
		return nil, fmt.Errorf("unsupported task value type %s", v.Type())
	}
}

func primitiveOf(lit *corepb.Literal) *corepb.Primitive {
	if s, ok := lit.GetValue().(*corepb.Literal_Scalar); ok {
		if p, ok := s.Scalar.GetValue().(*corepb.Scalar_Primitive); ok {
			return p.Primitive
		}
	}
	return nil
}

// fromLiteral converts a Flyte literal back to a native Go value of type t.
// Floats accept integer literals (Python's lenient int→float coercion).
//
// Every branch allocates a value of exactly t and sets into it: registration
// accepts named types (`type Label string`) because it checks Kind, so the
// value handed to fn.Call must be of the declared type, not the underlying one.
func fromLiteral(lit *corepb.Literal, t reflect.Type) (reflect.Value, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		p := primitiveOf(lit)
		iv, ok := p.GetValue().(*corepb.Primitive_Integer)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected integer literal for %s", t)
		}
		out := reflect.New(t).Elem()
		if out.OverflowInt(iv.Integer) {
			return reflect.Value{}, fmt.Errorf("integer %d out of range for %s", iv.Integer, t)
		}
		out.SetInt(iv.Integer)
		return out, nil
	case reflect.Float32, reflect.Float64:
		out := reflect.New(t).Elem()
		switch pv := primitiveOf(lit).GetValue().(type) {
		case *corepb.Primitive_FloatValue:
			out.SetFloat(pv.FloatValue)
		case *corepb.Primitive_Integer:
			out.SetFloat(float64(pv.Integer))
		default:
			return reflect.Value{}, fmt.Errorf("expected float literal for %s", t)
		}
		return out, nil
	case reflect.String:
		sv, ok := primitiveOf(lit).GetValue().(*corepb.Primitive_StringValue)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected string literal for %s", t)
		}
		out := reflect.New(t).Elem()
		out.SetString(sv.StringValue)
		return out, nil
	case reflect.Bool:
		bv, ok := primitiveOf(lit).GetValue().(*corepb.Primitive_Boolean)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected boolean literal for %s", t)
		}
		out := reflect.New(t).Elem()
		out.SetBool(bv.Boolean)
		return out, nil
	case reflect.Struct:
		s, ok := lit.GetValue().(*corepb.Literal_Scalar)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected scalar (msgpack) literal for %s", t)
		}
		b, ok := s.Scalar.GetValue().(*corepb.Scalar_Binary)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected binary (msgpack) literal for %s", t)
		}
		if tag := b.Binary.GetTag(); tag != "" && tag != msgpackTag {
			return reflect.Value{}, fmt.Errorf("unsupported binary literal tag %q (expected %q)", tag, msgpackTag)
		}
		out := reflect.New(t)
		if err := msgpack.Unmarshal(b.Binary.GetValue(), out.Interface()); err != nil {
			return reflect.Value{}, fmt.Errorf("msgpack decode into %s failed: %w", t, err)
		}
		return out.Elem(), nil
	default:
		return reflect.Value{}, fmt.Errorf("unsupported task value type %s", t)
	}
}

// buildInputs builds an Inputs envelope preserving declaration order.
func buildInputs(names []string, values []reflect.Value) (*taskpb.Inputs, error) {
	literals := make([]*taskpb.NamedLiteral, len(names))
	for i, name := range names {
		lit, err := toLiteral(values[i])
		if err != nil {
			return nil, fmt.Errorf("input %q: %w", name, err)
		}
		literals[i] = &taskpb.NamedLiteral{Name: name, Value: lit}
	}
	return &taskpb.Inputs{Literals: literals}, nil
}

// buildOutputs builds an Outputs envelope (names are "o0", "o1", ...).
func buildOutputs(names []string, values []reflect.Value) (*taskpb.Outputs, error) {
	literals := make([]*taskpb.NamedLiteral, len(names))
	for i, name := range names {
		lit, err := toLiteral(values[i])
		if err != nil {
			return nil, fmt.Errorf("output %q: %w", name, err)
		}
		literals[i] = &taskpb.NamedLiteral{Name: name, Value: lit}
	}
	return &taskpb.Outputs{Literals: literals}, nil
}

func namedLiteral(literals []*taskpb.NamedLiteral, name string) (*corepb.Literal, error) {
	for _, n := range literals {
		if n.GetName() == name && n.GetValue() != nil {
			return n.GetValue(), nil
		}
	}
	return nil, fmt.Errorf("missing literal %q", name)
}

// buildTypedInterface builds the wire TypedInterface with variables sorted by
// key, like Python's types_serde.transform_native_to_typed_interface (hash
// stability).
func buildTypedInterface(inputs, outputs []Variable) *corepb.TypedInterface {
	toMap := func(vars []Variable) *corepb.VariableMap {
		sorted := make([]Variable, len(vars))
		copy(sorted, vars)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
		entries := make([]*corepb.VariableEntry, len(sorted))
		for i, v := range sorted {
			entries[i] = &corepb.VariableEntry{
				Key:   v.Name,
				Value: &corepb.Variable{Type: v.LiteralType},
			}
		}
		return &corepb.VariableMap{Variables: entries}
	}
	return &corepb.TypedInterface{Inputs: toMap(inputs), Outputs: toMap(outputs)}
}

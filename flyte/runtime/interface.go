package runtime

import (
	"fmt"
	"strings"

	corepb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
)

// A task's interface, derived by RegisterTask from the function signature.
//
// Two shapes, one source of truth:
//
//   - Typed() is the wire form (core.TypedInterface, variables sorted by key)
//     used when recording actions.
//   - DescriptorJSON() is the descriptor, printed by `<binary>
//     describe-interface` and consumed by the Python companion to build a
//     NativeInterface. It preserves declaration order, so the launcher can bind
//     arguments positionally.
//
// Nothing outside the Go source ever restates the signature: rename an input
// and the descriptor changes with it.

// Variable is one input or output of a task.
type Variable struct {
	Name        string
	LiteralType *corepb.LiteralType
	// Always true in v1 — Go fn params have no defaults. Carried explicitly so
	// the launcher reads required-ness from data rather than assuming it.
	Required bool
}

// Interface is a task's inputs and outputs, in declaration order.
type Interface struct {
	Inputs  []Variable
	Outputs []Variable
}

// Typed returns the wire form, key-sorted for hash stability with the Python
// SDK.
func (i *Interface) Typed() *corepb.TypedInterface {
	return buildTypedInterface(i.Inputs, i.Outputs)
}

// typeTag returns the descriptor's type tag for a literal type, or "" if the
// type has no launcher-side equivalent (collections, blobs, ...).
func typeTag(lt *corepb.LiteralType) string {
	s, ok := lt.GetType().(*corepb.LiteralType_Simple)
	if !ok {
		return ""
	}
	switch s.Simple {
	case corepb.SimpleType_INTEGER:
		return "integer"
	case corepb.SimpleType_FLOAT:
		return "float"
	case corepb.SimpleType_STRING:
		return "string"
	case corepb.SimpleType_BOOLEAN:
		return "boolean"
	case corepb.SimpleType_STRUCT:
		return "struct"
	default:
		return ""
	}
}

// DescriptorJSON renders the launcher-facing descriptor: one line of JSON.
//
// Written by hand rather than via encoding/json: the only dynamic strings are
// variable names, which RegisterTask restricts to identifier characters, so no
// escaping is required and the output is byte-stable (a test pins it). Same
// format and version as the Rust SDK's descriptor.
func (i *Interface) DescriptorJSON(taskName string) string {
	var s strings.Builder
	fmt.Fprintf(&s, `{"flyte_interface_version":1,"task":"%s","inputs":[`, taskName)
	for idx, v := range i.Inputs {
		if idx > 0 {
			s.WriteByte(',')
		}
		writeVariable(&s, v, true)
	}
	s.WriteString(`],"outputs":[`)
	for idx, v := range i.Outputs {
		if idx > 0 {
			s.WriteByte(',')
		}
		writeVariable(&s, v, false)
	}
	s.WriteString("]}")
	return s.String()
}

// An unmappable type is reported rather than guessed at: the launcher raises on
// "unsupported", which beats silently binding the wrong Python type.
func writeVariable(s *strings.Builder, v Variable, withRequired bool) {
	fmt.Fprintf(s, `{"name":"%s","type":"`, v.Name)
	if tag := typeTag(v.LiteralType); tag != "" {
		fmt.Fprintf(s, `%s"`, tag)
	} else {
		fmt.Fprintf(s, `unsupported","detail":"%s"`, strings.ReplaceAll(v.LiteralType.String(), `"`, "'"))
	}
	if withRequired {
		fmt.Fprintf(s, `,"required":%t`, v.Required)
	}
	s.WriteByte('}')
}

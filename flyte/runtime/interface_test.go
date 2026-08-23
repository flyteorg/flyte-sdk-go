package runtime

import (
	"strings"
	"testing"

	corepb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	"github.com/stretchr/testify/assert"
)

func TestDescriptorKeepsDeclarationOrderWhileWireFormSorts(t *testing.T) {
	iface := &Interface{
		Inputs: []Variable{
			{Name: "x", LiteralType: simpleLiteralType(corepb.SimpleType_INTEGER), Required: true},
			{Name: "about", LiteralType: simpleLiteralType(corepb.SimpleType_STRING), Required: true},
		},
		Outputs: []Variable{
			{Name: "o0", LiteralType: simpleLiteralType(corepb.SimpleType_STRING), Required: true},
		},
	}

	// Descriptor: declaration order (x before about), pinned byte-for-byte —
	// same format as the Rust SDK's descriptor.
	json := iface.DescriptorJSON("t")
	assert.Equal(t,
		`{"flyte_interface_version":1,"task":"t","inputs":[`+
			`{"name":"x","type":"integer","required":true},`+
			`{"name":"about","type":"string","required":true}],`+
			`"outputs":[{"name":"o0","type":"string"}]}`,
		json)

	// Wire form: key-sorted (about before x), matching Python.
	var keys []string
	for _, v := range iface.Typed().GetInputs().GetVariables() {
		keys = append(keys, v.GetKey())
	}
	assert.Equal(t, []string{"about", "x"}, keys)
}

func TestUnmappableTypeIsReportedNotGuessed(t *testing.T) {
	unmappable := simpleLiteralType(corepb.SimpleType_BINARY)
	assert.Empty(t, typeTag(unmappable))
	json := (&Interface{Inputs: []Variable{{Name: "b", LiteralType: unmappable, Required: true}}}).DescriptorJSON("t")
	assert.Contains(t, json, `"type":"unsupported"`)
	assert.Contains(t, json, `"detail"`)
	assert.False(t, strings.Contains(json, `""detail`), "detail must not introduce stray quotes: %s", json)
}

func TestNoArgNoOutputTaskIsStillValidJSON(t *testing.T) {
	json := (&Interface{}).DescriptorJSON("nothing")
	assert.Equal(t, `{"flyte_interface_version":1,"task":"nothing","inputs":[],"outputs":[]}`, json)
}

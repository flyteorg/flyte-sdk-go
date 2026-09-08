package runtime

import (
	"encoding/hex"
	"reflect"
	"testing"

	corepb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stats struct {
	Mean  float64 `msgpack:"mean"`
	Count int64   `msgpack:"count"`
	Label string  `msgpack:"label"`
}

// Python: MessagePackEncoder(Stats).encode(Stats(mean=1.5, count=3, label="demo")).hex()
// mashumaro, rmp_serde::to_vec_named and vmihailenco/msgpack all write
// string-keyed maps in field order, so the bytes must match exactly.
func TestMsgpackStructMatchesMashumaro(t *testing.T) {
	const golden = "83a46d65616ecb3ff8000000000000a5636f756e7403a56c6162656ca464656d6f"
	s := stats{Mean: 1.5, Count: 3, Label: "demo"}

	lit, err := toLiteral(reflect.ValueOf(s))
	require.NoError(t, err)
	binary := lit.GetScalar().GetBinary()
	require.NotNil(t, binary)
	assert.Equal(t, "msgpack", binary.GetTag())
	assert.Equal(t, golden, hex.EncodeToString(binary.GetValue()))

	// Decode the Python-produced bytes back into the Go struct.
	decoded, err := fromLiteral(lit, reflect.TypeOf(stats{}))
	require.NoError(t, err)
	assert.Equal(t, s, decoded.Interface())
}

func TestPrimitiveLiteralRoundtrips(t *testing.T) {
	roundtrip := func(v any) any {
		lit, err := toLiteral(reflect.ValueOf(v))
		require.NoError(t, err)
		out, err := fromLiteral(lit, reflect.TypeOf(v))
		require.NoError(t, err)
		return out.Interface()
	}
	assert.Equal(t, int64(42), roundtrip(int64(42)))
	assert.Equal(t, int32(7), roundtrip(int32(7)))
	assert.Equal(t, 12, roundtrip(12))
	assert.Equal(t, 1.25, roundtrip(1.25))
	assert.Equal(t, "x", roundtrip("x"))
	assert.Equal(t, true, roundtrip(true))

	// Lenient int → float, matching Python.
	intLit, err := toLiteral(reflect.ValueOf(int64(3)))
	require.NoError(t, err)
	f, err := fromLiteral(intLit, reflect.TypeOf(float64(0)))
	require.NoError(t, err)
	assert.Equal(t, 3.0, f.Interface())

	// Type mismatch errors.
	boolLit, err := toLiteral(reflect.ValueOf(true))
	require.NoError(t, err)
	_, err = fromLiteral(boolLit, reflect.TypeOf(int64(0)))
	assert.Error(t, err)

	// Integer overflow for narrow targets errors.
	bigLit, err := toLiteral(reflect.ValueOf(int64(1) << 40))
	require.NoError(t, err)
	_, err = fromLiteral(bigLit, reflect.TypeOf(int32(0)))
	assert.Error(t, err)
}

// Named primitive types register fine (validation is Kind-based), so decoding
// must produce a value of the declared type — reflect.Call panics on a plain
// string handed to a `type Label string` parameter.
type (
	namedLabel string
	namedFlag  bool
	namedCount int32
	namedRatio float32
)

func TestNamedPrimitiveTypesRoundtrip(t *testing.T) {
	cases := []any{namedLabel("x"), namedFlag(true), namedCount(7), namedRatio(1.5)}
	for _, v := range cases {
		lit, err := toLiteral(reflect.ValueOf(v))
		require.NoError(t, err)
		out, err := fromLiteral(lit, reflect.TypeOf(v))
		require.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(v), out.Type(), "decoded value must have the declared type")
		assert.Equal(t, v, out.Interface())
	}
}

func TestTypedInterfaceIsKeySorted(t *testing.T) {
	intType := simpleLiteralType(corepb.SimpleType_INTEGER)
	strType := simpleLiteralType(corepb.SimpleType_STRING)
	boolType := simpleLiteralType(corepb.SimpleType_BOOLEAN)
	iface := buildTypedInterface(
		[]Variable{{Name: "zeta", LiteralType: intType}, {Name: "alpha", LiteralType: strType}},
		[]Variable{{Name: "o0", LiteralType: boolType}},
	)
	var keys []string
	for _, v := range iface.GetInputs().GetVariables() {
		keys = append(keys, v.GetKey())
	}
	assert.Equal(t, []string{"alpha", "zeta"}, keys)
}

func TestEnvelopesPreserveDeclarationOrder(t *testing.T) {
	inputs, err := buildInputs([]string{"b", "a"},
		[]reflect.Value{reflect.ValueOf(int64(1)), reflect.ValueOf(int64(2))})
	require.NoError(t, err)
	var names []string
	for _, n := range inputs.GetLiterals() {
		names = append(names, n.GetName())
	}
	assert.Equal(t, []string{"b", "a"}, names)
}

func TestUnsupportedTypesAreRejected(t *testing.T) {
	_, err := literalTypeOf(reflect.TypeOf([]int{}))
	assert.Error(t, err)
	_, err = literalTypeOf(reflect.TypeOf(map[string]int{}))
	assert.Error(t, err)
	_, err = literalTypeOf(reflect.TypeOf(&stats{}))
	assert.Error(t, err)
}

package runtime

// Cross-language golden tests: values generated from the Python SDK and pinned
// in flyte-sdk-rust's crates/flyte/tests/golden.rs; the Go implementation must
// stay byte-compatible with both.

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Python: base36_encode(hashlib.md5(b"a0-IH-TH-1").digest()) == "ape1kkafckt4ekjb0537lcq3u"
func TestSubActionNameMatchesPython(t *testing.T) {
	assert.Equal(t, "ape1kkafckt4ekjb0537lcq3u", subActionName("a0", "IH", "TH", 1))
}

// Python: generate_inputs_hash_from_proto(Inputs[a=42, b="hi"])
// == "dtUuRhyBlE9ABcxCjKH0XnafQA378BrguHhlFiUXcDs="
// Also pins the deterministic proto encoding of the int literal (0a040a02082a).
func TestInputsHashMatchesPython(t *testing.T) {
	litA, err := toLiteral(reflect.ValueOf(int64(42)))
	require.NoError(t, err)
	litB, err := toLiteral(reflect.ValueOf("hi"))
	require.NoError(t, err)

	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(litA)
	require.NoError(t, err)
	assert.Equal(t, "0a040a02082a", hex.EncodeToString(encoded))

	inputs, err := buildInputs([]string{"a", "b"}, []reflect.Value{reflect.ValueOf(int64(42)), reflect.ValueOf("hi")})
	require.NoError(t, err)
	_ = litB
	h, err := inputsHash(inputs)
	require.NoError(t, err)
	assert.Equal(t, "dtUuRhyBlE9ABcxCjKH0XnafQA378BrguHhlFiUXcDs=", h)
}

func TestEmptyInputsHashIsEmptyString(t *testing.T) {
	inputs, err := buildInputs(nil, nil)
	require.NoError(t, err)
	h, err := inputsHash(inputs)
	require.NoError(t, err)
	assert.Equal(t, "", h)
}

func TestSequencerCountsPerKeyFromOne(t *testing.T) {
	var s sequencer
	assert.Equal(t, uint32(1), s.next("f:h1"))
	assert.Equal(t, uint32(2), s.next("f:h1"))
	assert.Equal(t, uint32(1), s.next("f:h2"))
	assert.Equal(t, uint32(1), s.next("g:h1"))
	assert.Equal(t, uint32(3), s.next("f:h1"))
}

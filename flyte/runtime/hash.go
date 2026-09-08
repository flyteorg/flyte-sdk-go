package runtime

import (
	"crypto/md5" //nolint:gosec // action naming, not security — must match Python's algorithm
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"

	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"google.golang.org/protobuf/proto"
)

// Deterministic naming/hashing, byte-compatible with the Python and Rust SDKs.
//
// Python references:
//   - flyte/_utils/helpers.py::base36_encode
//   - flyte/_internal/runtime/convert.py::hash_data / generate_inputs_hash_from_proto
//   - flyte/models.py::ActionID.new_sub_action_from

// hashData is base64_std(sha256(data)) with padding — Python's convert.hash_data.
func hashData(data []byte) string {
	digest := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(digest[:])
}

// inputsHash hashes an Inputs envelope — Python's generate_inputs_hash_from_proto.
// Empty inputs hash to the empty string.
func inputsHash(inputs *taskpb.Inputs) (string, error) {
	if len(inputs.GetLiterals()) == 0 {
		return "", nil
	}
	var combined []byte
	for _, named := range inputs.GetLiterals() {
		combined = append(combined, named.GetName()...)
		combined = append(combined, ':')
		if named.GetValue() != nil {
			// Deterministic proto serialization: matches Python's
			// SerializeToString(deterministic=True) and prost's tag-order
			// encoding for our scalar/binary literals.
			b, err := proto.MarshalOptions{Deterministic: true}.Marshal(named.GetValue())
			if err != nil {
				return "", SystemErrorf("failed to serialize input literal %q: %w", named.GetName(), err)
			}
			combined = append(combined, b...)
		}
		combined = append(combined, ';')
	}
	return hashData(combined), nil
}

// base36Encode encodes a big-endian md5 digest with alphabet 0-9a-z — Python's
// base36_encode. big.Int.Text(36) uses exactly that alphabet.
func base36Encode(digest [16]byte) string {
	return new(big.Int).SetBytes(digest[:]).Text(36)
}

// subActionName is the deterministic sub-action name — Python's
// ActionID.new_sub_action_from. All components must be stable across attempts:
// recovery matches previously recorded actions by this name. seq keeps repeated
// identical calls distinct (N calls to a step are N steps, not one memoized
// action).
func subActionName(parent, inputHash, identity string, seq uint32) string {
	components := fmt.Sprintf("%s-%s-%s-%d", parent, inputHash, identity, seq)
	return base36Encode(md5.Sum([]byte(components))) //nolint:gosec
}

package flyte

import (
	"fmt"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
)

// Config holds the configuration for the Flyte client
type Config struct {
	// Endpoint is the gRPC endpoint (e.g., "dns:///localhost:8089")
	Endpoint string

	// Insecure indicates whether to use an insecure connection (for local dev)
	Insecure bool

	// Org is the organization name
	Org string

	// Project is the default project
	Project string

	// Domain is the default domain (e.g., "development", "production")
	Domain string

	// OAuth ClientSecret authentication
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scopes       []string
}

// Validate checks if the config has required fields
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if c.Project == "" {
		return fmt.Errorf("project is required")
	}
	if c.Domain == "" {
		return fmt.Errorf("domain is required")
	}
	// OAuth validation only if credentials are provided
	if c.ClientID != "" || c.ClientSecret != "" {
		if c.ClientID == "" {
			return fmt.Errorf("client_id is required when using OAuth")
		}
		if c.ClientSecret == "" {
			return fmt.Errorf("client_secret is required when using OAuth")
		}
		if c.TokenURL == "" {
			return fmt.Errorf("token_url is required when using OAuth")
		}
	}
	return nil
}

// isTerminalPhase checks if a phase is terminal
func isTerminalPhase(phase common.ActionPhase) bool {
	switch phase {
	case common.ActionPhase_ACTION_PHASE_SUCCEEDED,
		common.ActionPhase_ACTION_PHASE_FAILED,
		common.ActionPhase_ACTION_PHASE_ABORTED,
		common.ActionPhase_ACTION_PHASE_TIMED_OUT:
		return true
	default:
		return false
	}
}

// convertInputsToLiterals converts Go map to protobuf taskpb.Inputs
func convertInputsToLiterals(inputs map[string]interface{}) (*taskpb.Inputs, error) {
	if inputs == nil {
		return &taskpb.Inputs{}, nil
	}

	namedLiterals := make([]*taskpb.NamedLiteral, 0, len(inputs))
	for k, v := range inputs {
		lit, err := toLiteral(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert input %s: %w", k, err)
		}
		namedLiterals = append(namedLiterals, &taskpb.NamedLiteral{
			Name:  k,
			Value: lit,
		})
	}

	return &taskpb.Inputs{
		Literals: namedLiterals,
	}, nil
}

// toLiteral converts a Go value to a core.Literal
func toLiteral(v interface{}) (*core.Literal, error) {
	switch val := v.(type) {
	case int:
		return intLiteral(int64(val)), nil
	case int32:
		return intLiteral(int64(val)), nil
	case int64:
		return intLiteral(val), nil
	case float32:
		return floatLiteral(float64(val)), nil
	case float64:
		return floatLiteral(val), nil
	case string:
		return stringLiteral(val), nil
	case bool:
		return boolLiteral(val), nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

// Helper functions to create literals
func intLiteral(v int64) *core.Literal {
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: &core.Scalar{
				Value: &core.Scalar_Primitive{
					Primitive: &core.Primitive{
						Value: &core.Primitive_Integer{
							Integer: v,
						},
					},
				},
			},
		},
	}
}

func floatLiteral(v float64) *core.Literal {
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: &core.Scalar{
				Value: &core.Scalar_Primitive{
					Primitive: &core.Primitive{
						Value: &core.Primitive_FloatValue{
							FloatValue: v,
						},
					},
				},
			},
		},
	}
}

func stringLiteral(v string) *core.Literal {
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: &core.Scalar{
				Value: &core.Scalar_Primitive{
					Primitive: &core.Primitive{
						Value: &core.Primitive_StringValue{
							StringValue: v,
						},
					},
				},
			},
		},
	}
}

func boolLiteral(v bool) *core.Literal {
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: &core.Scalar{
				Value: &core.Scalar_Primitive{
					Primitive: &core.Primitive{
						Value: &core.Primitive_Boolean{
							Boolean: v,
						},
					},
				},
			},
		},
	}
}

// convertLiteralToGo converts a core.Literal back to a Go value
func convertLiteralToGo(lit *core.Literal) (interface{}, error) {
	if lit.GetScalar() != nil {
		scalar := lit.GetScalar()
		if prim := scalar.GetPrimitive(); prim != nil {
			switch prim.Value.(type) {
			case *core.Primitive_Integer:
				return prim.GetInteger(), nil
			case *core.Primitive_FloatValue:
				return prim.GetFloatValue(), nil
			case *core.Primitive_StringValue:
				return prim.GetStringValue(), nil
			case *core.Primitive_Boolean:
				return prim.GetBoolean(), nil
			}
		}
	}
	return nil, fmt.Errorf("unsupported literal type")
}

// convertOutputsToGo converts protobuf outputs to Go map
func convertOutputsToGo(outputs *taskpb.Outputs) (map[string]interface{}, error) {
	if outputs == nil || len(outputs.Literals) == 0 {
		return map[string]interface{}{}, nil
	}

	result := make(map[string]interface{}, len(outputs.Literals))
	for _, namedLit := range outputs.Literals {
		val, err := convertLiteralToGo(namedLit.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert output %s: %w", namedLit.Name, err)
		}
		result[namedLit.Name] = val
	}
	return result, nil
}

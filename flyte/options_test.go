package flyte

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRunSpecRelationAndRecover(t *testing.T) {
	t.Run("no relation by default", func(t *testing.T) {
		spec := buildRunSpec(newRunOptions(nil), "org", "proj", "dev")
		assert.Nil(t, spec.Relation)
		assert.Nil(t, spec.Recover)
	})

	t.Run("WithRelation", func(t *testing.T) {
		o := newRunOptions([]RunOption{WithRelation("src-run", RelationTypeRerun)})
		spec := buildRunSpec(o, "org", "proj", "dev")
		require.NotNil(t, spec.Relation)
		assert.Equal(t, "org", spec.Relation.GetRelatedTo().GetOrg())
		assert.Equal(t, "proj", spec.Relation.GetRelatedTo().GetProject())
		assert.Equal(t, "dev", spec.Relation.GetRelatedTo().GetDomain())
		assert.Equal(t, "src-run", spec.Relation.GetRelatedTo().GetName())
		assert.Equal(t, RelationTypeRerun, spec.Relation.GetRelationType())
		assert.Nil(t, spec.Recover)
	})

	t.Run("WithRecover", func(t *testing.T) {
		o := newRunOptions([]RunOption{WithRecover("failed-run")})
		spec := buildRunSpec(o, "org", "proj", "dev")
		require.NotNil(t, spec.Relation)
		assert.Equal(t, "failed-run", spec.Relation.GetRelatedTo().GetName())
		assert.Equal(t, RelationTypeRecover, spec.Relation.GetRelationType())
		require.NotNil(t, spec.Recover)
		assert.Empty(t, spec.Recover.GetForceRerunActions())
	})

	t.Run("WithRecover and WithForceRerunActions", func(t *testing.T) {
		o := newRunOptions([]RunOption{
			WithRecover("failed-run"),
			WithForceRerunActions("a1", "a2"),
			WithForceRerunActions("a3"),
		})
		spec := buildRunSpec(o, "org", "proj", "dev")
		require.NotNil(t, spec.Recover)
		assert.Equal(t, []string{"a1", "a2", "a3"}, spec.Recover.GetForceRerunActions())
	})
}

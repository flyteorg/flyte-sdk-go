package flyte

import (
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
)

// RunOption customizes a single Run call. The options mirror the parameters of
// the Python SDK's flyte.with_runcontext(...).
type RunOption func(*runOptions)

// RelationType describes how a new run was derived from an existing run.
type RelationType = common.RelationType

const (
	// RelationTypeSpawn marks a run programmatically spawned by another run.
	RelationTypeSpawn = common.RelationType_RELATION_TYPE_SPAWN
	// RelationTypeRerun marks a plain rerun: every action re-executes.
	RelationTypeRerun = common.RelationType_RELATION_TYPE_RERUN
	// RelationTypeRecover marks a recovery run; use WithRecover to create one.
	RelationTypeRecover = common.RelationType_RELATION_TYPE_RECOVER
)

type runOptions struct {
	name                 string
	project              string
	domain               string
	serviceAccount       string
	labels               map[string]string
	annotations          map[string]string
	envVars              map[string]string
	interruptible        *bool
	overwriteCache       bool
	queue                string
	maxActionConcurrency uint32
	rawDataPath          string
	runBaseDir           string
	relatedRun           string
	relationType         RelationType
	recover              bool
	forceRerunActions    []string
}

func newRunOptions(opts []RunOption) *runOptions {
	o := &runOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// WithRunName sets an explicit run name. Names must be unique within a
// project/domain; when unset the server generates one.
func WithRunName(name string) RunOption {
	return func(o *runOptions) { o.name = name }
}

// WithProject overrides the project the run is created in.
func WithProject(project string) RunOption {
	return func(o *runOptions) { o.project = project }
}

// WithDomain overrides the domain the run is created in.
func WithDomain(domain string) RunOption {
	return func(o *runOptions) { o.domain = domain }
}

// WithServiceAccount sets the Kubernetes service account the run executes as.
func WithServiceAccount(sa string) RunOption {
	return func(o *runOptions) { o.serviceAccount = sa }
}

// WithLabels attaches labels to the run (merged with any WithLabel calls).
func WithLabels(labels map[string]string) RunOption {
	return func(o *runOptions) {
		for k, v := range labels {
			o.setLabel(k, v)
		}
	}
}

// WithLabel attaches a single label to the run.
func WithLabel(key, value string) RunOption {
	return func(o *runOptions) { o.setLabel(key, value) }
}

// WithAnnotations attaches annotations to the run.
func WithAnnotations(annotations map[string]string) RunOption {
	return func(o *runOptions) {
		for k, v := range annotations {
			o.setAnnotation(k, v)
		}
	}
}

// WithAnnotation attaches a single annotation to the run.
func WithAnnotation(key, value string) RunOption {
	return func(o *runOptions) { o.setAnnotation(key, value) }
}

// WithEnvVars sets environment variables for the run's actions.
func WithEnvVars(envVars map[string]string) RunOption {
	return func(o *runOptions) {
		for k, v := range envVars {
			o.setEnvVar(k, v)
		}
	}
}

// WithEnvVar sets a single environment variable for the run's actions.
func WithEnvVar(key, value string) RunOption {
	return func(o *runOptions) { o.setEnvVar(key, value) }
}

// WithInterruptible explicitly marks the run interruptible (or not). When the
// option is not used, the platform default applies.
func WithInterruptible(interruptible bool) RunOption {
	return func(o *runOptions) { o.interruptible = &interruptible }
}

// WithOverwriteCache re-executes the run even when cached results exist.
func WithOverwriteCache(overwrite bool) RunOption {
	return func(o *runOptions) { o.overwriteCache = overwrite }
}

// WithQueue selects the queue (cluster) this run executes on.
func WithQueue(queue string) RunOption {
	return func(o *runOptions) { o.queue = queue }
}

// WithMaxActionConcurrency caps how many actions of this run may execute
// concurrently. Zero means unlimited.
func WithMaxActionConcurrency(n uint32) RunOption {
	return func(o *runOptions) { o.maxActionConcurrency = n }
}

// WithRawDataPath sets the prefix where offloaded user data (blobs, datasets)
// is written, e.g. "s3://bucket/prefix".
func WithRawDataPath(path string) RunOption {
	return func(o *runOptions) { o.rawDataPath = path }
}

// WithRunBaseDir sets the base directory for the run's metadata (inputs.pb,
// outputs.pb). Leave empty to use the org/project/domain settings default.
func WithRunBaseDir(dir string) RunOption {
	return func(o *runOptions) { o.runBaseDir = dir }
}

// WithRelation records that this run derives from an existing run in the same
// project/domain (e.g. RelationTypeRerun for provenance-only reruns). To create
// a recovery run use WithRecover, which also enables recovery semantics.
func WithRelation(runName string, relationType RelationType) RunOption {
	return func(o *runOptions) {
		o.relatedRun = runName
		o.relationType = relationType
	}
}

// WithRecover creates this run as a recovery of an existing run: actions that
// succeeded (or were recovered) in the source run are skipped and their outputs
// reused; everything else re-executes. It is the Go equivalent of Python's
// flyte.rerun with recover=True and implies
// WithRelation(runName, RelationTypeRecover).
func WithRecover(runName string) RunOption {
	return func(o *runOptions) {
		o.relatedRun = runName
		o.relationType = RelationTypeRecover
		o.recover = true
	}
}

// WithForceRerunActions names actions that must re-execute in a recovery run
// even when they succeeded in the source run. A listed parent action re-enqueues
// its children, each of which goes through the recovery decision individually.
// Only meaningful together with WithRecover; unknown names are ignored.
func WithForceRerunActions(names ...string) RunOption {
	return func(o *runOptions) {
		o.forceRerunActions = append(o.forceRerunActions, names...)
	}
}

func (o *runOptions) setLabel(k, v string) {
	if o.labels == nil {
		o.labels = map[string]string{}
	}
	o.labels[k] = v
}

func (o *runOptions) setAnnotation(k, v string) {
	if o.annotations == nil {
		o.annotations = map[string]string{}
	}
	o.annotations[k] = v
}

func (o *runOptions) setEnvVar(k, v string) {
	if o.envVars == nil {
		o.envVars = map[string]string{}
	}
	o.envVars[k] = v
}

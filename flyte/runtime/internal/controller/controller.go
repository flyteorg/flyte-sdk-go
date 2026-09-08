// Package controller is the task runtime's client for the unified
// flyteidl2.actions.ActionsService: it records finished trace actions
// (Enqueue) and looks up previously recorded ones through a per-parent
// informer over WatchForUpdates. The Go counterpart of flyte_core's
// CoreBaseController, minus everything v1 does not need.
package controller

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	stdconfig "github.com/flyteorg/flyte/v2/flytestdlib/config"
	actionspb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/actions"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/actions/actionsconnect"
	commonpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	corepb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	workflowpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	admin "github.com/unionai/flyte-sdk-go/flyte/client"
	"github.com/unionai/flyte-sdk-go/flyte/client/cache"
)

// RunIdentifier identifies the run every action of this worker belongs to.
type RunIdentifier = commonpb.RunIdentifier

// Recorded is what the runtime needs to know about a previously recorded
// action.
type Recorded struct {
	Failed bool
	// Full URI of the recorded outputs.pb, when outputs were recorded.
	OutputsURI string
}

// TraceRecord is everything needed to record a finished trace as a child
// action.
type TraceRecord struct {
	ParentActionName string
	ActionName       string
	FriendlyName     string
	InputsURI        string
	OutputsURI       string // empty when the trace has no outputs
	Start, End       float64
	RunOutputBase    string
	Interface        *corepb.TypedInterface
}

// Controller talks ActionsService for one run. Informers (watch streams) are
// created lazily per parent action and shared.
type Controller struct {
	client actionsconnect.ActionsServiceClient
	runID  *RunIdentifier

	mu        sync.Mutex
	informers map[string]*informer // keyed "{run}.{parent}" like flyte_core
}

// New builds a controller from the process environment, mirroring the Rust
// worker's build_controller:
//
//   - _UNION_EAGER_API_KEY (or EAGER_API_KEY) set → authenticated client
//     credentials against the key's endpoint.
//   - otherwise _U_EP_OVERRIDE (default host.docker.internal:8090), http://
//     when _U_INSECURE is truthy or the host is local (see localHost).
func New(ctx context.Context, runID *RunIdentifier) (*Controller, error) {
	client, err := buildClient(ctx)
	if err != nil {
		return nil, err
	}
	return &Controller{client: client, runID: runID, informers: map[string]*informer{}}, nil
}

func envNonEmpty(key string) string { return os.Getenv(key) }

func truthy(v string) bool {
	switch strings.ToLower(v) {
	case "1", "true", "t", "yes":
		return true
	}
	return false
}

func buildClient(ctx context.Context) (actionsconnect.ActionsServiceClient, error) {
	if key := apiKey(); key != "" {
		return buildAuthenticatedClient(ctx, key)
	}
	raw := envNonEmpty("_U_EP_OVERRIDE")
	if raw == "" {
		raw = "host.docker.internal:8090"
	}
	endpoint := endpointURL(raw, truthy(envNonEmpty("_U_INSECURE")))
	return actionsconnect.NewActionsServiceClient(&http.Client{}, endpoint), nil
}

// endpointURL adds a scheme to a bare host[:port] endpoint override: http://
// when insecure is set or the host is local, https:// otherwise. An override
// that already carries a scheme is used as is.
//
// The Rust worker substring-matches "localhost", "127.0.0.1" and "docker"
// anywhere in the string; that last one also matches hostnames like
// docker-gw.corp.example.com and would silently downgrade them to plaintext.
// Go matches the host exactly (see localHost) — anything else that needs
// plaintext must say so with _U_INSECURE=true.
func endpointURL(raw string, insecure bool) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if insecure || localHost(raw) {
		return "http://" + raw
	}
	return "https://" + raw
}

// localHost reports whether a host[:port] names the local machine or the
// Docker host gateway (the local-sandbox default): localhost and *.localhost,
// loopback addresses, host.docker.internal.
func localHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "host.docker.internal" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// apiKey reads the worker API key, promoting EAGER_API_KEY and translating
// url-safe base64 (-_ from `flyte create api-key`) to the standard alphabet.
func apiKey() string {
	key := envNonEmpty("_UNION_EAGER_API_KEY")
	if key == "" {
		key = envNonEmpty("EAGER_API_KEY")
	}
	return strings.NewReplacer("-", "+", "_", "/").Replace(key)
}

// buildAuthenticatedClient decodes the platform API key —
// base64("endpoint:clientId:clientSecret:org", endpoint may contain colons) —
// and builds a client-credentials-authenticated clientset. Same key format as
// flyte.DecodeAPIKey on the launch side.
func buildAuthenticatedClient(ctx context.Context, key string) (actionsconnect.ActionsServiceClient, error) {
	rawBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(key))
	if err != nil {
		return nil, fmt.Errorf("invalid api key: %w", err)
	}
	parts := strings.SplitN(string(rawBytes), ":", 4)
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid api key format: expected 4 ':'-separated parts, got %d", len(parts))
	}
	endpoint, clientID, clientSecret := parts[0], parts[1], parts[2]

	u, err := url.Parse(sanitizeEndpoint(endpoint))
	if err != nil {
		return nil, fmt.Errorf("invalid api key endpoint %q: %w", endpoint, err)
	}
	cfg := *admin.GetConfig(ctx)
	cfg.Endpoint = stdconfig.URL{URL: *u}
	cfg.AuthType = admin.AuthTypeClientSecret
	cfg.ClientID = clientID
	cfg.ClientSecret = clientSecret
	cfg.ClientSecretLocation = ""

	clientset, err := admin.NewRunClientsetBuilder().
		WithConfig(&cfg).
		WithTokenCache(cache.NewTokenCacheInMemoryProvider()).
		Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build authenticated client: %w", err)
	}
	return clientset.ActionsServiceClient(), nil
}

func sanitizeEndpoint(endpoint string) string {
	ep := strings.TrimSpace(strings.TrimSuffix(endpoint, "/"))
	ep = strings.TrimPrefix(ep, "dns:///")
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		ep = "https://" + ep
	}
	return ep
}

// setActionHeaders adds the routing metadata every ActionsService call
// carries (flyte_core's actions_metadata).
func (c *Controller) setActionHeaders(h http.Header, parentActionName string) {
	h.Set("x-actions-project", c.runID.GetProject())
	h.Set("x-actions-domain", c.runID.GetDomain())
	h.Set("x-actions-run", c.runID.GetName())
	h.Set("x-actions-parent-action", parentActionName)
}

// RecordTrace enqueues a finished trace action. AlreadyExists counts as
// success: re-recording on retry is idempotent.
func (c *Controller) RecordTrace(ctx context.Context, rec TraceRecord) error {
	var outputs *taskpb.OutputReferences
	if rec.OutputsURI != "" {
		outputs = &taskpb.OutputReferences{OutputUri: rec.OutputsURI}
	}
	end := floatTimestamp(rec.End)
	req := connect.NewRequest(&actionspb.EnqueueRequest{
		Action: &actionspb.Action{
			ActionId: &commonpb.ActionIdentifier{
				Run:  c.runID,
				Name: rec.ActionName,
			},
			ParentActionName: &rec.ParentActionName,
			InputUri:         rec.InputsURI,
			RunOutputBase:    rec.RunOutputBase,
			Spec: &actionspb.Action_Trace{Trace: &workflowpb.TraceAction{
				Name:      rec.FriendlyName,
				Phase:     commonpb.ActionPhase_ACTION_PHASE_SUCCEEDED,
				StartTime: floatTimestamp(rec.Start),
				EndTime:   end,
				Outputs:   outputs,
				Spec:      &taskpb.TraceSpec{Interface: rec.Interface},
			}},
		},
	})
	c.setActionHeaders(req.Header(), rec.ParentActionName)
	_, err := c.client.Enqueue(ctx, req)
	if connect.CodeOf(err) == connect.CodeAlreadyExists {
		slog.Info("trace action already exists, continuing", "action", rec.ActionName)
		return nil
	}
	if err != nil {
		return fmt.Errorf("enqueue failed: %w", err)
	}
	return nil
}

// LookupAction returns the recorded child action with the given name, or nil
// when nothing is recorded. The first lookup for a parent creates its informer
// and waits for the watch stream's initial sync (bounded; the bound is paid at
// most once per informer, so a dead ActionsService costs one wait, not one
// per traced call).
func (c *Controller) LookupAction(ctx context.Context, actionName, parentActionName string) (*Recorded, error) {
	inf := c.informerFor(parentActionName)
	inf.awaitReady(ctx, 5*time.Second)
	update := inf.get(actionName)
	if update == nil {
		return nil, nil
	}
	return &Recorded{
		Failed:     update.GetPhase() == commonpb.ActionPhase_ACTION_PHASE_FAILED || update.GetError() != nil,
		OutputsURI: update.GetOutputUri(),
	}, nil
}

// Finalize tears down the informer for a parent action once its task is done.
func (c *Controller) Finalize(parentActionName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.runID.GetName() + "." + parentActionName
	if inf, ok := c.informers[key]; ok {
		inf.stop()
		delete(c.informers, key)
	}
}

func (c *Controller) informerFor(parentActionName string) *informer {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.runID.GetName() + "." + parentActionName
	if inf, ok := c.informers[key]; ok {
		return inf
	}
	inf := newInformer(c.client, c.runID, parentActionName, c.setActionHeaders)
	c.informers[key] = inf
	return inf
}

func floatTimestamp(seconds float64) *timestamppb.Timestamp {
	sec := int64(seconds)
	nanos := int32((seconds - float64(sec)) * 1e9)
	return &timestamppb.Timestamp{Seconds: sec, Nanos: nanos}
}

// informer caches ActionUpdates for one parent action, fed by a reconnecting
// WatchForUpdates stream. The stream replays current state, emits a sentinel,
// then streams live updates — awaitReady unblocks at the first sentinel.
type informer struct {
	cancel context.CancelFunc
	ready  chan struct{}
	once   sync.Once

	mu      sync.Mutex
	actions map[string]*workflowpb.ActionUpdate
}

func newInformer(
	client actionsconnect.ActionsServiceClient,
	runID *RunIdentifier,
	parentActionName string,
	setHeaders func(http.Header, string),
) *informer {
	ctx, cancel := context.WithCancel(context.Background())
	inf := &informer{
		cancel:  cancel,
		ready:   make(chan struct{}),
		actions: map[string]*workflowpb.ActionUpdate{},
	}
	go inf.watch(ctx, client, runID, parentActionName, setHeaders)
	return inf
}

func (i *informer) watch(
	ctx context.Context,
	client actionsconnect.ActionsServiceClient,
	runID *RunIdentifier,
	parentActionName string,
	setHeaders func(http.Header, string),
) {
	backoff := time.Second
	for ctx.Err() == nil {
		req := connect.NewRequest(&actionspb.WatchForUpdatesRequest{
			Filter: &actionspb.WatchForUpdatesRequest_ParentActionId{
				ParentActionId: &commonpb.ActionIdentifier{Run: runID, Name: parentActionName},
			},
		})
		setHeaders(req.Header(), parentActionName)
		stream, err := client.WatchForUpdates(ctx, req)
		if err == nil {
			for stream.Receive() {
				switch msg := stream.Msg().GetMessage().(type) {
				case *actionspb.WatchForUpdatesResponse_ActionUpdate:
					i.mu.Lock()
					i.actions[msg.ActionUpdate.GetActionId().GetName()] = msg.ActionUpdate
					i.mu.Unlock()
				case *actionspb.WatchForUpdatesResponse_ControlMessage:
					if msg.ControlMessage.GetSentinel() {
						i.markReady()
						// A stream that reached its sentinel is healthy: a later
						// drop is a fresh incident, not a continuation of the last.
						backoff = time.Second
					}
				}
			}
			err = stream.Err()
			_ = stream.Close()
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("action watch stream dropped; reconnecting", "parent", parentActionName, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff += time.Second
		}
	}
}

func (i *informer) markReady() { i.once.Do(func() { close(i.ready) }) }

// awaitReady waits for the initial sync sentinel, bounded — a lookup against a
// stream that never syncs degrades to a cache miss rather than a hang. A
// timeout marks the informer ready so the wait is paid once, not on every
// lookup; if the stream syncs later, its updates land in the cache anyway.
func (i *informer) awaitReady(ctx context.Context, timeout time.Duration) {
	select {
	case <-i.ready:
	case <-ctx.Done():
	case <-time.After(timeout):
		slog.Warn("action watch did not sync in time; proceeding with possibly cold cache")
		i.markReady()
	}
}

func (i *informer) get(actionName string) *workflowpb.ActionUpdate {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.actions[actionName]
}

func (i *informer) stop() { i.cancel() }

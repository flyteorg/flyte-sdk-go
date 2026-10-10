package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"connectrpc.com/connect"
	"github.com/flyteorg/flyte/v2/flytestdlib/config"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/app"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/app/appconnect"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth/authconnect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAuth answers OAuth discovery with this server's token endpoint and a
// custom authorization header, as a Union control plane can.
type fakeAuth struct {
	authconnect.UnimplementedAuthMetadataServiceHandler
	tokenURL string
}

func (f *fakeAuth) GetOAuth2Metadata(context.Context, *connect.Request[auth.GetOAuth2MetadataRequest]) (*connect.Response[auth.GetOAuth2MetadataResponse], error) {
	return connect.NewResponse(&auth.GetOAuth2MetadataResponse{TokenEndpoint: f.tokenURL}), nil
}

func (f *fakeAuth) GetPublicClientConfig(context.Context, *connect.Request[auth.GetPublicClientConfigRequest]) (*connect.Response[auth.GetPublicClientConfigResponse], error) {
	return connect.NewResponse(&auth.GetPublicClientConfigResponse{AuthorizationMetadataKey: "flyte-authorization"}), nil
}

// fakeApps records the header each Get arrives with.
type fakeApps struct {
	appconnect.UnimplementedAppServiceHandler
	gotAuth string
}

func (f *fakeApps) Get(_ context.Context, req *connect.Request[app.GetRequest]) (*connect.Response[app.GetResponse], error) {
	f.gotAuth = req.Header().Get("flyte-authorization")
	id := req.Msg.GetAppId()
	return connect.NewResponse(&app.GetResponse{App: &app.App{Metadata: &app.Meta{Id: id}}}), nil
}

func TestRunClientsetAppServiceClient(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.Handle(authconnect.NewAuthMetadataServiceHandler(&fakeAuth{tokenURL: srv.URL + "/oauth2/token"}))
	apps := &fakeApps{}
	mux.Handle(appconnect.NewAppServiceHandler(apps))
	mux.HandleFunc("/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "token_type": "bearer", "expires_in": 3600})
	})

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	cs, err := NewRunClientsetBuilder().WithConfig(&Config{
		Endpoint:              config.URL{URL: *u},
		UseInsecureConnection: true,
		AuthType:              AuthTypeClientSecret,
		ClientID:              "id",
		ClientSecret:          "secret",
	}).Build(context.Background())
	require.NoError(t, err)

	id := &app.Identifier{Org: "acme", Project: "p", Domain: "d", Name: "hello"}
	resp, err := cs.AppServiceClient().Get(context.Background(),
		connect.NewRequest(&app.GetRequest{Identifier: &app.GetRequest_AppId{AppId: id}}))
	require.NoError(t, err)
	assert.Equal(t, "hello", resp.Msg.GetApp().GetMetadata().GetId().GetName())
	assert.Equal(t, "Bearer tok", apps.gotAuth, "authenticated like the other clients, on the server's header")
}

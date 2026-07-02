// Package authtest provides a fake AuthMetadataService client for tests.
package authtest

import (
	"context"

	"connectrpc.com/connect"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth/authconnect"
)

// FakeAuthMetadataClient implements authconnect.AuthMetadataServiceClient with
// canned responses for tests.
type FakeAuthMetadataClient struct {
	OAuth2Metadata        *auth.GetOAuth2MetadataResponse
	OAuth2MetadataErr     error
	PublicClientConfig    *auth.GetPublicClientConfigResponse
	PublicClientConfigErr error

	OAuth2MetadataCalls     int
	PublicClientConfigCalls int
}

var _ authconnect.AuthMetadataServiceClient = (*FakeAuthMetadataClient)(nil)

func (f *FakeAuthMetadataClient) GetOAuth2Metadata(
	_ context.Context, _ *connect.Request[auth.GetOAuth2MetadataRequest],
) (*connect.Response[auth.GetOAuth2MetadataResponse], error) {
	f.OAuth2MetadataCalls++
	if f.OAuth2MetadataErr != nil {
		return nil, f.OAuth2MetadataErr
	}
	return connect.NewResponse(f.OAuth2Metadata), nil
}

func (f *FakeAuthMetadataClient) GetPublicClientConfig(
	_ context.Context, _ *connect.Request[auth.GetPublicClientConfigRequest],
) (*connect.Response[auth.GetPublicClientConfigResponse], error) {
	f.PublicClientConfigCalls++
	if f.PublicClientConfigErr != nil {
		return nil, f.PublicClientConfigErr
	}
	return connect.NewResponse(f.PublicClientConfig), nil
}

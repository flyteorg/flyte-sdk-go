package oauth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth"

	"github.com/unionai/flyte-sdk-go/flyte/client/authtest"
)

func TestGenerateClientConfig(t *testing.T) {
	ctx := context.Background()
	fakeAuthClient := &authtest.FakeAuthMetadataClient{
		PublicClientConfig: &auth.GetPublicClientConfigResponse{
			ClientId:    "dummyClient",
			RedirectUri: "dummyRedirectUri",
			Scopes:      []string{"dummyScopes"},
			Audience:    "dummyAudience",
		},
		OAuth2Metadata: &auth.GetOAuth2MetadataResponse{
			Issuer:                        "dummyIssuer",
			AuthorizationEndpoint:         "dummyAuthEndPoint",
			TokenEndpoint:                 "dummyTokenEndpoint",
			CodeChallengeMethodsSupported: []string{"dummyCodeChallenege"},
			DeviceAuthorizationEndpoint:   "dummyDeviceEndpoint",
		},
	}
	oauthConfig, err := BuildConfigFromMetadataService(ctx, fakeAuthClient)
	assert.Nil(t, err)
	assert.NotNil(t, oauthConfig)
	assert.Equal(t, "dummyClient", oauthConfig.ClientID)
	assert.Equal(t, "dummyRedirectUri", oauthConfig.RedirectURL)
	assert.Equal(t, "dummyTokenEndpoint", oauthConfig.Endpoint.TokenURL)
	assert.Equal(t, "dummyAuthEndPoint", oauthConfig.Endpoint.AuthURL)
	assert.Equal(t, "dummyDeviceEndpoint", oauthConfig.DeviceEndpoint)
	assert.Equal(t, "dummyAudience", oauthConfig.Audience)
}

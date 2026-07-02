package admin

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth"
	"github.com/unionai/flyte-sdk-go/flyte/client/authtest"
	tokenCacheMocks "github.com/unionai/flyte-sdk-go/flyte/client/cache/mocks"
	"github.com/unionai/flyte-sdk-go/flyte/client/utils"
)

func TestNewTokenSourceProvider(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                     string
		audienceCfg              string
		scopesCfg                []string
		useAudienceFromAdmin     bool
		clientConfigResponse     *auth.GetPublicClientConfigResponse
		expectedAudience         string
		expectedScopes           []string
		expectedCallsPubEndpoint int
	}{
		{
			name:                     "audience from client config",
			audienceCfg:              "clientConfiguredAud",
			scopesCfg:                []string{"all"},
			clientConfigResponse:     &auth.GetPublicClientConfigResponse{},
			expectedAudience:         "clientConfiguredAud",
			expectedScopes:           []string{"all"},
			expectedCallsPubEndpoint: 0,
		},
		{
			name:                     "audience from public client response",
			audienceCfg:              "clientConfiguredAud",
			useAudienceFromAdmin:     true,
			scopesCfg:                []string{"all"},
			clientConfigResponse:     &auth.GetPublicClientConfigResponse{Audience: "AdminConfiguredAud", Scopes: []string{}},
			expectedAudience:         "AdminConfiguredAud",
			expectedScopes:           []string{"all"},
			expectedCallsPubEndpoint: 1,
		},

		{
			name:                     "audience from client with useAudience from admin false",
			audienceCfg:              "clientConfiguredAud",
			useAudienceFromAdmin:     false,
			scopesCfg:                []string{"all"},
			clientConfigResponse:     &auth.GetPublicClientConfigResponse{Audience: "AdminConfiguredAud", Scopes: []string{}},
			expectedAudience:         "clientConfiguredAud",
			expectedScopes:           []string{"all"},
			expectedCallsPubEndpoint: 0,
		},
	}
	for _, test := range tests {
		cfg := GetConfig(ctx)
		tokenCache := &tokenCacheMocks.TokenCache{}
		metadataClient := &authtest.FakeAuthMetadataClient{
			OAuth2Metadata:     &auth.GetOAuth2MetadataResponse{},
			PublicClientConfig: test.clientConfigResponse,
		}
		cfg.AuthType = AuthTypeClientSecret
		cfg.ClientSecretLocation = "testdata/secret_key"
		cfg.Audience = test.audienceCfg
		cfg.Scopes = test.scopesCfg
		cfg.UseAudienceFromAdmin = test.useAudienceFromAdmin
		flyteTokenSource, err := NewTokenSourceProvider(ctx, cfg, tokenCache, metadataClient)
		assert.Equal(t, test.expectedCallsPubEndpoint, metadataClient.PublicClientConfigCalls)
		assert.NoError(t, err)
		assert.NotNil(t, flyteTokenSource)
		clientCredSourceProvider, ok := flyteTokenSource.(ClientCredentialsTokenSourceProvider)
		assert.True(t, ok)
		assert.Equal(t, test.expectedScopes, clientCredSourceProvider.ccConfig.Scopes)
		assert.Equal(t, url.Values{audienceKey: {test.expectedAudience}}, clientCredSourceProvider.ccConfig.EndpointParams)
	}
}

func TestCustomTokenSource_Token(t *testing.T) {
	ctx := context.Background()
	cfg := GetConfig(ctx)
	cfg.ClientSecretLocation = ""

	minuteAgo := time.Now().Add(-time.Minute)
	hourAhead := time.Now().Add(time.Hour)
	twoHourAhead := time.Now().Add(2 * time.Hour)
	invalidToken := utils.GenTokenWithCustomExpiry(t, minuteAgo)
	validToken := utils.GenTokenWithCustomExpiry(t, hourAhead)
	newToken := utils.GenTokenWithCustomExpiry(t, twoHourAhead)

	tests := []struct {
		name          string
		token         *oauth2.Token
		newToken      *oauth2.Token
		expectedToken *oauth2.Token
	}{
		{
			name:          "no cached token",
			token:         nil,
			newToken:      newToken,
			expectedToken: newToken,
		},
		{
			name:          "cached token valid",
			token:         validToken,
			newToken:      nil,
			expectedToken: validToken,
		},
		{
			name:          "cached token expired",
			token:         invalidToken,
			newToken:      newToken,
			expectedToken: newToken,
		},
		{
			name:          "failed new token",
			token:         invalidToken,
			newToken:      nil,
			expectedToken: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tokenCache := &tokenCacheMocks.TokenCache{}
			tokenCache.On("GetToken").Return(test.token, nil).Maybe()
			tokenCache.On("Lock").Return().Maybe()
			tokenCache.On("Unlock").Return().Maybe()
			provider, err := NewClientCredentialsTokenSourceProvider(ctx, cfg, []string{}, "", tokenCache, "")
			assert.NoError(t, err)
			source, err := provider.GetTokenSource(ctx)
			assert.NoError(t, err)
			customSource, ok := source.(*customTokenSource)
			assert.True(t, ok)

			mockSource := &fakeTokenSource{token: test.newToken}
			if test.newToken == nil {
				mockSource.err = fmt.Errorf("refresh token failed")
			}
			customSource.new = mockSource
			if test.newToken != nil {
				tokenCache.On("SaveToken", test.newToken).Return(nil).Once()
			}
			token, err := source.Token()
			if test.expectedToken != nil {
				assert.Equal(t, test.expectedToken, token)
				assert.NoError(t, err)
			} else {
				assert.Nil(t, token)
				assert.Error(t, err)
			}
			tokenCache.AssertExpectations(t)
			if test.token != validToken && test.newToken != nil {
				assert.Equal(t, 1, mockSource.calls)
			}
		})
	}
}

// fakeTokenSource is a stub oauth2.TokenSource for tests.
type fakeTokenSource struct {
	token *oauth2.Token
	err   error
	calls int
}

func (f *fakeTokenSource) Token() (*oauth2.Token, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.token, nil
}

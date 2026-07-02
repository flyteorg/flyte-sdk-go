package oauth

import (
	"context"

	"connectrpc.com/connect"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/auth/authconnect"
	"golang.org/x/oauth2"
)

// Config oauth2.Config overridden with device endpoint for supporting Device Authorization Grant flow [RFC8268]
type Config struct {
	*oauth2.Config
	DeviceEndpoint string
	// Audience value to be passed when requesting access token using device flow.This needs to be passed in the first request of the device flow currently and is configured in admin public client config.Required when auth server hasn't been configured with default audience"`
	Audience string
}

// BuildConfigFromMetadataService builds OAuth2 config from information retrieved through the anonymous auth metadata service.
func BuildConfigFromMetadataService(ctx context.Context, authMetadataClient authconnect.AuthMetadataServiceClient) (clientConf *Config, err error) {
	clientResp, err := authMetadataClient.GetPublicClientConfig(ctx, connect.NewRequest(&auth.GetPublicClientConfigRequest{}))
	if err != nil {
		return nil, err
	}

	oauthMetaResp, err := authMetadataClient.GetOAuth2Metadata(ctx, connect.NewRequest(&auth.GetOAuth2MetadataRequest{}))
	if err != nil {
		return nil, err
	}

	clientConf = &Config{
		Config: &oauth2.Config{
			ClientID:    clientResp.Msg.GetClientId(),
			RedirectURL: clientResp.Msg.GetRedirectUri(),
			Scopes:      clientResp.Msg.GetScopes(),
			Endpoint: oauth2.Endpoint{
				TokenURL: oauthMetaResp.Msg.GetTokenEndpoint(),
				AuthURL:  oauthMetaResp.Msg.GetAuthorizationEndpoint(),
			},
		},
		DeviceEndpoint: oauthMetaResp.Msg.GetDeviceAuthorizationEndpoint(),
		Audience:       clientResp.Msg.GetAudience(),
	}

	return clientConf, nil
}

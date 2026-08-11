// Initializes an Admin Client that exposes all implemented services by FlyteAdmin server. The library supports different
// authentication flows (see AuthType). It initializes the grpc connection once and reuses it. A grpc load balancing policy
// can be configured as well.
package admin

import (
	"context"
	"path/filepath"
	"time"

	"github.com/flyteorg/flyte/v2/flytestdlib/config"
	"github.com/flyteorg/flyte/v2/flytestdlib/logger"

	"github.com/unionai/flyte-sdk-go/flyte/client/deviceflow"
	"github.com/unionai/flyte-sdk-go/flyte/client/pkce"
)

//go:generate pflags Config --default-var=defaultConfig

const (
	configSectionKey = "admin"
	DefaultClientID  = "flytepropeller"
)

var DefaultClientSecretLocation = filepath.Join(string(filepath.Separator), "etc", "secrets", "client_secret")

//go:generate enumer --type=AuthType -json -yaml -trimprefix=AuthType
type AuthType uint8

const (
	// AuthTypeClientSecret Chooses Client Secret OAuth2 protocol (ref: https://tools.ietf.org/html/rfc6749#section-4.4)
	AuthTypeClientSecret AuthType = iota
	// AuthTypePkce Chooses Proof Key Code Exchange OAuth2 extension protocol (ref: https://tools.ietf.org/html/rfc7636)
	AuthTypePkce
	// AuthTypeExternalCommand Chooses an external authentication process
	AuthTypeExternalCommand
	// AuthTypeDeviceFlow Uses device flow to authenticate in a constrained environment with no access to browser
	AuthTypeDeviceFlow
)

type Config struct {
	Endpoint              config.URL      `json:"endpoint" pflag:",For admin types, specify where the uri of the service is located."`
	UseInsecureConnection bool            `json:"insecure" pflag:",Use insecure connection."`
	InsecureSkipVerify    bool            `json:"insecureSkipVerify" pflag:",InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name. Caution : shouldn't be use for production usecases'"`
	CACertFilePath        string          `json:"caCertFilePath" pflag:",Use specified certificate file to verify the admin server peer."`
	MaxBackoffDelay       config.Duration `json:"maxBackoffDelay" pflag:",Max delay between RPC retries"`
	PerRetryTimeout       config.Duration `json:"perRetryTimeout" pflag:",Per retry timeout"`
	MaxRetries            int             `json:"maxRetries" pflag:",Max number of RPC retries"`
	MaxMessageSizeBytes   int             `json:"maxMessageSizeBytes" pflag:",The max size in bytes for incoming messages"`
	AuthType              AuthType        `json:"authType" pflag:",Type of OAuth2 flow used for communicating with admin.ClientSecret,Pkce,ExternalCommand are valid values"`
	ClientID              string          `json:"clientId" pflag:",Client ID"`
	ClientSecret          string          `json:"clientSecret" pflag:",Client secret literal. Takes precedence over ClientSecretEnvVar and ClientSecretLocation. Prefer the env var or file variants outside of tests."`
	ClientSecretLocation  string          `json:"clientSecretLocation" pflag:",File containing the client secret"`
	ClientSecretEnvVar    string          `json:"clientSecretEnvVar" pflag:",Environment variable containing the client secret"`
	Scopes                []string        `json:"scopes" pflag:",List of scopes to request"`
	UseAudienceFromAdmin  bool            `json:"useAudienceFromAdmin" pflag:",Use Audience configured from admins public endpoint config."`
	Audience              string          `json:"audience" pflag:",Audience to use when initiating OAuth2 authorization requests."`

	// If not provided, it'll be discovered through admin's anonymously accessible metadata endpoint.
	TokenURL string `json:"tokenUrl" pflag:",OPTIONAL: Your IdP's token endpoint. It'll be discovered from flyte admin's OAuth Metadata endpoint if not provided."`

	// See the implementation of the 'grpcAuthorizationHeader' option in Flyte Admin for more information. But
	// basically we want to be able to use a different string to pass the token from this client to the the Admin service
	// because things might be running in a service mesh (like Envoy) that already uses the default 'authorization' header
	AuthorizationHeader string `json:"authorizationHeader" pflag:",Custom metadata header to pass JWT"`

	PkceConfig pkce.Config `json:"pkceConfig" pflag:",Config for Pkce authentication flow."`

	DeviceFlowConfig deviceflow.Config `json:"deviceFlowConfig" pflag:",Config for Device authentication flow."`

	Command []string `json:"command" pflag:",Command for external authentication token generation"`

	ProxyCommand []string `json:"proxyCommand" pflag:",Command for external proxy-authorization token generation"`

	// HTTPProxyURL allows operators to access external OAuth2 servers using an external HTTP Proxy
	HTTPProxyURL config.URL `json:"httpProxyURL" pflag:",OPTIONAL: HTTP Proxy to be used for OAuth requests."`

	DefaultOrg string `json:"defaultOrg" pflag:",OPTIONAL: Default org to use to support non-org based cli's.'."`
}

var (
	defaultConfig = Config{
		MaxBackoffDelay:      config.Duration{Duration: 8 * time.Second},
		PerRetryTimeout:      config.Duration{Duration: 15 * time.Second},
		MaxRetries:           4,
		ClientID:             DefaultClientID,
		AuthType:             AuthTypeClientSecret,
		ClientSecretLocation: DefaultClientSecretLocation,
		MaxMessageSizeBytes:  10 * 1024 * 1024, // 10MB
		PkceConfig: pkce.Config{
			TokenRefreshGracePeriod: config.Duration{Duration: 5 * time.Minute},
			BrowserSessionTimeout:   config.Duration{Duration: 10 * time.Minute},
		},
		DeviceFlowConfig: deviceflow.Config{
			TokenRefreshGracePeriod: config.Duration{Duration: 5 * time.Minute},
			Timeout:                 config.Duration{Duration: 10 * time.Minute},
			PollInterval:            config.Duration{Duration: 5 * time.Second},
		},
	}

	configSection = config.MustRegisterSectionWithUpdates(configSectionKey, &defaultConfig, func(ctx context.Context, newValue config.Config) {
		if newValue.(*Config).MaxRetries < 0 {
			logger.Panicf(ctx, "Admin configuration given with negative gRPC retry value.")
		}
	})
)

func GetConfig(ctx context.Context) *Config {
	if c, ok := configSection.GetConfig().(*Config); ok {
		return c
	}

	logger.Warnf(ctx, "Failed to retrieve config section [%v].", configSectionKey)
	return nil
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(cfg)
}

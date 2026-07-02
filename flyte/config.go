package flyte

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	stdconfig "github.com/flyteorg/flyte/flytestdlib/config"
	"gopkg.in/yaml.v3"

	client "github.com/unionai/flyte-sdk-go/flyte/client"
	"github.com/unionai/flyte-sdk-go/flyte/client/deviceflow"
	"github.com/unionai/flyte-sdk-go/flyte/client/pkce"
)

// AuthType values accepted by Config.AuthType. These mirror the Python SDK's
// auth_type parameter of flyte.init().
const (
	AuthTypePkce            = "Pkce"
	AuthTypeClientSecret    = "ClientSecret"
	AuthTypeDeviceFlow      = "DeviceFlow"
	AuthTypeExternalCommand = "ExternalCommand"
)

// EnvAPIKey is consulted by InitFromAPIKey when no key is passed explicitly.
// It matches the Python SDK's FLYTE_API_KEY environment variable.
const EnvAPIKey = "FLYTE_API_KEY"

// Config is the user-facing SDK configuration. It mirrors the parameters of the
// Python SDK's flyte.init() while staying a plain Go struct: fill in what you
// need and pass it to Init. Zero values mean "use the default / discover".
//
// Minimal example:
//
//	err := flyte.Init(ctx, flyte.Config{
//	    Endpoint: "acme.example.com",
//	    Project:  "my-project",
//	    Domain:   "development",
//	})
type Config struct {
	// Endpoint of the Flyte control plane. Accepts a bare host
	// ("acme.example.com"), an http(s) URL, or a gRPC target
	// ("dns:///acme.example.com"). TLS is used unless Insecure is set.
	Endpoint string

	// Org is the organization. When empty it is derived from the endpoint
	// hostname's first DNS label (e.g. "acme" for acme.example.com), matching
	// the Python SDK behavior. Set it explicitly for deployments where the
	// first label is not an org.
	Org string

	// Project and Domain are the defaults for task fetches and runs.
	Project string
	Domain  string

	// Insecure disables TLS entirely (local development).
	Insecure bool
	// InsecureSkipVerify skips server certificate verification.
	InsecureSkipVerify bool
	// CACertFilePath points at a CA bundle used to verify the server.
	CACertFilePath string

	// AuthType selects the authentication flow: AuthTypePkce (default),
	// AuthTypeClientSecret, AuthTypeDeviceFlow or AuthTypeExternalCommand.
	// It is inferred as ClientSecret when client credentials or an APIKey are set.
	AuthType string

	// APIKey is a platform API key: a base64 encoding of
	// "endpoint:clientId:clientSecret:org". When set it provides the endpoint,
	// org and client credentials, and forces the ClientSecret flow.
	APIKey string

	// ClientID / ClientSecret* configure the OAuth2 client-credentials flow.
	// The secret may be given as a literal, an env var name, or a file path;
	// they are used in that order of precedence.
	ClientID             string
	ClientSecret         string
	ClientSecretEnvVar   string
	ClientSecretLocation string
	// Scopes to request. Discovered from the server when empty.
	Scopes []string

	// DisableKeyring stops tokens from being persisted in the OS keyring.
	// Only interactive logins (PKCE, device flow) use the keyring — shared
	// with the Python SDK/CLI, so re-runs don't re-login. Headless flows
	// (api-key, client secret, external command) never touch it, so services
	// embedding the SDK don't need this unless they use an interactive flow.
	DisableKeyring bool

	// Command is an external command that prints a bearer token
	// (AuthTypeExternalCommand).
	Command []string
	// ProxyCommand generates proxy-authorization tokens for proxies in front
	// of the control plane.
	ProxyCommand []string

	// ClientConfig is an advanced escape hatch: when set it is used as the base
	// client configuration and the fields above are layered on top.
	ClientConfig *client.Config
}

// DecodeAPIKey decodes a platform API key into its parts:
// endpoint, clientID, clientSecret and org. The key is a base64 encoding of
// "endpoint:clientId:clientSecret:org" (endpoint may contain colons).
func DecodeAPIKey(encoded string) (endpoint, clientID, clientSecret, org string, err error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return "", "", "", "", fmt.Errorf("invalid api key: %w", err)
	}
	parts := strings.SplitN(string(raw), ":", 4)
	if len(parts) != 4 {
		return "", "", "", "", fmt.Errorf("invalid api key format: expected 4 ':'-separated parts, got %d", len(parts))
	}
	return parts[0], parts[1], parts[2], parts[3], nil
}

// sanitizeEndpoint normalizes an endpoint to a gRPC dns target ("dns:///host").
// Mirrors the Python SDK's sanitize_endpoint.
func sanitizeEndpoint(endpoint string) string {
	ep := strings.TrimSpace(strings.TrimSuffix(endpoint, "/"))
	for _, prefix := range []string{"https://", "http://"} {
		ep = strings.TrimPrefix(ep, prefix)
	}
	if !strings.HasPrefix(ep, "dns:///") {
		ep = "dns:///" + ep
	}
	return ep
}

// endpointHost extracts the hostname (no scheme, no port) from any accepted
// endpoint form.
func endpointHost(endpoint string) string {
	host := strings.TrimPrefix(sanitizeEndpoint(endpoint), "dns:///")
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	return host
}

// orgFromEndpoint derives the org from the endpoint hostname's first DNS label,
// mirroring the Python SDK: "acme.example.com" -> "acme". Hostnames
// with two or fewer labels yield "".
func orgFromEndpoint(endpoint string) string {
	host := endpointHost(endpoint)
	labels := strings.Split(host, ".")
	if len(labels) > 2 {
		return labels[0]
	}
	return ""
}

// resolve validates the config, applies API-key/auth defaults and produces the
// underlying client configuration. It returns a copy of the Config with all
// derived fields (Org, AuthType, credentials from APIKey) filled in.
func (c Config) resolve() (Config, *client.Config, error) {
	if c.APIKey != "" {
		endpoint, clientID, clientSecret, org, err := DecodeAPIKey(c.APIKey)
		if err != nil {
			return c, nil, err
		}
		if c.Endpoint == "" {
			c.Endpoint = endpoint
		}
		if c.Org == "" && org != "" && org != "None" {
			c.Org = org
		}
		c.ClientID = clientID
		c.ClientSecret = clientSecret
		c.AuthType = AuthTypeClientSecret
	}

	if c.Endpoint == "" {
		return c, nil, fmt.Errorf("endpoint is required (set Config.Endpoint or Config.APIKey)")
	}
	c.Endpoint = sanitizeEndpoint(c.Endpoint)

	if c.Org == "" {
		c.Org = orgFromEndpoint(c.Endpoint)
	}

	if c.AuthType == "" {
		// Client credentials present -> ClientSecret; explicit command -> ExternalCommand;
		// otherwise browser-based PKCE, matching the Python SDK defaults.
		switch {
		case c.ClientSecret != "" || c.ClientSecretEnvVar != "" || c.ClientSecretLocation != "":
			c.AuthType = AuthTypeClientSecret
		case len(c.Command) > 0:
			c.AuthType = AuthTypeExternalCommand
		default:
			c.AuthType = AuthTypePkce
		}
	}
	authType, err := client.AuthTypeString(c.AuthType)
	if err != nil {
		return c, nil, fmt.Errorf("invalid auth type %q (valid: Pkce, ClientSecret, DeviceFlow, ExternalCommand)", c.AuthType)
	}
	if c.AuthType == AuthTypeClientSecret && c.ClientID == "" {
		return c, nil, fmt.Errorf("ClientID is required for ClientSecret auth")
	}
	if c.AuthType == AuthTypeExternalCommand && len(c.Command) == 0 {
		return c, nil, fmt.Errorf("Command is required for ExternalCommand auth")
	}

	base := c.ClientConfig
	if base == nil {
		base = defaultClientConfig()
	}
	cc := *base // shallow copy so callers' ClientConfig is not mutated

	endpointURL, err := url.Parse(c.Endpoint)
	if err != nil {
		return c, nil, fmt.Errorf("invalid endpoint %q: %w", c.Endpoint, err)
	}
	cc.Endpoint = stdconfig.URL{URL: *endpointURL}
	cc.UseInsecureConnection = c.Insecure
	cc.InsecureSkipVerify = c.InsecureSkipVerify
	if c.CACertFilePath != "" {
		cc.CACertFilePath = c.CACertFilePath
	}
	cc.AuthType = authType
	if c.ClientID != "" {
		cc.ClientID = c.ClientID
	}
	cc.ClientSecret = c.ClientSecret
	if c.ClientSecretEnvVar != "" {
		cc.ClientSecretEnvVar = c.ClientSecretEnvVar
	} else if c.ClientSecret != "" {
		cc.ClientSecretEnvVar = ""
	}
	if c.ClientSecretLocation != "" {
		cc.ClientSecretLocation = c.ClientSecretLocation
	} else if c.ClientSecret != "" || c.ClientSecretEnvVar != "" {
		// Don't fall back to a stale base env var or the default /etc/secrets
		// location when the secret was provided some other way.
		cc.ClientSecretLocation = ""
	}
	if len(c.Scopes) > 0 {
		cc.Scopes = c.Scopes
	}
	if len(c.Command) > 0 {
		cc.Command = c.Command
	}
	if len(c.ProxyCommand) > 0 {
		cc.ProxyCommand = c.ProxyCommand
	}
	if c.Org != "" {
		cc.DefaultOrg = c.Org
	}

	return c, &cc, nil
}

// defaultClientConfig returns client defaults equivalent to the client
// package's registered defaults, without depending on global config sections.
func defaultClientConfig() *client.Config {
	return &client.Config{
		MaxBackoffDelay:     stdconfig.Duration{Duration: 8 * time.Second},
		PerRetryTimeout:     stdconfig.Duration{Duration: 15 * time.Second},
		MaxRetries:          4,
		AuthType:            client.AuthTypePkce,
		MaxMessageSizeBytes: 100 * 1024 * 1024,
		PkceConfig: pkce.Config{
			TokenRefreshGracePeriod: stdconfig.Duration{Duration: 5 * time.Minute},
			// Generous: users may need to type credentials and pass MFA.
			BrowserSessionTimeout: stdconfig.Duration{Duration: 10 * time.Minute},
		},
		DeviceFlowConfig: deviceflow.Config{
			TokenRefreshGracePeriod: stdconfig.Duration{Duration: 5 * time.Minute},
			Timeout:                 stdconfig.Duration{Duration: 10 * time.Minute},
			PollInterval:            stdconfig.Duration{Duration: 5 * time.Second},
		},
	}
}

// fileConfig is the on-disk flytectl/uctl-compatible YAML layout, e.g.:
//
//	admin:
//	  endpoint: dns:///acme.example.com
//	  authType: Pkce
//	  insecure: false
//	task:
//	  org: demo
//	  project: my-project
//	  domain: development
type fileConfig struct {
	Admin struct {
		Endpoint             string   `yaml:"endpoint"`
		Insecure             bool     `yaml:"insecure"`
		InsecureSkipVerify   bool     `yaml:"insecureSkipVerify"`
		CACertFilePath       string   `yaml:"caCertFilePath"`
		AuthType             string   `yaml:"authType"`
		ClientID             string   `yaml:"clientId"`
		ClientSecretLocation string   `yaml:"clientSecretLocation"`
		ClientSecretEnvVar   string   `yaml:"clientSecretEnvVar"`
		Command              []string `yaml:"command"`
		ProxyCommand         []string `yaml:"proxyCommand"`
		Scopes               []string `yaml:"scopes"`
	} `yaml:"admin"`
	Task struct {
		Org     string `yaml:"org"`
		Project string `yaml:"project"`
		Domain  string `yaml:"domain"`
	} `yaml:"task"`
}

// LoadConfigFile reads a flytectl/uctl-style YAML config file into a Config.
func LoadConfigFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file %s: %w", path, err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return Config{}, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}
	return Config{
		Endpoint:             fc.Admin.Endpoint,
		Insecure:             fc.Admin.Insecure,
		InsecureSkipVerify:   fc.Admin.InsecureSkipVerify,
		CACertFilePath:       fc.Admin.CACertFilePath,
		AuthType:             fc.Admin.AuthType,
		ClientID:             fc.Admin.ClientID,
		ClientSecretLocation: fc.Admin.ClientSecretLocation,
		ClientSecretEnvVar:   fc.Admin.ClientSecretEnvVar,
		Command:              fc.Admin.Command,
		ProxyCommand:         fc.Admin.ProxyCommand,
		Scopes:               fc.Admin.Scopes,
		Org:                  fc.Task.Org,
		Project:              fc.Task.Project,
		Domain:               fc.Task.Domain,
	}, nil
}

// FindConfigPath returns the first existing config file, using the same search
// order as the Python SDK:
//
//	./config.yaml, ./.flyte/config.yaml, <git root>/.flyte/config.yaml,
//	$UCTL_CONFIG, $FLYTECTL_CONFIG, ~/.union/config.yaml, ~/.flyte/config.yaml
//
// Returns "" when nothing is found.
func FindConfigPath() string {
	var candidates []string
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates,
			filepath.Join(cwd, "config.yaml"),
			filepath.Join(cwd, ".flyte", "config.yaml"),
		)
		if root := findGitRoot(cwd); root != "" && root != cwd {
			candidates = append(candidates, filepath.Join(root, ".flyte", "config.yaml"))
		}
	}
	for _, env := range []string{"UCTL_CONFIG", "FLYTECTL_CONFIG"} {
		if p := os.Getenv(env); p != "" {
			candidates = append(candidates, p)
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates,
			filepath.Join(home, ".union", "config.yaml"),
			filepath.Join(home, ".flyte", "config.yaml"),
		)
	}
	for _, p := range candidates {
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p
		}
	}
	return ""
}

func findGitRoot(dir string) string {
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

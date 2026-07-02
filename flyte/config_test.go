package flyte

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	client "github.com/unionai/flyte-sdk-go/flyte/client"
)

func encodeAPIKey(endpoint, clientID, secret, org string) string {
	return base64.StdEncoding.EncodeToString([]byte(endpoint + ":" + clientID + ":" + secret + ":" + org))
}

func TestDecodeAPIKey(t *testing.T) {
	// Real api-keys carry a plain host endpoint (no scheme): the format
	// is a left-split on ':' with the remainder folded into org, matching the
	// Python SDK's decode_api_key.
	key := encodeAPIKey("acme.example.com", "app-id", "s3cr3t", "acme")
	endpoint, clientID, secret, org, err := DecodeAPIKey(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint != "acme.example.com" || clientID != "app-id" || secret != "s3cr3t" || org != "acme" {
		t.Fatalf("unexpected parts: %q %q %q %q", endpoint, clientID, secret, org)
	}

	if _, _, _, _, err := DecodeAPIKey("not-base64!!"); err == nil {
		t.Fatal("expected error for invalid base64")
	}
	if _, _, _, _, err := DecodeAPIKey(base64.StdEncoding.EncodeToString([]byte("too:few"))); err == nil {
		t.Fatal("expected error for wrong part count")
	}
}

func TestConfigResolveAPIKey(t *testing.T) {
	cfg := Config{
		APIKey:  encodeAPIKey("acme.example.com", "app-id", "s3cr3t", "acme"),
		Project: "my-project",
		Domain:  "development",
	}
	resolved, clientCfg, err := cfg.resolve()
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if resolved.Endpoint != "dns:///acme.example.com" {
		t.Errorf("endpoint = %q", resolved.Endpoint)
	}
	if resolved.Org != "acme" {
		t.Errorf("org = %q", resolved.Org)
	}
	if resolved.AuthType != AuthTypeClientSecret {
		t.Errorf("auth type = %q", resolved.AuthType)
	}
	if clientCfg.AuthType != client.AuthTypeClientSecret {
		t.Errorf("client auth type = %v", clientCfg.AuthType)
	}
	if clientCfg.ClientID != "app-id" || clientCfg.ClientSecret != "s3cr3t" {
		t.Errorf("client credentials not threaded through: %q %q", clientCfg.ClientID, clientCfg.ClientSecret)
	}
	if clientCfg.ClientSecretLocation != "" {
		t.Errorf("default secret location should be cleared, got %q", clientCfg.ClientSecretLocation)
	}
	if clientCfg.DefaultOrg != "acme" {
		t.Errorf("default org = %q", clientCfg.DefaultOrg)
	}
}

func TestConfigResolveDefaults(t *testing.T) {
	resolved, clientCfg, err := Config{Endpoint: "https://acme.example.com/"}.resolve()
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if resolved.Endpoint != "dns:///acme.example.com" {
		t.Errorf("endpoint = %q", resolved.Endpoint)
	}
	if resolved.Org != "acme" {
		t.Errorf("org should be derived from endpoint, got %q", resolved.Org)
	}
	if resolved.AuthType != AuthTypePkce {
		t.Errorf("default auth type = %q", resolved.AuthType)
	}
	if clientCfg.PkceConfig.BrowserSessionTimeout.Duration == 0 {
		t.Error("pkce defaults not applied")
	}
}

func TestConfigResolveErrors(t *testing.T) {
	if _, _, err := (Config{}).resolve(); err == nil {
		t.Error("expected error for missing endpoint")
	}
	if _, _, err := (Config{Endpoint: "x.y.z", AuthType: AuthTypeClientSecret}).resolve(); err == nil {
		t.Error("expected error for ClientSecret without ClientID")
	}
	if _, _, err := (Config{Endpoint: "x.y.z", AuthType: AuthTypeExternalCommand}).resolve(); err == nil {
		t.Error("expected error for ExternalCommand without Command")
	}
	if _, _, err := (Config{Endpoint: "x.y.z", AuthType: "Bogus"}).resolve(); err == nil {
		t.Error("expected error for unknown auth type")
	}
}

func TestOrgFromEndpoint(t *testing.T) {
	cases := map[string]string{
		"acme.example.com":         "acme",
		"dns:///acme.example.com":  "acme",
		"https://acme.example.com": "acme",
		"localhost:8089":           "",
		"example.com":              "",
	}
	for endpoint, want := range cases {
		if got := orgFromEndpoint(endpoint); got != want {
			t.Errorf("orgFromEndpoint(%q) = %q, want %q", endpoint, got, want)
		}
	}
}

func TestLoadConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
admin:
  endpoint: dns:///acme.example.com
  authType: DeviceFlow
  insecure: false
task:
  org: acme
  project: my-project
  domain: development
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatalf("LoadConfigFile failed: %v", err)
	}
	if cfg.Endpoint != "dns:///acme.example.com" || cfg.AuthType != "DeviceFlow" ||
		cfg.Org != "acme" || cfg.Project != "my-project" || cfg.Domain != "development" {
		t.Errorf("unexpected config: %+v", cfg)
	}
}

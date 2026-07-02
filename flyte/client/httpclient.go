package admin

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// baseURL derives the Connect base URL (e.g. "https://host") from the
// configured endpoint, which may be a bare host, an http(s) URL, or a legacy
// gRPC dns:/// target.
func baseURL(cfg *Config) string {
	host := cfg.Endpoint.String()
	host = strings.TrimSuffix(host, "/")
	for _, prefix := range []string{"dns:///", "https://", "http://"} {
		host = strings.TrimPrefix(host, prefix)
	}
	scheme := "https"
	if cfg.UseInsecureConnection {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s", scheme, host)
}

// newHTTPClient builds the http.Client all Connect RPCs share, honoring the
// TLS settings in the config.
func newHTTPClient(cfg *Config) (*http.Client, error) {
	tlsConfig := &tls.Config{} //nolint:gosec
	if cfg.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true //nolint:gosec
	}
	if cfg.CACertFilePath != "" {
		var caCerts *x509.CertPool
		caCerts, err := readCACerts(cfg.CACertFilePath)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = caCerts
	}

	transport := &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		TLSClientConfig:   tlsConfig,
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   90 * time.Second,
	}
	return &http.Client{Transport: transport}, nil
}

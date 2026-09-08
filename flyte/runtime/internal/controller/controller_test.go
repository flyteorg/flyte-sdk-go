package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEndpointURL(t *testing.T) {
	cases := []struct {
		raw      string
		insecure bool
		want     string
	}{
		// Explicit schemes pass through.
		{"http://anything.example.com", false, "http://anything.example.com"},
		{"https://anything.example.com", true, "https://anything.example.com"},
		// Local hosts default to plaintext.
		{"host.docker.internal:8090", false, "http://host.docker.internal:8090"},
		{"localhost:8090", false, "http://localhost:8090"},
		{"api.localhost:8090", false, "http://api.localhost:8090"},
		{"127.0.0.1:8090", false, "http://127.0.0.1:8090"},
		{"[::1]:8090", false, "http://[::1]:8090"},
		{"127.0.0.1", false, "http://127.0.0.1"},
		// Anything else is TLS unless _U_INSECURE says otherwise — including
		// hosts that merely contain "docker" or "localhost".
		{"docker-gw.corp.example.com:8090", false, "https://docker-gw.corp.example.com:8090"},
		{"notlocalhost.example.com", false, "https://notlocalhost.example.com"},
		{"dns.flyte.example.com", false, "https://dns.flyte.example.com"},
		{"docker-gw.corp.example.com:8090", true, "http://docker-gw.corp.example.com:8090"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, endpointURL(c.raw, c.insecure), "%s insecure=%v", c.raw, c.insecure)
	}
}

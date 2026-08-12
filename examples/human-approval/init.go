package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/unionai/flyte-sdk-go/flyte"
)

// mustInit initializes the SDK from environment variables:
//
//	FLYTE_ENDPOINT      control plane endpoint, e.g. my-org.example.com
//	FLYTE_PROJECT       project the runs are created in
//	FLYTE_DOMAIN        domain (default development)
//	FLYTE_API_KEY       API key -> headless ClientSecret auth
//	FLYTE_AUTH_COMMAND  external command that prints a bearer token
//
// Without FLYTE_API_KEY or FLYTE_AUTH_COMMAND the browser PKCE flow is used.
func mustInit(ctx context.Context) {
	cfg := flyte.Config{
		Endpoint: os.Getenv("FLYTE_ENDPOINT"),
		Project:  os.Getenv("FLYTE_PROJECT"),
		Domain:   getEnv("FLYTE_DOMAIN", "development"),
		APIKey:   os.Getenv("FLYTE_API_KEY"),
	}
	if (cfg.Endpoint == "" && cfg.APIKey == "") || cfg.Project == "" {
		log.Fatal("set FLYTE_ENDPOINT (or FLYTE_API_KEY) and FLYTE_PROJECT")
	}
	if cmd := os.Getenv("FLYTE_AUTH_COMMAND"); cmd != "" {
		cfg.AuthType = flyte.AuthTypeExternalCommand
		cfg.Command = strings.Fields(cmd)
	}
	if err := flyte.Init(ctx, cfg); err != nil {
		log.Fatalf("failed to initialize: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

package flyte

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TokenSource provides OAuth tokens for authentication
type TokenSource interface {
	// Token returns a valid OAuth token
	Token() (string, error)
}

// clientSecretTokenSource implements TokenSource using OAuth ClientSecret flow
type clientSecretTokenSource struct {
	config *clientcredentials.Config
	mu     sync.Mutex
	token  *oauth2.Token
}

// newClientSecretTokenSource creates a new OAuth ClientSecret token source
func newClientSecretTokenSource(clientID, clientSecret, tokenURL string, scopes []string) TokenSource {
	config := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
		Scopes:       scopes,
	}

	return &clientSecretTokenSource{
		config: config,
	}
}

// Token obtains a valid OAuth token, reusing cached token if still valid
func (t *clientSecretTokenSource) Token() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if we have a cached token that's still valid
	if t.token != nil && t.token.Valid() {
		return t.token.AccessToken, nil
	}

	// Fetch a new token
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token, err := t.config.Token(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to obtain OAuth token: %w", err)
	}

	t.token = token
	return token.AccessToken, nil
}

// AuthInterceptor provides gRPC interceptors for injecting authentication
type AuthInterceptor struct {
	tokenSource TokenSource
}

// newAuthInterceptor creates a new auth interceptor
func newAuthInterceptor(config *Config) (*AuthInterceptor, error) {
	// If no OAuth credentials provided, return nil (no auth)
	if config.ClientID == "" && config.ClientSecret == "" {
		return nil, nil
	}

	// Default scopes if none provided
	scopes := config.Scopes
	if len(scopes) == 0 {
		scopes = []string{"all"}
	}

	tokenSource := newClientSecretTokenSource(
		config.ClientID,
		config.ClientSecret,
		config.TokenURL,
		scopes,
	)

	return &AuthInterceptor{
		tokenSource: tokenSource,
	}, nil
}

// Unary returns a unary client interceptor for authentication
func (a *AuthInterceptor) Unary() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		// Get token
		token, err := a.tokenSource.Token()
		if err != nil {
			return fmt.Errorf("failed to get auth token: %w", err)
		}

		// Inject authorization header
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

		// Invoke the RPC
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// Stream returns a stream client interceptor for authentication
func (a *AuthInterceptor) Stream() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		// Get token
		token, err := a.tokenSource.Token()
		if err != nil {
			return nil, fmt.Errorf("failed to get auth token: %w", err)
		}

		// Inject authorization header
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

		// Create the stream
		return streamer(ctx, desc, cc, method, opts...)
	}
}

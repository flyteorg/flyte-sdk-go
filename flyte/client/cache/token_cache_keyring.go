package cache

import (
	"fmt"
	"sync"

	"github.com/zalando/go-keyring"
	"golang.org/x/oauth2"
)

const (
	keyringKeyAccessToken  = "access_token"
	keyringKeyRefreshToken = "refresh_token"
)

// TokenCacheKeyringProvider persists tokens in the OS keyring so logins
// survive process restarts. It uses the same layout as the Python flyte SDK
// (service = endpoint host, keys "access_token"/"refresh_token"), so Go and
// Python tools reuse each other's cached logins for the same endpoint.
//
// Token expiry is not stored: consumers derive it from the JWT's exp claim
// (see utils.Valid), matching the Python SDK.
type TokenCacheKeyringProvider struct {
	serviceName string

	mu         sync.Mutex
	condLocker NoopLocker
	cond       *sync.Cond
}

// NewTokenCacheKeyringProvider creates a keyring-backed token cache scoped to
// the given endpoint host (e.g. "acme.example.com").
func NewTokenCacheKeyringProvider(endpointHost string) *TokenCacheKeyringProvider {
	p := &TokenCacheKeyringProvider{serviceName: endpointHost}
	p.cond = sync.NewCond(&p.condLocker)
	return p
}

func (t *TokenCacheKeyringProvider) SaveToken(token *oauth2.Token) error {
	if token == nil || token.AccessToken == "" {
		return fmt.Errorf("cannot save empty token")
	}
	if err := keyring.Set(t.serviceName, keyringKeyAccessToken, token.AccessToken); err != nil {
		return fmt.Errorf("unable to save access token to keyring: %w", err)
	}
	if token.RefreshToken != "" {
		if err := keyring.Set(t.serviceName, keyringKeyRefreshToken, token.RefreshToken); err != nil {
			return fmt.Errorf("unable to save refresh token to keyring: %w", err)
		}
	}
	return nil
}

func (t *TokenCacheKeyringProvider) GetToken() (*oauth2.Token, error) {
	accessToken, err := keyring.Get(t.serviceName, keyringKeyAccessToken)
	if err != nil || accessToken == "" {
		return nil, ErrNotFound
	}
	token := &oauth2.Token{AccessToken: accessToken, TokenType: "bearer"}
	// A missing refresh token is fine; the caller falls back to a fresh login.
	if refreshToken, err := keyring.Get(t.serviceName, keyringKeyRefreshToken); err == nil {
		token.RefreshToken = refreshToken
	}
	return token, nil
}

func (t *TokenCacheKeyringProvider) PurgeIfEquals(existing *oauth2.Token) (bool, error) {
	if existing == nil {
		return false, nil
	}
	current, err := keyring.Get(t.serviceName, keyringKeyAccessToken)
	if err != nil {
		return false, ErrNotFound
	}
	if current != existing.AccessToken {
		return false, nil
	}
	_ = keyring.Delete(t.serviceName, keyringKeyAccessToken)
	_ = keyring.Delete(t.serviceName, keyringKeyRefreshToken)
	return true, nil
}

func (t *TokenCacheKeyringProvider) Lock() {
	t.mu.Lock()
}

func (t *TokenCacheKeyringProvider) TryLock() bool {
	return t.mu.TryLock()
}

func (t *TokenCacheKeyringProvider) Unlock() {
	t.mu.Unlock()
}

// CondWait waits on the condition variable; used by the auth interceptor to
// coordinate token refreshes across goroutines. See the in-memory provider for
// why the no-op locker is safe here.
func (t *TokenCacheKeyringProvider) CondWait() {
	t.condLocker.Lock()
	t.cond.Wait()
	t.condLocker.Unlock()
}

func (t *TokenCacheKeyringProvider) CondBroadcast() {
	t.cond.Broadcast()
}

var _ TokenCache = (*TokenCacheKeyringProvider)(nil)

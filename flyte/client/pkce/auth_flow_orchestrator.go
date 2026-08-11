package pkce

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/browser"
	"golang.org/x/oauth2"

	"github.com/unionai/flyte-sdk-go/flyte/client/tokenorchestrator"

	"github.com/flyteorg/flyte/v2/flytestdlib/logger"
)

const (
	stateKey               = "state"
	nonceKey               = "nonce"
	codeChallengeKey       = "code_challenge"
	codeChallengeMethodKey = "code_challenge_method"
	codeChallengeMethodVal = "S256"
)

// TokenOrchestrator implements the main logic to initiate Pkce flow to issue access token and refresh token as well as
// refreshing the access token if a refresh token is present.
type TokenOrchestrator struct {
	tokenorchestrator.BaseTokenOrchestrator
	Config Config
}

// FetchTokenFromAuthFlow starts a webserver to listen to redirect callback from the authorization server at the end
// of the flow. It then launches the browser to authenticate the user.
func (f TokenOrchestrator) FetchTokenFromAuthFlow(ctx context.Context) (*oauth2.Token, error) {
	var err error
	var redirectURL *url.URL
	if redirectURL, err = url.Parse(f.ClientConfig.RedirectURL); err != nil {
		return nil, err
	}

	// pkceCodeVerifier stores the generated random value which the client will on-send to the auth server with the received
	// authorization code. This way the oauth server can verify that the base64URLEncoded(sha265(codeVerifier)) matches
	// the stored code challenge, which was initially sent through with the code+PKCE authorization request to ensure
	// that this is the original user-agent who requested the access token.
	pkceCodeVerifier := generateCodeVerifier(64)

	// pkceCodeChallenge stores the base64(sha256(codeVerifier)) which is sent from the
	// client to the auth server as required for PKCE.
	pkceCodeChallenge := generateCodeChallenge(pkceCodeVerifier)

	stateString := state(32)
	nonces := state(32)

	tokenChannel := make(chan *oauth2.Token, 1)
	errorChannel := make(chan error, 1)

	values := url.Values{}
	values.Add(stateKey, stateString)
	values.Add(nonceKey, nonces)
	values.Add(codeChallengeKey, pkceCodeChallenge)
	values.Add(codeChallengeMethodKey, codeChallengeMethodVal)
	urlToOpen := fmt.Sprintf("%s&%s", f.ClientConfig.AuthCodeURL(""), values.Encode())

	serveMux := http.NewServeMux()
	server := &http.Server{Addr: redirectURL.Host, Handler: serveMux, ReadHeaderTimeout: 0}
	// Register the call back handler

	// Pass along http client used in oauth2
	httpClient, ok := ctx.Value(oauth2.HTTPClient).(*http.Client)
	if !ok {
		logger.Debugf(ctx, "using default http client instead")
		httpClient = http.DefaultClient
	}

	serveMux.HandleFunc(redirectURL.Path, getAuthServerCallbackHandler(f.ClientConfig, pkceCodeVerifier,
		tokenChannel, errorChannel, stateString, httpClient)) // the oauth2 callback endpoint
	// Shut down gracefully so an in-flight response (the success/error page the
	// browser is loading) finishes before the listener goes away.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go func() {
		if serveErr := server.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			errorChannel <- fmt.Errorf("couldn't start the callback http server on %s (is another login in progress?): %w",
				redirectURL.Host, serveErr)
		}
	}()

	logger.Infof(ctx, "Opening the browser at %s", urlToOpen)
	if err = browser.OpenURL(urlToOpen); err != nil {
		return nil, err
	}

	ctx, cancelNow := context.WithTimeout(ctx, f.Config.BrowserSessionTimeout.Duration)
	defer cancelNow()

	select {
	case err = <-errorChannel:
		return nil, err
	case <-ctx.Done():
		return nil, fmt.Errorf("browser login did not complete within %v (increase PkceConfig.BrowserSessionTimeout if you need more time)",
			f.Config.BrowserSessionTimeout.Duration)
	case token := <-tokenChannel:
		if err = f.TokenCache.SaveToken(token); err != nil {
			logger.Errorf(ctx, "unable to save the new token. Will ignore the error and use the issued token. Error: %v", err)
		}
		return token, nil
	}
}

// NewTokenOrchestrator creates a new TokenOrchestrator that implements the main logic to initiate Pkce flow to issue
// access token and refresh token as well as refreshing the access token if a refresh token is present.
func NewTokenOrchestrator(baseOrchestrator tokenorchestrator.BaseTokenOrchestrator, cfg Config) (TokenOrchestrator, error) {
	return TokenOrchestrator{
		BaseTokenOrchestrator: baseOrchestrator,
		Config:                cfg,
	}, nil
}

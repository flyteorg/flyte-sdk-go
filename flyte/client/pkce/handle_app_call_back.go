package pkce

import (
	"context"
	// This import is used to embed the callback.html
	_ "embed"
	"fmt"
	"net/http"
	"text/template"

	"golang.org/x/oauth2"

	"github.com/unionai/flyte-sdk-go/flyte/client/oauth"
)

//go:embed callback.html
var callbackHTML string

var callbackTemplate = template.Must(template.New("callback").Parse(callbackHTML))

type callbackData struct {
	Error            string
	ErrorHint        string
	ErrorDescription string
	NoCode           bool
	WrongState       bool
	AccessTokenError string
}

func getAuthServerCallbackHandler(c *oauth.Config, codeVerifier string, tokenChannel chan *oauth2.Token,
	errorChannel chan error, stateString string, client *http.Client) func(rw http.ResponseWriter, req *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		data := callbackData{
			Error:            req.URL.Query().Get("error"),
			ErrorHint:        req.URL.Query().Get("error_hint"),
			ErrorDescription: req.URL.Query().Get("error_description"),
			NoCode:           req.URL.Query().Get("code") == "",
			WrongState:       req.URL.Query().Get("state") != stateString,
		}

		// Determine the outcome first, then render the page and flush it to the
		// browser BEFORE signaling the orchestrator: as soon as the orchestrator
		// receives the result it shuts the callback server down, so signaling
		// before the response is written races the shutdown and leaves the user
		// staring at a connection error after a successful login.
		var token *oauth2.Token
		var outcome error
		switch {
		case data.Error != "":
			outcome = fmt.Errorf("error on callback during authorization due to %v", data.Error)
		case data.NoCode:
			outcome = fmt.Errorf("could not find the authorize code")
		case data.WrongState:
			outcome = fmt.Errorf("possibly a csrf attack")
		default:
			// Send the code_verifier along when exchanging the code for a token (PKCE).
			opts := []oauth2.AuthCodeOption{oauth2.SetAuthURLParam("code_verifier", codeVerifier)}
			ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
			var err error
			token, err = c.Exchange(ctx, req.URL.Query().Get("code"), opts...)
			if err != nil {
				data.AccessTokenError = err.Error()
				outcome = fmt.Errorf("error while exchanging auth code due to %v", err)
			}
		}

		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = callbackTemplate.Execute(rw, data)
		if flusher, ok := rw.(http.Flusher); ok {
			flusher.Flush()
		}

		if outcome != nil {
			errorChannel <- outcome
			return
		}
		tokenChannel <- token
	}
}

package mcp

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

const (
	// Path is where the JSON-RPC endpoint answers.
	Path = "/mcp"
	// HealthPath is the kubelet's probe. It is the one unauthenticated path.
	HealthPath = "/healthz"

	headerAuthorization = "Authorization"
	headerContentType   = "Content-Type"
	headerOrigin        = "Origin"
	bearerPrefix        = "Bearer "
	contentTypeJSON     = "application/json"

	// maxBodyBytes bounds one JSON-RPC frame.
	maxBodyBytes = 4 << 20

	msgUnauthorized = "unauthorized"
	msgNoBrowsers   = "browser requests are refused; MCP clients send no Origin"
	msgBodyTooLarge = "request body too large or unreadable"
	msgNoToken      = "the token file %s is empty"
)

// errEmptyToken is a token file that holds nothing, which authenticates no one.
var errEmptyToken = errors.New("empty token")

// ReadToken reads the bearer token callers must present. A file that is
// missing or empty is an error: a server with no token authenticates no one,
// and must say so rather than fall back to anything.
func ReadToken(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf(msgNoToken+": %w", path, errEmptyToken)
	}
	return token, nil
}

// Handler serves the MCP endpoint and the health probe. The token is read
// from tokenFile on every request, so a rotated Secret takes effect without a
// restart, and a file that becomes unreadable or empty refuses every request.
func Handler(s *Server, tokenFile string, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+HealthPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerContentType, contentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "name": ServerName, "version": ServerVersion})
	})
	mux.HandleFunc("POST "+Path, func(w http.ResponseWriter, r *http.Request) {
		// MCP clients are programs, not pages. A browser always sends Origin,
		// so refusing it closes the DNS-rebinding door a port-forward to
		// localhost would otherwise leave open.
		if r.Header.Get(headerOrigin) != "" {
			http.Error(w, msgNoBrowsers, http.StatusForbidden)
			return
		}
		if !authorized(r, tokenFile, log) {
			http.Error(w, msgUnauthorized, http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, msgBodyTooLarge, http.StatusRequestEntityTooLarge)
			return
		}
		resp := s.HandleJSONRPC(r.Context(), body)
		if len(resp) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set(headerContentType, contentTypeJSON)
		_, _ = w.Write(resp)
	})
	return mux
}

// authorized reports whether r carries the server's token.
func authorized(r *http.Request, tokenFile string, log *slog.Logger) bool {
	want, err := ReadToken(tokenFile)
	if err != nil {
		log.Error("mcp token unreadable; refusing every request", "file", tokenFile, "err", err)
		return false
	}
	got, ok := strings.CutPrefix(r.Header.Get(headerAuthorization), bearerPrefix)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), []byte(want)) == 1
}

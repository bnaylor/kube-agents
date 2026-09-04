package authcallout

import (
	"encoding/json"
	"net/http"
)

// The runtime status surface.
//
// The deployment spec asks that the callout "logs the map version it is serving
// and exposes it at runtime, so 'the map says X' is checkable against the
// running system rather than against the rendered object". This is the second
// half of that. It carries no secret — the version, the users being served, and
// whether the last update was refused — and it is what the readiness probe
// reads, which is what makes "all replicas ready" mean "all replicas serving
// the rendered map" and gives BusCredentialsReady something true to be set from.

const (
	// StatusPath answers with the served version and identities. Readable by
	// a human with kubectl port-forward during an incident.
	StatusPath = "/status"

	// ReadyPath is the readiness probe. It answers 503 until a map is being
	// served, so a callout that cannot read its map is taken out of the
	// Service rather than left to refuse every connection it is handed.
	ReadyPath = "/readyz"

	// LivePath is the liveness probe. It answers as long as the process is
	// up, and deliberately does NOT consult the map: a callout with a stale
	// or missing map is a callout to stop sending traffic to, not one to
	// restart. Restarting it loses the map it did have and cannot make the
	// API server answer any faster.
	LivePath = "/livez"
)

// Status is the JSON the status endpoint answers with.
type Status struct {
	// Version is the identity map version being served, empty if none is.
	// The operator compares this against what it rendered.
	Version string `json:"version"`

	// Serving is true when a map is loaded.
	Serving bool `json:"serving"`

	// Users are the NATS user names the map covers, sorted. Named rather
	// than counted because the question during an incident is "is the
	// callout serving the identity this workload needs", and a count cannot
	// answer it.
	Users []string `json:"users"`

	// LastError is why the most recent update was refused, if it was. A
	// callout serving version N with a LastError set is one whose newer
	// render did not land, which is the state that would otherwise look
	// identical to nothing having been rendered.
	LastError string `json:"lastError,omitempty"`
}

// StatusOf snapshots the store.
func StatusOf(s *Store) Status {
	st := Status{
		Version:   s.Version(),
		Serving:   s.Ready(),
		LastError: s.LastError(),
		Users:     []string{},
	}
	if m := s.Current(); m != nil {
		st.Users = m.Users()
	}
	return st
}

// StatusHandler serves the status, readiness and liveness paths for a store.
func StatusHandler(s *Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(StatusPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		_ = enc.Encode(StatusOf(s))
	})

	mux.HandleFunc(ReadyPath, func(w http.ResponseWriter, r *http.Request) {
		if !s.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			if e := s.LastError(); e != "" {
				_, _ = w.Write([]byte("no identity map: " + e + "\n"))
				return
			}
			_, _ = w.Write([]byte("no identity map\n"))
			return
		}
		// The version in the readiness body, so `kubectl describe pod` on a
		// probe failure and a curl on a success answer the same question.
		_, _ = w.Write([]byte("serving " + s.Version() + "\n"))
	})

	mux.HandleFunc(LivePath, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	return mux
}

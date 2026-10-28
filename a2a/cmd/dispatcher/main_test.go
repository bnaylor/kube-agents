package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/dispatch"
	"github.com/gke-labs/kube-agents/a2a/lib"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestReadSettingsNeedsTheBusTheNamespaceAndTheConfigMap(t *testing.T) {
	full := map[string]string{"NATS_URL": "nats://x", envNamespace: "ka", envConfigMap: "agent-a2a-dispatch",
		"NATS_USER": "dispatcher", "NATS_PASSWORD": "pw"}
	if _, err := readSettings(env(full)); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"NATS_URL", envNamespace, envConfigMap} {
		m := map[string]string{}
		for kk, v := range full {
			m[kk] = v
		}
		delete(m, k)
		if _, err := readSettings(env(m)); err == nil {
			t.Errorf("accepted without %s", k)
		}
	}
	// The deployment's path: a projected token that must exist.
	noUser := map[string]string{"NATS_URL": "nats://x", envNamespace: "ka", envConfigMap: "m",
		lib.EnvBusTokenFile: filepath.Join(t.TempDir(), "missing")}
	if _, err := readSettings(env(noUser)); err == nil {
		t.Error("accepted a missing bus token")
	}
	tok := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("t"), 0o600); err != nil {
		t.Fatal(err)
	}
	noUser[lib.EnvBusTokenFile] = tok
	if _, err := readSettings(env(noUser)); err != nil {
		t.Errorf("with a token: %v", err)
	}
}

func TestTheVerifierFailsClosedUntilProfileRunLands(t *testing.T) {
	err := unavailableVerifier{}.CheckProfileRun(context.Background(), capability.Ref{Key: "k", Revision: 1}, "tse", "read", "namespace/x")
	var r *capability.Refusal
	if !errors.As(err, &r) {
		t.Fatalf("got %v, want a refusal", err)
	}
}

func TestAMovedProfileNamespaceEndsTheProcess(t *testing.T) {
	src := dispatch.NewStaticSource(lib.DispatchConfig{ProfileNamespace: "a"})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	go watchProfileNamespace(ctx, cancel, src, "a")
	src.Set(lib.DispatchConfig{ProfileNamespace: "b"})
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), errProfileNamespaceMoved) {
			t.Errorf("cause %v", context.Cause(ctx))
		}
	case <-time.After(namespaceCheckInterval + 5*time.Second):
		t.Fatal("a moved profile namespace did not end the process")
	}
}

// The environment and the probe port are the ones the operator's
// Deployment renders (buildA2ADispatcherDeployment): a renamed variable
// here is a pod that exits at start.
func TestTheSettingsAreTheDeploymentsNames(t *testing.T) {
	if envNamespace != "POD_NAMESPACE" || envConfigMap != "DISPATCH_CONFIGMAP" {
		t.Errorf("env names %q %q", envNamespace, envConfigMap)
	}
	if lib.EnvNATSCAFile != "NATS_CA_FILE" {
		t.Errorf("CA env %q", lib.EnvNATSCAFile)
	}
	if lib.BusTokenPath != "/var/run/secrets/a2a-bus/token" {
		t.Errorf("bus token path %q", lib.BusTokenPath)
	}
	if statusAddr != ":8080" {
		t.Errorf("status address %q", statusAddr)
	}
	if natsUser != "dispatcher" {
		t.Errorf("bus user %q", natsUser)
	}
}

func probe(t *testing.T, p *probes, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

// /readyz needs the config, the bus and a dispatcher with every durable
// bound, and says which is missing; /healthz answers throughout.
func TestReadyzNeedsConfigBusAndBoundDurables(t *testing.T) {
	var loaded, connected atomic.Bool
	bound, why := false, "durables not bound yet: profile-tse-read-top"
	var mu sync.Mutex
	p := &probes{configLoaded: loaded.Load}
	check := func(wantCode int, wantBody string) {
		t.Helper()
		code, body := probe(t, p, "/readyz")
		if code != wantCode || !strings.Contains(body, wantBody) {
			t.Errorf("/readyz = %d %q, want %d containing %q", code, body, wantCode, wantBody)
		}
		if code, _ := probe(t, p, "/healthz"); code != http.StatusOK {
			t.Errorf("/healthz = %d", code)
		}
	}
	check(http.StatusServiceUnavailable, "config")
	loaded.Store(true)
	check(http.StatusServiceUnavailable, "bus")
	p.setBus(connected.Load)
	check(http.StatusServiceUnavailable, "bus")
	connected.Store(true)
	check(http.StatusServiceUnavailable, "not running")
	p.setDispatcher(func() (bool, string) { mu.Lock(); defer mu.Unlock(); return bound, why })
	check(http.StatusServiceUnavailable, "profile-tse-read-top")
	mu.Lock()
	bound, why = true, ""
	mu.Unlock()
	check(http.StatusOK, "ok")
	connected.Store(false)
	check(http.StatusServiceUnavailable, "bus")
}

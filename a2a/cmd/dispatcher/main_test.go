package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/dispatch"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

// The process refuses to start on intervals that would fail live work.
func TestParseConfigEnforcesTheIntervalBounds(t *testing.T) {
	cfg, path, err := parseConfig(nil, env(nil))
	if err != nil || cfg.Lease != dispatch.DefaultLease || path != defaultProfilesFile {
		t.Fatalf("defaults: %+v %q %v", cfg, path, err)
	}
	if _, _, err := parseConfig([]string{"-restart-grace=30s"}, env(nil)); err == nil || !strings.Contains(err.Error(), "restart grace") {
		t.Errorf("grace 30s under two renewals of 20s: %v", err)
	}
	if _, _, err := parseConfig(nil, env(map[string]string{dispatch.EnvLeaseSeconds: "40"})); err == nil {
		t.Error("a lease of two renewals was accepted")
	}
	cfg, _, err = parseConfig([]string{"-renew=10s", "-restart-grace=20s", "-profiles=/x.json"}, env(nil))
	if err != nil || cfg.Renew != 10*time.Second {
		t.Errorf("flags: %+v %v", cfg, err)
	}
}

func TestTheVerifierFailsClosedUntilProfileRunLands(t *testing.T) {
	err := unavailableVerifier{}.CheckProfileRun(context.Background(), capability.Ref{Key: "k", Revision: 1}, "tse", "read", "namespace/x")
	var r *capability.Refusal
	if !errors.As(err, &r) {
		t.Fatalf("got %v, want a refusal", err)
	}
}

func TestTheProfileFileIsReadAndReloaded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.json")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"namespace":"ka","ceiling":6,"profiles":[{"name":"tse","concurrency":2,"activeDeadlineSeconds":600,
		"parameters":[{"name":"project","required":true}],"pods":{"read":["tse-read-0"]}}]}`)
	src, err := newFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := src.Snapshot().Profile("tse"); !ok || p.Pods["read"][0] != "tse-read-0" || !p.Parameters[0].Required {
		t.Fatalf("snapshot %+v", src.Snapshot())
	}
	time.Sleep(10 * time.Millisecond)
	write(`{"namespace":"ka","ceiling":9,"profiles":[]}`)
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if err := src.reload(); err != nil || src.Snapshot().Ceiling != 9 {
		t.Fatalf("reload: %v, ceiling %d", err, src.Snapshot().Ceiling)
	}
	write(`{"ceiling":`)
	past := future.Add(time.Hour)
	_ = os.Chtimes(path, past, past)
	if err := src.reload(); err == nil || src.Snapshot().Ceiling != 9 {
		t.Errorf("a broken file replaced the last good snapshot (%v)", err)
	}
	for name, body := range map[string]string{
		"unknown field":    `{"profiles":[],"cieling":3}`,
		"zero concurrency": `{"profiles":[{"name":"a","concurrency":0,"activeDeadlineSeconds":5}]}`,
		"no deadline":      `{"profiles":[{"name":"a","concurrency":1}]}`,
		"dotted name":      `{"profiles":[{"name":"a.b","concurrency":1,"activeDeadlineSeconds":5}]}`,
	} {
		write(body)
		if _, err := newFileSource(path); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

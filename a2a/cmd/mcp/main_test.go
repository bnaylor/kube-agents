package main

import (
	"os"
	"path/filepath"
	"testing"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func file(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestConfigFromEnv(t *testing.T) {
	token, pass := file(t, "tok\n"), file(t, "pw\n")
	s, err := configFromEnv(env(map[string]string{
		envBusURL:       "tls://platform-agent-a2a-nats.kubeagents-system.svc:4222",
		envTokenFile:    token,
		envPasswordFile: pass,
		envAgent:        "platform-agent",
		envNamespace:    "kubeagents-system",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.listen != defaultListen || s.tokenFile != token {
		t.Errorf("listen %q token %q", s.listen, s.tokenFile)
	}
	if s.bus.User != defaultBusUser || s.bus.Password != "pw" {
		t.Errorf("bus user %q password %q, want the console user and the trimmed file", s.bus.User, s.bus.Password)
	}
	if s.bus.Deployment.Agent != "platform-agent" || s.bus.Deployment.Namespace != "kubeagents-system" {
		t.Errorf("deployment = %+v", s.bus.Deployment)
	}
}

// Each of these is a pod that must not start: there is no default bus, no
// running without a token, and no running without the bus credential.
func TestConfigFromEnvRefusesToStartWithoutWhatItNeeds(t *testing.T) {
	token, pass := file(t, "tok"), file(t, "pw")
	base := map[string]string{envBusURL: "tls://bus:4222", envTokenFile: token, envPasswordFile: pass}
	for name, override := range map[string]map[string]string{
		"no bus URL":         {envBusURL: ""},
		"a missing token":    {envTokenFile: filepath.Join(t.TempDir(), "absent")},
		"an empty token":     {envTokenFile: file(t, " \n")},
		"a missing password": {envPasswordFile: filepath.Join(t.TempDir(), "absent")},
		"an empty password":  {envPasswordFile: file(t, "\n")},
	} {
		t.Run(name, func(t *testing.T) {
			m := map[string]string{}
			for k, v := range base {
				m[k] = v
			}
			for k, v := range override {
				m[k] = v
			}
			if _, err := configFromEnv(env(m)); err == nil {
				t.Errorf("started with %s", name)
			}
		})
	}
}

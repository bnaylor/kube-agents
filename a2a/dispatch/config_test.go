package dispatch

import (
	"strings"
	"testing"
	"time"
)

func TestConfigDefaultsAreValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	c := DefaultConfig()
	if c.Lease != 90*time.Second || c.Renew != 20*time.Second || c.RestartGrace != 60*time.Second || c.AcceptTimeout != 10*time.Second {
		t.Errorf("defaults %+v", c)
	}
}

// The restart grace must span at least two renewals, and the lease three.
func TestConfigRefusesUnsafeIntervals(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"grace under two renewals":   func(c *Config) { c.RestartGrace = 2*c.Renew - time.Second },
		"lease under three renewals": func(c *Config) { c.Lease = 3*c.Renew - time.Second },
		"lease equal to renewal":     func(c *Config) { c.Lease = c.Renew },
		"fractional renewal":         func(c *Config) { c.Renew = 1500 * time.Millisecond },
		"zero accept timeout":        func(c *Config) { c.AcceptTimeout = 0 },
		"no submitter":               func(c *Config) { c.Submitter = "" },
	} {
		c := DefaultConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, c)
		}
	}
	c := DefaultConfig()
	c.RestartGrace = 2 * c.Renew
	if err := c.Validate(); err != nil {
		t.Errorf("grace of exactly two renewals refused: %v", err)
	}
}

func TestConfigFromEnv(t *testing.T) {
	env := map[string]string{EnvLeaseSeconds: "120", EnvRenewSeconds: "30", EnvRestartGraceSeconds: "61", EnvAcceptTimeoutSeconds: "5"}
	c, err := ConfigFromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.Lease != 120*time.Second || c.Renew != 30*time.Second || c.RestartGrace != 61*time.Second || c.AcceptTimeout != 5*time.Second {
		t.Errorf("from env %+v", c)
	}
	env[EnvRestartGraceSeconds] = "59" // under two renewals of 30s
	if _, err := ConfigFromEnv(func(k string) string { return env[k] }); err == nil || !strings.Contains(err.Error(), "restart grace") {
		t.Errorf("grace under two renewals from env: %v", err)
	}
	env[EnvRestartGraceSeconds] = "soon"
	if _, err := ConfigFromEnv(func(k string) string { return env[k] }); err == nil {
		t.Error("a non-numeric interval was accepted")
	}
}

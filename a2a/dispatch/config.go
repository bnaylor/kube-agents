// Package dispatch is the profile dispatcher (#3655; the design is
// docs/designs/spec-profiles-dispatch.md, "Admission and holding"): the only
// fetcher and holder of profile submissions. It binds the operator's held
// durable for each (profile, tier, kind), admits or refuses each submission,
// applies the per-profile and install caps, hands an admitted task to one
// warm worker on that worker's own dispatch subject, and holds the
// submission unacknowledged for the task's whole life while the worker
// renews a liveness lease. A task whose lease expires or whose deadline
// passes ends in a terminal failure and is never run again.
//
// The wire shapes (the hand-off payloads, the lease record, the reason
// codes, the terminal notice) follow #3655's interface contract and its
// addendum, and are provisional until PR 2 lands the shared types.
package dispatch

import (
	"fmt"
	"strconv"
	"time"
)

// The environment the operator renders the PlatformAgent's
// spec.harness.tuning.profiles intervals into (contract section 5). The
// names are this PR's proposal; PR 2 renders them.
const (
	EnvLeaseSeconds         = "PROFILE_LEASE_SECONDS"
	EnvRenewSeconds         = "PROFILE_RENEW_SECONDS"
	EnvRestartGraceSeconds  = "PROFILE_RESTART_GRACE_SECONDS"
	EnvAcceptTimeoutSeconds = "PROFILE_ACCEPT_TIMEOUT_SECONDS"
)

// The interval defaults, from the contract's table.
const (
	DefaultLease         = 90 * time.Second
	DefaultRenew         = 20 * time.Second
	DefaultRestartGrace  = 60 * time.Second
	DefaultAcceptTimeout = 10 * time.Second
)

const (
	// DefaultRetryDelay is how long a submission that is over a cap, or has
	// no free worker, waits before it is offered to the dispatcher again.
	DefaultRetryDelay = 5 * time.Second
	// DefaultHealthInterval paces the health line.
	DefaultHealthInterval = time.Minute
	// DefaultStallAfter is how long a queue may wait over its cap with no
	// admission before the health line calls it stalled: the shape a
	// cross-chain admission cycle takes before the parents' deadlines end it.
	DefaultStallAfter = 5 * time.Minute
	// DefaultRefreshInterval paces re-reading the profile config and binding
	// any durable it adds.
	DefaultRefreshInterval = 30 * time.Second
	// DefaultSubmitter is the one principal whose submissions are held.
	DefaultSubmitter = "gateway"
	// DefaultStream is the stream the durables are on.
	DefaultStream = "TASKS"
	// selfSession and selfAgentType are the `from` of the terminals the
	// dispatcher writes.
	selfSession   = "dispatcher"
	selfAgentType = "a2a-dispatcher"

	// minGraceRenewals is how many renewal intervals the restart grace must
	// span at least, so a live worker gets two chances to be heard by a
	// restarted dispatcher before its task is failed.
	minGraceRenewals = 2
	// minLeaseRenewals is how many renewals a lease must span: the bound
	// lib.DispatchMessage.Validate holds every offer to, so a config under
	// it could not make one valid offer.
	minLeaseRenewals = 3
	// intervalUnit is the offer's unit: lease and renewal ride it as whole
	// seconds.
	intervalUnit = time.Second
)

// Config is the dispatcher's timing and identity. Every interval is
// explicit, with the defaults above.
type Config struct {
	// Lease is how long a lease lives without a renewal.
	Lease time.Duration
	// Renew is how often a worker renews; it rides the offer.
	Renew time.Duration
	// RestartGrace counts from the dispatcher's start: a re-adopted task is
	// failed lease-expired only if no renewal arrives within it.
	RestartGrace time.Duration
	// AcceptTimeout is how long an offer waits for its accept.
	AcceptTimeout time.Duration

	RetryDelay      time.Duration
	HealthInterval  time.Duration
	StallAfter      time.Duration
	RefreshInterval time.Duration
	Submitter       string
	Stream          string
}

// DefaultConfig is every default.
func DefaultConfig() Config {
	return Config{
		Lease:           DefaultLease,
		Renew:           DefaultRenew,
		RestartGrace:    DefaultRestartGrace,
		AcceptTimeout:   DefaultAcceptTimeout,
		RetryDelay:      DefaultRetryDelay,
		HealthInterval:  DefaultHealthInterval,
		StallAfter:      DefaultStallAfter,
		RefreshInterval: DefaultRefreshInterval,
		Submitter:       DefaultSubmitter,
		Stream:          DefaultStream,
	}
}

// Validate refuses a config that would fail live work or every offer: a
// lease under three renewals (the offer's own bound, and fewer would expire
// a live task on one or two lost renewals), a restart grace under two
// renewals (a restarted dispatcher could fail a live task on one lost
// renewal), and a lease or renewal that isn't whole seconds (the offer
// carries seconds).
func (c Config) Validate() error {
	switch {
	case c.Renew <= 0 || c.Lease <= 0 || c.RestartGrace <= 0 || c.AcceptTimeout <= 0:
		return fmt.Errorf("dispatch config: lease %v, renew %v, restart grace %v and accept timeout %v must all be positive",
			c.Lease, c.Renew, c.RestartGrace, c.AcceptTimeout)
	case c.Lease%intervalUnit != 0 || c.Renew%intervalUnit != 0:
		return fmt.Errorf("dispatch config: lease %v and renewal interval %v must be whole seconds", c.Lease, c.Renew)
	case c.Lease < minLeaseRenewals*c.Renew:
		return fmt.Errorf("dispatch config: lease %v must be at least %d renewal intervals (%v)", c.Lease, minLeaseRenewals, minLeaseRenewals*c.Renew)
	case c.RestartGrace < minGraceRenewals*c.Renew:
		return fmt.Errorf("dispatch config: restart grace %v must be at least %d renewal intervals (%v)",
			c.RestartGrace, minGraceRenewals, minGraceRenewals*c.Renew)
	case c.RetryDelay <= 0 || c.HealthInterval <= 0 || c.StallAfter <= 0 || c.RefreshInterval <= 0:
		return fmt.Errorf("dispatch config: retry delay, health interval, stall threshold and refresh interval must be positive")
	case c.Submitter == "" || c.Stream == "":
		return fmt.Errorf("dispatch config: submitter and stream are required")
	}
	return nil
}

// ConfigFromEnv is DefaultConfig with the four intervals read from the
// environment (whole seconds), then validated.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	c := DefaultConfig()
	for _, f := range []struct {
		name string
		into *time.Duration
	}{
		{EnvLeaseSeconds, &c.Lease},
		{EnvRenewSeconds, &c.Renew},
		{EnvRestartGraceSeconds, &c.RestartGrace},
		{EnvAcceptTimeoutSeconds, &c.AcceptTimeout},
	} {
		v := getenv(f.name)
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("%s=%q: want a positive whole number of seconds", f.name, v)
		}
		*f.into = time.Duration(n) * time.Second
	}
	return c, c.Validate()
}

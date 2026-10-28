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
// The wire shapes and the install config are #3655's interface contract
// (lib/dispatch.go, from PR 2); the reason codes and the terminal notice
// are provisional until the contract settles them.
package dispatch

import (
	"fmt"
	"slices"
	"time"
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
	// DefaultRefreshInterval paces re-reading the config's durables and
	// binding or unbinding them.
	DefaultRefreshInterval = 10 * time.Second
	// DefaultSubmitter is the one principal whose submissions are held.
	DefaultSubmitter = "gateway"
	// DefaultStream is the stream the durables are on.
	DefaultStream = "TASKS"
	// DefaultUnattendedTier is the one unattended tier until tier CRs say
	// which tiers are.
	DefaultUnattendedTier = "unattended"
	// selfSession and selfAgentType are the `from` of the terminals the
	// dispatcher writes.
	selfSession   = "dispatcher"
	selfAgentType = "a2a-dispatcher"
)

// Config is the dispatcher's process-level settings. The install's caps,
// intervals and profiles are not here: they come from the Source, per
// decision.
type Config struct {
	RetryDelay      time.Duration
	HealthInterval  time.Duration
	StallAfter      time.Duration
	RefreshInterval time.Duration
	Submitter       string
	Stream          string
	// UnattendedTiers are the tiers on the unattended side of the
	// interactive reserve.
	UnattendedTiers []string
}

// DefaultConfig is every default.
func DefaultConfig() Config {
	return Config{
		RetryDelay:      DefaultRetryDelay,
		HealthInterval:  DefaultHealthInterval,
		StallAfter:      DefaultStallAfter,
		RefreshInterval: DefaultRefreshInterval,
		Submitter:       DefaultSubmitter,
		Stream:          DefaultStream,
		UnattendedTiers: []string{DefaultUnattendedTier},
	}
}

// Validate refuses a config the dispatcher can't run on.
func (c Config) Validate() error {
	switch {
	case c.RetryDelay <= 0 || c.HealthInterval <= 0 || c.StallAfter <= 0 || c.RefreshInterval <= 0:
		return fmt.Errorf("dispatch config: retry delay, health interval, stall threshold and refresh interval must be positive")
	case c.Submitter == "" || c.Stream == "":
		return fmt.Errorf("dispatch config: submitter and stream are required")
	}
	return nil
}

func (c Config) unattended(tier string) bool { return slices.Contains(c.UnattendedTiers, tier) }

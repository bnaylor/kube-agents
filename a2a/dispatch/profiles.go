package dispatch

import (
	"slices"
	"sync"
	"time"
)

const (
	// DefaultQueueTimeout is the spec's default for an AgentProfile's
	// queueTimeoutSeconds.
	DefaultQueueTimeout = 3600 * time.Second
	// DefaultCeiling is the install ceiling when the config gives none: the
	// spec's maxInProgress default, which the ceiling follows while both
	// exist.
	DefaultCeiling = 6
	// defaultTier is a profile's allowed tier when it lists none.
	defaultTier = "read"
	// defaultUnattendedTier is the one unattended tier until tier CRs say
	// which tiers are.
	defaultUnattendedTier = "unattended"
	// reserveDivisor is the 3 in max(1, ceil(ceiling/3)).
	reserveDivisor = 3
)

// Parameter is one entry of an AgentProfile's spec.parameters (contract
// addendum 1).
type Parameter struct {
	Name     string `json:"name"`
	Required bool   `json:"required,omitempty"`
}

// Profile is what the dispatcher needs from one AgentProfile, plus the ready
// worker pods of each of its (profile, tier) pools.
type Profile struct {
	Name                  string      `json:"name"`
	Concurrency           int         `json:"concurrency"`
	AllowedTiers          []string    `json:"allowedTiers,omitempty"`
	QueueTimeoutSeconds   int         `json:"queueTimeoutSeconds,omitempty"`
	ActiveDeadlineSeconds int         `json:"activeDeadlineSeconds"`
	Parameters            []Parameter `json:"parameters,omitempty"`
	// Pods are the ready warm-worker pods by tier.
	Pods map[string][]string `json:"pods,omitempty"`
}

// Tiers applies the default (read alone) to an empty list.
func (p Profile) Tiers() []string {
	if len(p.AllowedTiers) == 0 {
		return []string{defaultTier}
	}
	return p.AllowedTiers
}

// AllowsTier reports whether tier is in the profile's allowed tiers.
func (p Profile) AllowsTier(tier string) bool { return slices.Contains(p.Tiers(), tier) }

// QueueTimeout applies the spec's default.
func (p Profile) QueueTimeout() time.Duration {
	if p.QueueTimeoutSeconds <= 0 {
		return DefaultQueueTimeout
	}
	return time.Duration(p.QueueTimeoutSeconds) * time.Second
}

// ActiveDeadline is the task's wall-clock limit.
func (p Profile) ActiveDeadline() time.Duration {
	return time.Duration(p.ActiveDeadlineSeconds) * time.Second
}

// Snapshot is the install's profile config at one moment. Caps are computed
// from it per decision, never fixed at startup.
type Snapshot struct {
	// Namespace is the install's namespace, the verifier target of a task
	// with no cluster parameters.
	Namespace string `json:"namespace"`
	// Ceiling is the install-wide limit on running profile tasks, every kind
	// counted. Zero means DefaultCeiling.
	Ceiling int `json:"ceiling,omitempty"`
	// ChildReserve is the share of Ceiling top-level tasks can't take. Nil
	// means max(1, ceil(Ceiling/3)).
	ChildReserve *int `json:"childReserve,omitempty"`
	// UnattendedTiers are the tiers on the unattended side of the
	// interactive reserve. Nil means ["unattended"].
	UnattendedTiers []string  `json:"unattendedTiers,omitempty"`
	Profiles        []Profile `json:"profiles"`
}

// Profile finds one profile by name.
func (s Snapshot) Profile(name string) (Profile, bool) {
	for _, p := range s.Profiles {
		if p.Name == name {
			return p, true
		}
	}
	return Profile{}, false
}

// EffectiveCeiling applies the default.
func (s Snapshot) EffectiveCeiling() int {
	if s.Ceiling <= 0 {
		return DefaultCeiling
	}
	return s.Ceiling
}

// reserveOf is max(1, ceil(n/3)).
func reserveOf(n int) int {
	return max(1, (n+reserveDivisor-1)/reserveDivisor)
}

// InteractiveReserve is the share of the ceiling unattended work can't take.
func (s Snapshot) InteractiveReserve() int { return reserveOf(s.EffectiveCeiling()) }

// EffectiveChildReserve is the share of the ceiling top-level work can't
// take, so parents waiting on their children never hold every slot.
func (s Snapshot) EffectiveChildReserve() int {
	if s.ChildReserve != nil {
		return max(0, *s.ChildReserve)
	}
	return reserveOf(s.EffectiveCeiling())
}

// Unattended reports whether tier is on the unattended side.
func (s Snapshot) Unattended(tier string) bool {
	if s.UnattendedTiers == nil {
		return tier == defaultUnattendedTier
	}
	return slices.Contains(s.UnattendedTiers, tier)
}

// Source hands out the current snapshot. It is read once per decision.
type Source interface {
	Snapshot() Snapshot
}

// StaticSource is a Source whose snapshot is replaced whole by Set: what the
// config-file loader and the tests use.
type StaticSource struct {
	mu   sync.RWMutex
	snap Snapshot
}

// NewStaticSource starts a source at snap.
func NewStaticSource(snap Snapshot) *StaticSource { return &StaticSource{snap: snap} }

// Snapshot implements Source.
func (s *StaticSource) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

// Set replaces the snapshot.
func (s *StaticSource) Set(snap Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = snap
}

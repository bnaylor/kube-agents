// Package authcallout implements the NATS auth callout service the deployment
// spec specifies: it validates a client's Kubernetes ServiceAccount token
// against the cluster and answers with the account and permission set that
// identity is mapped to.
//
// The property it exists to preserve is the deployment spec's, unchanged: the
// bus decides who may say what before a message is read. What changes is where
// the permission set comes from. Statically rendered users put one password per
// role in a Secret, so every workload sharing a role shares a credential and
// the grant list can only be as fine as the roles someone thought to write. The
// callout resolves an identity the cluster already issues and vouches for, so
// the grant list can be as fine as the identities are.
//
// The agents never read the map. The constrained party does not see its own
// ceiling; it just hits it.
package authcallout

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	// ServiceAccountPrefix is how the Kubernetes TokenReview API spells a
	// ServiceAccount in the username it returns:
	// system:serviceaccount:<namespace>:<name>. Entries are keyed by that
	// exact string so the lookup compares against what the API server said
	// rather than against something reassembled from parts.
	ServiceAccountPrefix = "system:serviceaccount:"

	// serviceAccountFields is the token count of a well-formed
	// ServiceAccount username once split on ":" — system, serviceaccount,
	// namespace, name.
	serviceAccountFields = 4
)

// Grants is one identity's subject permissions, deny-by-default: a subject
// absent from both lists is refused by the server, and the lists are exact
// rather than namespace wildcards wherever the deployment spec requires it.
type Grants struct {
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
}

// Identity is one entry in the map: a Kubernetes ServiceAccount, the NATS user
// it authenticates as, and what that user may do.
type Identity struct {
	// ServiceAccount is the full TokenReview username this entry matches.
	ServiceAccount string `json:"serviceAccount"`

	// User is the NATS user name issued for this identity. It is not
	// decoration: the grant lists carry a per-user inbox prefix
	// (_INBOX.<user>.>), and a client must set a matching custom inbox
	// prefix or every reply it waits on times out. The operator renders
	// this name and the client's own copy of it from one source for that
	// reason.
	User string `json:"user"`

	// Account is the NATS account the issued user lands in. The account is
	// the tenant boundary and the blast-radius container.
	Account string `json:"account"`

	Grants Grants `json:"grants"`
}

// IdentityMap is what the operator renders and the callout serves. Version is
// the operator's content hash of the entries; the callout reports the version
// it is serving so "the map says X" is checkable against the running system
// rather than against the rendered object.
type IdentityMap struct {
	Version    string     `json:"version"`
	Identities []Identity `json:"identities"`
}

// ParseIdentityMap decodes a rendered map and rejects one it cannot serve
// safely. Rejecting here rather than at lookup time is deliberate: a malformed
// entry that surfaces only when its identity happens to connect is a refusal
// for one legitimate workload at an arbitrary later moment, which is the
// failure mode the BusCredentialsReady ordering exists to prevent.
func ParseIdentityMap(raw []byte) (*IdentityMap, error) {
	var m IdentityMap
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decoding identity map: %w", err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

func (m *IdentityMap) validate() error {
	if m.Version == "" {
		return fmt.Errorf("identity map has no version")
	}
	seenSA := make(map[string]bool, len(m.Identities))
	seenUser := make(map[string]bool, len(m.Identities))
	for i, id := range m.Identities {
		if err := id.validate(); err != nil {
			return fmt.Errorf("identity %d: %w", i, err)
		}
		// Two entries for one ServiceAccount would make the grant set a
		// function of map order, and two ServiceAccounts sharing a NATS
		// user would silently re-create the shared-credential problem
		// the callout exists to end — the second one also collides on
		// the first one's inbox prefix.
		if seenSA[id.ServiceAccount] {
			return fmt.Errorf("identity %d: duplicate serviceAccount %q", i, id.ServiceAccount)
		}
		if seenUser[id.User] {
			return fmt.Errorf("identity %d: duplicate user %q", i, id.User)
		}
		seenSA[id.ServiceAccount] = true
		seenUser[id.User] = true
	}
	return nil
}

func (id Identity) validate() error {
	if !strings.HasPrefix(id.ServiceAccount, ServiceAccountPrefix) ||
		len(strings.Split(id.ServiceAccount, ":")) != serviceAccountFields {
		return fmt.Errorf("serviceAccount %q is not %s<namespace>:<name>", id.ServiceAccount, ServiceAccountPrefix)
	}
	if id.User == "" {
		return fmt.Errorf("serviceAccount %q has no user", id.ServiceAccount)
	}
	if id.Account == "" {
		return fmt.Errorf("user %q has no account", id.User)
	}
	// An entry granting nothing at all is almost certainly a render bug,
	// and serving it produces a client that connects and then hangs on its
	// first reply — the hardest failure in this system to read from the
	// outside. Refuse the map instead.
	if len(id.Grants.Publish) == 0 && len(id.Grants.Subscribe) == 0 {
		return fmt.Errorf("user %q has no grants", id.User)
	}
	return nil
}

// Lookup returns the identity mapped to a TokenReview username. The bool
// result distinguishes an unmapped identity — which the callout refuses
// outright — from a mapped one, and callers must not treat a zero Identity as
// a usable grant set.
func (m *IdentityMap) Lookup(serviceAccount string) (Identity, bool) {
	for _, id := range m.Identities {
		if id.ServiceAccount == serviceAccount {
			return id, true
		}
	}
	return Identity{}, false
}

// Users returns the NATS user names the map serves, sorted. The operator reads
// this back when deciding whether the callout is serving the identity a
// workload is about to need.
func (m *IdentityMap) Users() []string {
	users := make([]string, 0, len(m.Identities))
	for _, id := range m.Identities {
		users = append(users, id.User)
	}
	sort.Strings(users)
	return users
}

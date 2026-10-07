package authcallout

import (
	"fmt"
	"strings"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// Reserved principal names: the users a narrowed pod may not be named after.
//
// A narrowed user is named for its pod, and that name is also its inbox
// prefix: sessionGrants hands it `_INBOX.<pod>.>` on both sides. A pod named
// after another principal would therefore be granted that principal's inbox,
// and could read the JetStream replies delivered there or publish forged ones
// into it. The gateway mints session pod names that never collide with a
// principal; a pod someone created by hand under a narrowed ServiceAccount can
// be named anything.
//
// The static users nats.conf authenticates by password (gateway, bridge, seed,
// web, console, sys, and the callout's own user) are in no identity map, so the
// callout cannot learn them from the map it serves. The operator renders them
// into the callout's environment instead, from the same list it renders
// nats.conf's auth_users from, as a comma-separated list. The callout does not
// read nats.conf: that file carries every static user's password, and the
// callout has no reason to hold any of them.

const (
	// reservedPrincipalSeparator splits the operator-rendered list.
	reservedPrincipalSeparator = ","
)

// ParseReservedPrincipals reads the operator-rendered list of static principal
// names. It fails closed: an empty list, an empty element, or a name that is
// not a single dot-free DNS-1123 label is an error, never a smaller set. A
// smaller set would admit a narrowed pod named after the dropped principal, and
// a parse that quietly skipped a malformed name would do exactly that.
//
// The label check is also what makes an exact comparison the right one. Pod
// names are DNS-1123 and so lowercase; a reserved name that is lowercase
// DNS-1123 too can be compared byte for byte, and a rendered name in any other
// case is refused here rather than silently never matching a pod.
func ParseReservedPrincipals(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("the reserved principal list is empty; a narrowed pod could take any static principal's name and inbox")
	}
	var names []string
	for _, field := range strings.Split(raw, reservedPrincipalSeparator) {
		name := strings.TrimSpace(field)
		if name == "" {
			return nil, fmt.Errorf("the reserved principal list %q has an empty element", raw)
		}
		if !lib.ValidSubjectToken(name) {
			return nil, fmt.Errorf("reserved principal %q is not a dot-free DNS-1123 label, so no pod could be refused for it", name)
		}
		names = append(names, name)
	}
	return names, nil
}

// reservedSet builds the lookup the callout refuses against. It refuses an
// empty input for the same reason ParseReservedPrincipals does, so a caller
// that skipped the parser cannot construct a Service that reserves nothing.
func reservedSet(names []string) (map[string]struct{}, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("the callout needs the static principal names; with none, a narrowed pod could take any of their inboxes")
	}
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		if !lib.ValidSubjectToken(n) {
			return nil, fmt.Errorf("reserved principal %q is not a dot-free DNS-1123 label, so no pod could be refused for it", n)
		}
		set[n] = struct{}{}
	}
	return set, nil
}

// reservedPrincipal reports whether a narrowed user name belongs to a static
// principal.
func (s *Service) reservedPrincipal(user string) bool {
	_, ok := s.reserved[user]
	return ok
}

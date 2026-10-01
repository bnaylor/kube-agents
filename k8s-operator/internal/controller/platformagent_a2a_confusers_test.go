package controller

import (
	"strings"
	"testing"
)

// A static user with one side populated and the other empty must render an
// explicit deny on the empty side.
//
// This is the same defect the callout's identity-map validator refuses, in the
// half of the render that has no validator. An absent `subscribe` key is not an
// empty allow list: nats-server's parseUserPermissions sets only the side it
// finds, and a side it never set is unrestricted. So a publish-only static user
// would ship able to subscribe to every subject on the bus -- including every
// other principal's inbox -- while its publishes stayed correctly narrow.
//
// Measured against a real nats-server 2.14.6 before the fix: a one-sided user
// subscribed to ">" with no error, while a two-sided one was refused with a
// permissions violation.
func TestAOneSidedStaticUserRendersADenyOnTheEmptySide(t *testing.T) {
	for _, tc := range []struct {
		name          string
		publish       []string
		subscribe     []string
		wantDenied    string
		wantNotDenied string
	}{
		{
			name:          "subscribe empty",
			publish:       []string{"a2a.tasks.>"},
			wantDenied:    "subscribe",
			wantNotDenied: "publish",
		},
		{
			name:          "publish empty",
			subscribe:     []string{"_INBOX.u.>"},
			wantDenied:    "publish",
			wantNotDenied: "subscribe",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := renderA2AStaticUser(a2aIdentity{
				user:      "u",
				account:   a2aAccountApp,
				auth:      a2aAuthStatic,
				comment:   "a one-sided principal",
				publish:   tc.publish,
				subscribe: tc.subscribe,
			}, "pw")

			if !strings.Contains(got, tc.wantDenied+` { deny = [">"] }`) {
				t.Errorf("%s side is empty but renders no deny, so the server reads it as unrestricted:\n%s", tc.wantDenied, got)
			}
			if strings.Contains(got, tc.wantNotDenied+` { deny =`) {
				t.Errorf("%s side has grants and must not gain a deny:\n%s", tc.wantNotDenied, got)
			}
		})
	}
}

// Neither side populated still means no permissions block at all. This is how
// the $SYS user ships: it holds the system account's own privileges, and a
// block naming no subjects would take them away.
func TestAStaticUserWithNoSubjectsRendersNoPermissionsBlock(t *testing.T) {
	got := renderA2AStaticUser(a2aIdentity{
		user:    "sys",
		account: a2aAccountSys,
		auth:    a2aAuthStatic,
		comment: "the system account's own user",
	}, "pw")

	if strings.Contains(got, "permissions") {
		t.Errorf("a user with no subject lists gained a permissions block, which would deny it everything:\n%s", got)
	}
}

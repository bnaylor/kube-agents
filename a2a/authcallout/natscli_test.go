package authcallout

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The provision Job's authentication path, run with the real nats CLI.
//
// This is the one product client that authenticates through the callout in this
// change, and its credential handling is not our code: the operator renders a
// shell script that hands the CLI a ServiceAccount token in the password field,
// and whether that works is a fact about the CLI's flag handling rather than
// about the callout. Everything else in this package tests our own client
// library, which would agree with us whether or not the CLI does.
//
// So the shape asserted here is the shape the render emits:
//
//	nats --server S --user <serviceaccount> --password <token> \
//	     --inbox-prefix=_INBOX.provision ...
//
// The CLI comes from `go install github.com/nats-io/natscli/nats`, which needs
// no container; the a2a CI job installs it for exactly this test.
func natsCLI(t *testing.T) string {
	t.Helper()
	if path := os.Getenv("NATS_CLI"); path != "" {
		return path
	}
	path, err := exec.LookPath("nats")
	if err != nil {
		t.Skip("the nats CLI is not on PATH; set NATS_CLI or `go install github.com/nats-io/natscli/nats@latest`")
	}
	return path
}

func TestTheProvisionJobsCredentialShapeWorksWithTheRealCLI(t *testing.T) {
	cli := natsCLI(t)
	h := startLiveHarness(t)

	token := h.mintToken(t, provisionSAName, busAudience)
	serviceAccount := "system:serviceaccount:" + h.namespace + ":" + provisionSAName
	server := strings.TrimPrefix(h.nats.ClientURL(), "nats://")

	run := func(extra ...string) (string, error) {
		args := append([]string{
			"--server", server,
			"--user", serviceAccount,
			"--password", token,
			"--inbox-prefix=_INBOX.provision",
		}, extra...)
		out, err := exec.Command(cli, args...).CombinedOutput()
		return string(out), err
	}

	// A $JS.API request, which is what every stream and bucket call in the
	// provisioning script is. It only returns if the reply reached the inbox
	// prefix this principal is granted.
	if out, err := run("stream", "ls"); err != nil {
		t.Errorf("the provision credential could not make a JetStream API call: %v\n%s", err, out)
	}

	// A provisioned topic, which the script writes starter entries to.
	if out, err := run("pub", "a2a.topics.shared.blueprint", "hello"); err != nil {
		t.Errorf("the provision credential could not publish a granted topic: %v\n%s", err, out)
	}

	// The task plane, which this principal deliberately does not hold: a
	// provisioner that can publish tasks can impersonate the fabric.
	out, err := run("pub", "a2a.tasks.platform.t1.in", "nope")
	if err == nil {
		t.Error("the provision credential reached the task plane")
	}
	if !strings.Contains(out, "Permissions Violation") {
		t.Errorf("refusal did not come from the server as a permissions violation:\n%s", out)
	}
}

// The inbox trap, reproduced against the real CLI because this is the failure
// mode the render's --inbox-prefix flag exists to prevent, and it is the one
// that does not look like an authorization failure.
//
// Every JetStream API call is answered on an inbox subject. This principal is
// granted only _INBOX.provision.>, and the CLI's default is _INBOX.<nuid> — so
// without the flag the request goes out, the reply is refused by the caller's
// own grant, and the CLI reports a TIMEOUT. W6 lost a provisioning Job to
// exactly this: the Job could never succeed and nothing said why.
func TestWithoutTheInboxPrefixTheProvisionCredentialTimesOutRatherThanFailing(t *testing.T) {
	cli := natsCLI(t)
	h := startLiveHarness(t)

	token := h.mintToken(t, provisionSAName, busAudience)
	serviceAccount := "system:serviceaccount:" + h.namespace + ":" + provisionSAName
	server := strings.TrimPrefix(h.nats.ClientURL(), "nats://")

	out, err := exec.Command(cli,
		"--server", server,
		"--user", serviceAccount,
		"--password", token,
		"stream", "ls",
	).CombinedOutput()

	if err == nil {
		t.Fatal("a call on an ungranted inbox prefix succeeded; the per-user inbox grant is not being enforced")
	}
	// The point of the assertion: it is a deadline, not a permissions error.
	// Anyone debugging this on a cluster will go looking at the network.
	if !strings.Contains(string(out), "deadline exceeded") && !strings.Contains(string(out), "timeout") {
		t.Errorf("expected a timeout, got:\n%s", out)
	}
}

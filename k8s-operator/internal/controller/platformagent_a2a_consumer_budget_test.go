package controller

// maxSessions and the TASKS consumer budget.
//
// The CRD accepts maxSessions up to 10000. Every session pod creates three
// named consumers on TASKS, so a stream pinned at max_consumers=64 cannot hold
// more than about twenty sessions: the twenty-first session's consumer create
// is refused by the server, and the operator's spec said the number was
// allowed. These tests pin the two halves of the fix — the render derives the
// cap from the CR, and the provision script refuses an install whose LIVE
// stream cannot hold what the CR asks for, loudly and at configuration time.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

const a2aSessionRolesSource = "../../../a2a/lib/session.go"

// a2aSessionConsumersPerSession is a copy of a number that lives in the a2a
// module, which this module cannot import. A copy goes stale silently, so this
// reads the original: it parses lib.SessionConsumerRoles and counts it.
//
// The parse is deliberately not a regex over the file. A regex that stops
// matching returns nothing, and "nothing" reads the same as "no roles", which
// would make this test pass by finding a count of zero on the day the
// declaration moves. Every step below fails the test instead of falling back
// to a default.
func TestSessionConsumerCountMatchesTheA2AModule(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, a2aSessionRolesSource, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v (if the a2a module moved, this test's path must move with it — it is the only thing keeping a2aSessionConsumersPerSession honest)", a2aSessionRolesSource, err)
	}

	var roles *ast.CompositeLit
	ast.Inspect(f, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range spec.Names {
			if name.Name != "SessionConsumerRoles" || i >= len(spec.Values) {
				continue
			}
			if lit, ok := spec.Values[i].(*ast.CompositeLit); ok {
				roles = lit
			}
		}
		return true
	})
	if roles == nil {
		t.Fatalf("no `var SessionConsumerRoles = []string{...}` in %s; it is what a2aSessionConsumersPerSession counts, and this test cannot check a number it cannot find", a2aSessionRolesSource)
	}
	if len(roles.Elts) == 0 {
		t.Fatalf("SessionConsumerRoles parsed as empty in %s; an empty slice here would make every budget below zero-sized", a2aSessionRolesSource)
	}

	if len(roles.Elts) != a2aSessionConsumersPerSession {
		t.Errorf("lib.SessionConsumerRoles has %d roles, a2aSessionConsumersPerSession says %d: TASKS would be sized for the wrong number of consumers per session. Update the constant (and the provision script's message, which quotes it).",
			len(roles.Elts), a2aSessionConsumersPerSession)
	}
}

// The derivation, including the floor.
//
// The floor matters as much as the arithmetic: 64 is what TASKS shipped with,
// so rendering below it on a small install would TIGHTEN a live stream's cap
// relative to today. Deriving downward is a silent capacity regression on
// somebody's working install; deriving upward is the thing being fixed. Hence
// max(64, budget), and hence the first row.
func TestTasksMaxConsumersDerivation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maxSessions *int
		wantBudget  int
		wantRender  int
	}{
		{"unset stays at the shipped cap", nil, 46, 64},
		{"below the floor does not tighten it", ptrInt(2), 22, 64},
		{"just under the floor still does not", ptrInt(15), 61, 64},
		{"above the floor derives", ptrInt(20), 76, 76},
		{"the CRD maximum", ptrInt(10000), 30016, 30016},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent := &agentv1alpha1.PlatformAgent{ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: "test-ns"}}
			if tc.maxSessions != nil {
				agent.Spec.Harness = &agentv1alpha1.HarnessSpec{Tuning: &agentv1alpha1.TuningSpec{MaxSessions: tc.maxSessions}}
			}
			if got := a2aTasksConsumerBudget(agent); got != tc.wantBudget {
				t.Errorf("budget = %d, want %d", got, tc.wantBudget)
			}
			if got := a2aTasksMaxConsumers(agent); got != tc.wantRender {
				t.Errorf("rendered max_consumers = %d, want %d", got, tc.wantRender)
			}
			if got := a2aTasksMaxConsumers(agent); got < a2aTasksMaxConsumersFloor {
				t.Errorf("rendered max_consumers = %d, below the shipped floor %d: this would tighten an existing install", got, a2aTasksMaxConsumersFloor)
			}
		})
	}
}

func ptrInt(i int) *int { return &i }

// The rendered flags, so a refactor that drops one is caught without running
// anything.
func TestTasksStreamCarriesItsLimits(t *testing.T) {
	agent := &agentv1alpha1.PlatformAgent{ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: "test-ns"}}
	sessions := 40
	agent.Spec.Harness = &agentv1alpha1.HarnessSpec{Tuning: &agentv1alpha1.TuningSpec{MaxSessions: &sessions}}
	script := a2aProvisionScript(agent)

	add, ok := streamAddInvocation(script, "TASKS")
	if !ok {
		t.Fatal("no `stream add TASKS` in the provision script; every assertion below would pass vacuously on a script that stopped creating it")
	}
	for _, want := range []string{
		"--max-msgs-per-subject=4096",
		"--max-consumers=136",
		"--discard=old",
	} {
		if !strings.Contains(add, want) {
			t.Errorf("stream add TASKS is missing %s\ngot: %s", want, add)
		}
	}
	// The per-subject cap is the one that changes replay, so it must not
	// spread to the append-only siblings by copy-paste. TOPICS-JOURNAL is
	// TASKS' retention-class twin and carries none.
	journal, ok := streamAddInvocation(script, "TOPICS-JOURNAL")
	if !ok {
		t.Fatal("no `stream add TOPICS-JOURNAL` in the provision script")
	}
	if strings.Contains(journal, "--max-msgs-per-subject") {
		t.Error("TOPICS-JOURNAL grew a per-subject cap; it is append-only and a cap there silently truncates a topic's history")
	}
}

// streamAddInvocation returns the `stream add <name>` command, joined onto one
// line through its backslash continuations.
func streamAddInvocation(script, stream string) (string, bool) {
	flat := strings.ReplaceAll(script, "\\\n", " ")
	for _, line := range strings.Split(flat, "\n") {
		if strings.Contains(line, "stream add "+stream+" ") {
			return strings.Join(strings.Fields(line), " "), true
		}
	}
	return "", false
}

// Proven by configuring it wrong: the script, executed, against a stream too
// small for the CR that rendered it.
//
// Provisioning is create-only convergence — the `stream info X || stream add X`
// guards never edit a stream that already exists — so raising maxSessions on a
// live install leaves TASKS at the cap it was created with. Without this check
// the first symptom is a legitimate session's consumer create being refused at
// load and surfacing as a task failure. With it, the provision Job fails, and a
// failed provision Job is already on the CR.
func TestProvisionRefusesATasksStreamTooSmallForMaxSessions(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		// Not skipped: the script opens with `set -euo pipefail`, which
		// dash does not have, so a runner without bash cannot execute
		// what ships either.
		t.Fatalf("bash is required to execute the provision script (it uses `set -o pipefail`): %v", err)
	}

	agent := &agentv1alpha1.PlatformAgent{ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: "test-ns"}}
	sessions := 100
	agent.Spec.Harness = &agentv1alpha1.HarnessSpec{Tuning: &agentv1alpha1.TuningSpec{MaxSessions: &sessions}}

	for _, tc := range []struct {
		name       string
		liveJSON   string
		wantExit   int
		wantStderr []string
	}{
		{
			name:       "a stream at the shipped cap cannot hold this CR",
			liveJSON:   `{"name":"TASKS","max_consumers":64,"max_msgs_per_subject":4096}`,
			wantExit:   1,
			wantStderr: []string{"max_consumers=64", "needs 316", "nats stream edit TASKS --max-consumers=316"},
		},
		{
			name:     "a stream sized for it passes",
			liveJSON: `{"name":"TASKS","max_consumers":316,"max_msgs_per_subject":4096}`,
			wantExit: 0,
		},
		{
			name:     "an operator who set it unlimited is not second-guessed",
			liveJSON: `{"name":"TASKS","max_consumers":-1,"max_msgs_per_subject":4096}`,
			wantExit: 0,
		},
		{
			// The property the comment beside the grep claims: a check
			// whose extractor stops matching must fail, not skip.
			name:       "an answer the extractor cannot read is a failure",
			liveJSON:   `{"name":"TASKS","consumer_limit":64}`,
			wantExit:   1,
			wantStderr: []string{"could not read max_consumers"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			script := stageProvisionScript(t, dir, a2aProvisionScript(agent))
			stubNats(t, dir, tc.liveJSON)

			cmd := exec.Command(bash, script)
			cmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(dir, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"),
				"BUS_USER=test-agent-a2a-provision",
			)
			var stderr strings.Builder
			cmd.Stderr = &stderr
			cmd.Stdout = &strings.Builder{}
			runErr := cmd.Run()

			gotExit := 0
			if runErr != nil {
				ee, ok := runErr.(*exec.ExitError)
				if !ok {
					t.Fatalf("running the provision script: %v\nstderr:\n%s", runErr, stderr.String())
				}
				gotExit = ee.ExitCode()
			}
			if gotExit != tc.wantExit {
				t.Fatalf("exit %d, want %d\nstderr:\n%s", gotExit, tc.wantExit, stderr.String())
			}
			for _, want := range tc.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr does not name %q; an operator cannot act on a refusal that does not say what to change\ngot:\n%s", want, stderr.String())
				}
			}
			if tc.wantExit == 0 && strings.Contains(stderr.String(), "max_consumers") {
				t.Errorf("a sufficient stream still produced a complaint:\n%s", stderr.String())
			}
		})
	}
}

// stageProvisionScript writes the script somewhere runnable, with the one
// change a test outside a pod has to make: the projected token path.
//
// The substitution is asserted rather than assumed. If the script stops reading
// that path, a silent no-op replace would leave the test running something that
// is no longer what ships.
func stageProvisionScript(t *testing.T, dir, script string) string {
	t.Helper()
	const tokenRead = `BUS_TOKEN="$(cat /var/run/secrets/a2a-bus/token)"`
	if strings.Count(script, tokenRead) != 1 {
		t.Fatalf("expected exactly one %q in the provision script, found %d; this test would otherwise run a script it did not finish adapting",
			tokenRead, strings.Count(script, tokenRead))
	}
	script = strings.Replace(script, tokenRead, `BUS_TOKEN="stub-token"`, 1)

	path := filepath.Join(dir, "provision.sh")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// stubNats puts a `nats` on PATH that reports every stream and bucket as
// already existing — the live-install shape, where the create-only guards all
// short-circuit — and answers `stream info TASKS --json` with liveJSON.
func stubNats(t *testing.T, dir, liveJSON string) {
	t.Helper()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	stub := fmt.Sprintf(`#!/bin/sh
for a in "$@"; do
  if [ "$a" = "--json" ]; then
    cat <<'JSON'
%s
JSON
    exit 0
  fi
  if [ "$a" = "add" ]; then
    echo "the stub reports every object as existing; a create here means a guard stopped guarding" >&2
    exit 1
  fi
done
exit 0
`, liveJSON)
	if err := os.WriteFile(filepath.Join(bin, "nats"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
}

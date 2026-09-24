// hermes-bridge consumes tasks addressed to the platform profile and answers
// them by invoking the hermes CLI, one subprocess per task. It runs as a
// sidecar in the platform-agent pod. Design: a2a/docs/hermes-bridge.md.
//
// PLAYGROUND POSTURE: this deployment exists to prove the A2A fabric shape.
// No queue-staleness guard is the playground, not the product; the stage-3
// dispatcher replaces it.
//
// The bus user is no longer shared. It was `worker`, one credential covering
// both this program's task plane and the `a2a` CLI's topic blackboard in the
// container next door; that split into `bridge` (here) and `agent` (the CLI),
// and main() below dials as `bridge` with a password from
// <agent>-a2a-nats-creds.
//
// Still a password, and not a debt this program can pay. The auth callout is
// armed, but it resolves an identity from a ServiceAccount token and the API
// server issues one ServiceAccount per POD -- this container shares the agent's
// pod, so a token would resolve it to the `agent` entry in the map and hand
// both containers the union of the two grant sets, which is `worker` rebuilt.
// The map's `agent` principal is therefore the one identity this program must
// NOT reach for. What unblocks a token here is the bridge leaving the pod,
// which is the stage-3 dispatcher; see bridgeIdentity in
// k8s-operator/internal/controller/platformagent_a2a_identities.go and
// a2a/docs/hermes-bridge.md.
package main

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/gke-labs/kube-agents/a2a/capability"
	hermesbridge "github.com/gke-labs/kube-agents/a2a/hermes-bridge"
)

const (
	// exitFailure is the exit for anything that went wrong after the
	// environment was read: bridge init, the run itself.
	exitFailure = 1
	// exitUsage is the exit for a missing NATS_URL, the one thing the bridge
	// cannot default; it is the code the sidecar has always used for it.
	exitUsage = 2

	// The default* values below are the environment's spelling of the
	// zero-value defaults hermesbridge.Config applies in defaults()
	// (a2a/hermes-bridge/bridge.go); the two must agree, because a variable
	// left unset and one set to its default have to configure the same
	// bridge. defaultConcurrency and defaultTaskDeadlineSeconds are the
	// platform profile's concurrency and activeDeadlineSeconds in
	// docs/designs/spec-subagent-profiles.md, which is where the numbers
	// come from.
	defaultProfile             = "platform"
	defaultConcurrency         = 2
	defaultTaskDeadlineSeconds = 7200
	defaultKillGraceSeconds    = 10
	defaultKVBucket            = "runtime-state"
	// defaultProgressIntervalSeconds is hermesbridge.DefaultProgressInterval
	// in the environment's unit.
	defaultProgressIntervalSeconds = 60
	// activityListenOff is the value that closes the activity door. The
	// Config zero value means "off" but an empty environment variable reads
	// as unset, so the daemon needs a word for it.
	activityListenOff = "off"

	// saNamespaceFile is the kubelet's projection of the pod's own
	// namespace. The bridge needs the namespace to build the scope it is
	// checked at, and unlike every other A2A workload it cannot be handed
	// POD_NAMESPACE by the operator: it is deployed through the CR's
	// spec.deployment.sidecars (a2a/docs/hermes-bridge.md), which is a
	// user-written container the operator copies verbatim. A file the
	// kubelet always mounts is the one source that does not depend on
	// whoever wrote that YAML remembering a variable.
	saNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

// errUsage is what realMain returns when NATS_URL is missing, so run can
// keep the usage exit code distinct from every other failure.
var errUsage = errors.New("NATS_URL is required")

func main() {
	os.Exit(run())
}

// run owns the process logger, the signal context and the exit code.
// Everything that can fail is in realMain, which returns the error instead
// of exiting so a test can drive it to each failure.
func run() int {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := realMain(ctx, log); err != nil {
		if errors.Is(err, errUsage) {
			return exitUsage
		}
		return exitFailure
	}
	return 0
}

// managedScopeDir is hermes's managed scope as this process sees it, the
// source each child's scope is copied from: HERMES_MANAGED_DIR when set, else
// /etc/hermes when it is a directory, else none (the child gets a hook-only
// scope). Resolved here with the rest of the environment, so the library's
// defaults stay environment-free and a test's bridge copies nothing from
// the machine it runs on.
func managedScopeDir() string {
	if v := strings.TrimSpace(os.Getenv(hermesbridge.ManagedDirEnv)); v != "" {
		return v
	}
	if st, err := os.Stat(hermesbridge.DefaultManagedDir); err == nil && st.IsDir() {
		return hermesbridge.DefaultManagedDir
	}
	return ""
}

// realMain is the bridge from environment to shutdown. Every failure is
// logged where it is found and then returned; a missing NATS_URL returns
// errUsage before anything is dialed.
func realMain(ctx context.Context, log *slog.Logger) error {
	url := os.Getenv("NATS_URL")
	if url == "" {
		log.Error("NATS_URL is required")
		return errUsage
	}
	cfg := hermesbridge.Config{
		NATSURL:      url,
		Profile:      envOr("BRIDGE_PROFILE", defaultProfile),
		Concurrency:  envInt(log, "BRIDGE_CONCURRENCY", defaultConcurrency),
		TaskDeadline: time.Duration(envInt(log, "BRIDGE_TASK_DEADLINE_SECONDS", defaultTaskDeadlineSeconds)) * time.Second,
		KillGrace:    time.Duration(envInt(log, "BRIDGE_KILL_GRACE_SECONDS", defaultKillGraceSeconds)) * time.Second,
		KVBucket:     envOr("BRIDGE_KV_BUCKET", defaultKVBucket),
		// The activity door (a2a/hermes-bridge/activity.go): on by default;
		// each child is handed whatever address the door bound.
		ActivityListen:   activityListen(envOr("BRIDGE_ACTIVITY_LISTEN", hermesbridge.DefaultActivityListen)),
		ScratchDir:       os.Getenv("BRIDGE_SCRATCH_DIR"),
		ManagedScopeDir:  managedScopeDir(),
		ProgressInterval: progressInterval(log, envInt(log, "BRIDGE_PROGRESS_INTERVAL_SECONDS", defaultProgressIntervalSeconds)),
		Logger:           log,
		// Unset means required: a submission with no capability is
		// refused. "false" is the mixed-version window only — a gateway
		// that predates the mint. It does not switch enforcement off; a
		// capability that is present is always checked.
		CapabilityOptional: os.Getenv("A2A_CAPABILITY_REQUIRED") == "false",
	}
	cfg.Scope = capabilityScope(log)
	if bin := os.Getenv("HERMES_BIN"); bin != "" {
		cfg.Command = []string{bin, "-p", cfg.Profile, "chat", "-Q", "-q"}
	}
	if user := os.Getenv("NATS_USER"); user != "" {
		cfg.NATSOptions = append(cfg.NATSOptions, nats.UserInfo(user, os.Getenv("NATS_PASSWORD")))
		// Push delivery answers on inbox subjects, and this user may only
		// subscribe under its own prefix - the CLI default _INBOX.<nuid>
		// would be refused and every JS API call would time out.
		cfg.NATSOptions = append(cfg.NATSOptions, nats.CustomInboxPrefix("_INBOX."+user))
	}

	b, err := hermesbridge.New(ctx, cfg)
	if err != nil {
		log.Error("bridge init failed", "err", err)
		return err
	}
	if err := b.Run(ctx); err != nil {
		log.Error("bridge exited", "err", err)
		return err
	}
	log.Info("bridge shut down cleanly")
	return nil
}

// maxDurationSeconds is the largest count of seconds a time.Duration holds;
// past it the multiplication wraps negative, which the Config reads as off.
const maxDurationSeconds = int64(math.MaxInt64) / int64(time.Second)

// progressInterval maps the environment's seconds to the Config's duration:
// 0 is off there (the Config's off is negative; its zero is the default). A
// count the duration cannot hold is refused the way envInt refuses a
// non-integer, loudly and with the default in its place, rather than
// wrapping into a silent off.
func progressInterval(log *slog.Logger, seconds int) time.Duration {
	if seconds <= 0 {
		return -1
	}
	if int64(seconds) > maxDurationSeconds {
		log.Error("progress interval out of range; using default", "key", "BRIDGE_PROGRESS_INTERVAL_SECONDS", "value", seconds, "default", defaultProgressIntervalSeconds)
		return time.Duration(defaultProgressIntervalSeconds) * time.Second
	}
	return time.Duration(seconds) * time.Second
}

// activityListen maps the environment's spelling of "off" to the Config's.
func activityListen(v string) string {
	if v == activityListenOff {
		return ""
	}
	return v
}

// capabilityScope resolves the scope this executor is checked at. It must be
// one the gateway's minted capability contains, and the gateway's unconfigured
// ceiling is namespace-scoped to the namespace the gateway runs in.
//
// The gateway is its OWN Deployment, not a container in this pod, and an
// earlier version of this comment said otherwise ("which is this pod, since
// the bridge is a sidecar in it"). What makes the two namespaces agree is not
// co-location: it is that the operator renders every object it owns into the
// CR's namespace. That is a property of the renderer, and it is the reason
// this function may read a local namespace at all.
//
// A2A_AUTHORITY_SCOPE overrides, for the same reason the session executor
// takes one: an install whose gateway was given a narrower ceiling has to be
// able to say so here too.
//
// POD_NAMESPACE is what a rendered install actually takes. The operator sets
// it from the downward API on every sidecar it renders (a2aExecutorSidecarEnv,
// k8s-operator/internal/controller/platformagent_a2a_callout.go), because the
// third rung below cannot resolve in this pod: buildPodTemplateSpec sets
// AutomountServiceAccountToken false, so the kubelet projects no serviceaccount
// directory and the read returns ENOENT. That was not a gap in coverage, it was
// a default install refusing every `platform` task, and the fix is on the
// operator rather than here because the pod cannot supply the value itself.
//
// The file read stays, for a bridge run outside the operator's render — the
// live harness, a hand-written Deployment — where a projected token is the
// normal thing to have. It is the last rung, not the expected one.
//
// A namespace this cannot resolve is deliberately left empty rather than
// guessed. An empty scope is contained by nothing, so every task is refused
// and the install fails loudly at the first turn — the alternative, defaulting
// to a plausible namespace, would pass the check against a capability minted
// for a different one.
func capabilityScope(log *slog.Logger) capability.Scope {
	if s := os.Getenv("A2A_AUTHORITY_SCOPE"); s != "" {
		return capability.Scope(s)
	}
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		b, err := os.ReadFile(saNamespaceFile)
		if err != nil {
			log.Error("cannot resolve this pod's namespace; every task will be refused for want of a scope",
				"file", saNamespaceFile, "err", err)
			return ""
		}
		ns = strings.TrimSpace(string(b))
	}
	if ns == "" {
		log.Error("this pod's namespace resolved empty; every task will be refused for want of a scope")
		return ""
	}
	return capability.NamespaceScope(ns)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(log *slog.Logger, key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Error("bad integer env value; using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

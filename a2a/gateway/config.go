package gateway

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// defaultMaxSessions is what MaxSessions means when unset; the field's
// comment carries the sizing rationale.
const defaultMaxSessions = 10

// Config is the gateway's runtime configuration. The env contract matches
// what the W6 operator renders onto the a2a-gateway Deployment; everything
// else has playground defaults.
type Config struct {
	NATSURL      string
	NATSUser     string
	NATSPassword string
	DiscordToken string

	// PrincipalMapPath is the mounted principal-map ConfigMap.
	PrincipalMapPath string

	// DefaultAddressee is where every conversation's tasks route until a
	// per-conversation override says otherwise. Retarget 8/26: the first
	// shipped configuration routes everything to "platform" (the W7 bridge
	// executes) and spawns no session pods — the W4 switch is this setting,
	// not surgery.
	DefaultAddressee string

	// SpawnSessions arms the session-pod path (spawn/rehydrate/sweep with
	// client-go). Off until W4's worker image exists; the gateway pod has no
	// service-account token until this arms, so the k8s client is built
	// lazily.
	SpawnSessions bool

	// IdleTTL is the reap threshold since the last user message (decided
	// 8/24: 30 minutes, config-backed).
	IdleTTL time.Duration

	// AttributionSalt keys the HMAC pseudonyms in authority blocks. One
	// shared Secret per install once the deployment provisions it; this
	// install's creds Secret carries no salt key yet, so absent the env var
	// the gateway derives a stable per-install salt from the NATS password
	// (HMAC never reveals its key). Playground posture — the product gets
	// the provisioned Secret.
	AttributionSalt []byte

	// Namespace, WorkerImage, and NATSCredsSecret configure the dark spawn
	// path; the secret holds the worker user's password for spawned pods.
	Namespace       string
	WorkerImage     string
	NATSCredsSecret string

	// MaxSessions caps how many session pods run concurrently, gateway-wide
	// (A2A_MAX_SESSIONS). "Delegate:" makes pod creation user-triggerable and
	// threads are free, so the principal map bounds WHO can spawn and this
	// bounds HOW MANY - without it, one mapped user's afternoon can fill the
	// namespace. At the cap a new delegation (or a session-routed first ask)
	// is refused with a chat reply naming the numbers; nothing queues,
	// nothing drops silently.
	//
	// Zero means 10, the harness spike's "busy day": at the worker shape's
	// requests (250m/512Mi) ten concurrent sessions hold 2.5 CPU / 5Gi, which
	// a small dev cluster absorbs without preemption. Raising it buys more
	// concurrent delegations at that per-pod price plus model-quota
	// contention; lowering it turns busy-hour delegations into refusals
	// sooner - a UX decision, not a safety one, because this cap is the
	// usability half. The enforcement half is the namespace ResourceQuota
	// the operator renders above this number (a compromised or buggy gateway
	// ignores its own cap and cannot ignore that one), which is also what
	// bounds the count-then-create race between concurrent conversations.
	MaxSessions int
}

// FromEnv loads the config from the environment.
func FromEnv() (*Config, error) {
	cfg := &Config{
		NATSURL:          os.Getenv("NATS_URL"),
		NATSUser:         os.Getenv("NATS_USER"),
		NATSPassword:     os.Getenv("NATS_PASSWORD"),
		DiscordToken:     os.Getenv("DISCORD_TOKEN"),
		PrincipalMapPath: envOr("A2A_PRINCIPAL_MAP", "/etc/a2a/principal-map"),
		DefaultAddressee: envOr("A2A_DEFAULT_ADDRESSEE", "platform"),
		SpawnSessions:    os.Getenv("A2A_SPAWN_SESSIONS") == "true",
		Namespace:        envOr("POD_NAMESPACE", "kubeagents-system"),
		WorkerImage:      envOr("A2A_WORKER_IMAGE", "northamerica-northeast1-docker.pkg.dev/bnaylor-kagents-dev/a2a-demo/worker-next:latest"),
		NATSCredsSecret:  envOr("A2A_NATS_CREDS_SECRET", "platform-agent-a2a-nats-creds"),
	}
	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("NATS_URL is required")
	}
	if cfg.DiscordToken == "" {
		return nil, fmt.Errorf("DISCORD_TOKEN is required (W0's discord-bot Secret)")
	}
	// The addressee is a subject token; validate at boot, not per-message.
	// The "session" sentinel passes by construction; whether a spawner backs
	// it is checked where the spawner is built (gateway.New).
	if !lib.ValidSubjectToken(cfg.DefaultAddressee) {
		return nil, fmt.Errorf("A2A_DEFAULT_ADDRESSEE %q is not a dot-free DNS-1123 label", cfg.DefaultAddressee)
	}
	// The session cap: absent means the documented default; a value the cap
	// cannot honestly enforce (zero, negative, junk) refuses at boot rather
	// than surprising at spawn time. Keep the default in step with the
	// operator's (resolveA2AMaxSessions in the k8s-operator module), which
	// renders it explicitly onto this env var.
	maxSessions := envOr("A2A_MAX_SESSIONS", strconv.Itoa(defaultMaxSessions))
	n, err := strconv.Atoi(maxSessions)
	if err != nil || n < 1 {
		return nil, fmt.Errorf("A2A_MAX_SESSIONS %q: need an integer >= 1", maxSessions)
	}
	cfg.MaxSessions = n
	ttl := envOr("A2A_IDLE_TTL", "30m")
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return nil, fmt.Errorf("A2A_IDLE_TTL %q: %w", ttl, err)
	}
	if d < time.Minute {
		return nil, fmt.Errorf("A2A_IDLE_TTL %q is under the 1m floor; an instant reap deletes pods mid-conversation", ttl)
	}
	cfg.IdleTTL = d
	if salt := os.Getenv("A2A_ATTRIBUTION_SALT"); salt != "" {
		cfg.AttributionSalt = []byte(salt)
	} else {
		// Derived fallback while the install has no provisioned salt Secret.
		// An empty password would make this a public constant and the
		// pseudonyms an offline dictionary away from plaintext — refuse.
		if cfg.NATSPassword == "" {
			return nil, fmt.Errorf("A2A_ATTRIBUTION_SALT is required when NATS_PASSWORD is empty: the derived fallback would be a public constant")
		}
		derived := sha256.Sum256([]byte("a2a-attribution-salt:" + cfg.NATSPassword))
		cfg.AttributionSalt = derived[:]
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

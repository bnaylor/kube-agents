package gateway

import (
	"crypto/sha256"
	"fmt"
	"os"
	"time"
)

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

	// Namespace and WorkerImage configure the dark spawn path.
	Namespace   string
	WorkerImage string
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
	}
	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("NATS_URL is required")
	}
	if cfg.DiscordToken == "" {
		return nil, fmt.Errorf("DISCORD_TOKEN is required (W0's discord-bot Secret)")
	}
	ttl := envOr("A2A_IDLE_TTL", "30m")
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return nil, fmt.Errorf("A2A_IDLE_TTL %q: %w", ttl, err)
	}
	cfg.IdleTTL = d
	if salt := os.Getenv("A2A_ATTRIBUTION_SALT"); salt != "" {
		cfg.AttributionSalt = []byte(salt)
	} else {
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

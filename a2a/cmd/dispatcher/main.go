// The profile dispatcher (#3655): the only fetcher and holder of profile
// submissions. The work is in a2a/dispatch; this program reads its config,
// connects as the `dispatcher` bus identity and runs it. PR 2 of #3655
// renders its Deployment, identity, grants and the env read here.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/dispatch"
	"github.com/gke-labs/kube-agents/a2a/lib"
)

// natsUser is the principal this program authenticates as and its inbox
// owner: the contract's static identity-map entry, whose grants include
// _INBOX.dispatcher.>.
const natsUser = "dispatcher"

const (
	// envProfilesFile names the profile config (dispatch.Snapshot as JSON):
	// the AgentProfiles' dispatch fields, the install ceiling and reserves,
	// and each pool's ready pods. PROPOSED: PR 2 renders it.
	envProfilesFile = "DISPATCH_PROFILES_FILE"
	// defaultProfilesFile is where it is mounted when the env names none.
	defaultProfilesFile = "/etc/kube-agents/dispatch/profiles.json"
	// maxProfilesFileBytes bounds the config read.
	maxProfilesFileBytes = 4 << 20

	reconnectWait   = 2 * time.Second
	reconnectJitter = 500 * time.Millisecond
	clientName      = "a2a-dispatcher"
)

// errProfileRunUnavailable is the verifier's answer until #3734's
// capability.Client.CheckProfileRun is on main: every submission is refused
// verifier-refused, so the dispatcher fails closed rather than running
// unverified work.
var errProfileRunUnavailable = &capability.Refusal{Rule: "the profile.run verb is not available in this build"}

// unavailableVerifier stands in for capability.Client until #3734 lands;
// then this becomes capability.NewClient(nc, natsUser).
type unavailableVerifier struct{}

func (unavailableVerifier) CheckProfileRun(context.Context, capability.Ref, string, capability.Tier, capability.Scope) error {
	return errProfileRunUnavailable
}

func main() { os.Exit(run(os.Args[1:], os.Getenv)) }

func run(args []string, getenv func(string) string) int {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)
	cfg, profilesPath, err := parseConfig(args, getenv)
	if err != nil {
		log.Error("config", "err", err)
		return 2
	}
	url := getenv("NATS_URL")
	if url == "" {
		log.Error("NATS_URL is required")
		return 2
	}
	src, err := newFileSource(profilesPath)
	if err != nil {
		log.Error("profile config", "path", profilesPath, "err", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts, err := authOptions(getenv)
	if err != nil {
		log.Error("bus credentials", "err", err)
		return 1
	}
	client, err := lib.Connect(ctx, url, lib.WithName(clientName), lib.WithLogger(log),
		lib.WithAgreementPolicy(lib.AgreementPolicy{Submitter: cfg.Submitter}),
		lib.WithNATSOptions(append(opts, nats.ReconnectWait(reconnectWait), nats.ReconnectJitter(reconnectJitter, reconnectJitter))...))
	if err != nil {
		log.Error("bus connect", "err", err)
		return 1
	}
	defer client.Close()

	d, err := dispatch.New(cfg, client, src, unavailableVerifier{}, log)
	if err != nil {
		log.Error("dispatcher", "err", err)
		return 1
	}
	go src.watch(ctx, cfg.RefreshInterval, log)
	log.Info("dispatching", "lease", cfg.Lease, "renew", cfg.Renew, "restartGrace", cfg.RestartGrace,
		"acceptTimeout", cfg.AcceptTimeout, "profiles", profilesPath)
	if err := d.Run(ctx); err != nil {
		log.Error("dispatcher stopped", "err", err)
		return 1
	}
	log.Info("stopped")
	return 0
}

// parseConfig is the env's intervals with flags over them (for a run by
// hand), validated: the lease must span three renewals and the restart grace
// two, or the process does not start.
func parseConfig(args []string, getenv func(string) string) (dispatch.Config, string, error) {
	cfg, err := dispatch.ConfigFromEnv(getenv)
	if err != nil {
		return dispatch.Config{}, "", err
	}
	fs := flag.NewFlagSet("dispatcher", flag.ContinueOnError)
	fs.DurationVar(&cfg.Lease, "lease", cfg.Lease, "how long a lease lives without a renewal ("+dispatch.EnvLeaseSeconds+")")
	fs.DurationVar(&cfg.Renew, "renew", cfg.Renew, "how often a worker renews ("+dispatch.EnvRenewSeconds+")")
	fs.DurationVar(&cfg.RestartGrace, "restart-grace", cfg.RestartGrace, "after a start, how long a re-adopted task may go unrenewed ("+dispatch.EnvRestartGraceSeconds+")")
	fs.DurationVar(&cfg.AcceptTimeout, "accept-timeout", cfg.AcceptTimeout, "how long an offer waits for its accept ("+dispatch.EnvAcceptTimeoutSeconds+")")
	path := getenv(envProfilesFile)
	if path == "" {
		path = defaultProfilesFile
	}
	fs.StringVar(&path, "profiles", path, "the profile config file ("+envProfilesFile+")")
	if err := fs.Parse(args); err != nil {
		return dispatch.Config{}, "", err
	}
	if fs.NArg() > 0 {
		return dispatch.Config{}, "", fmt.Errorf("unexpected arguments %v", fs.Args())
	}
	return cfg, path, cfg.Validate()
}

// authOptions is the projected-token identity the deployment uses, or a
// static user for a run by hand against a bus with no callout.
func authOptions(getenv func(string) string) ([]nats.Option, error) {
	if user := getenv("NATS_USER"); user != "" {
		return []nats.Option{nats.UserInfo(user, getenv("NATS_PASSWORD")), nats.CustomInboxPrefix("_INBOX." + user)}, nil
	}
	tokenFile := getenv(lib.EnvBusTokenFile)
	if tokenFile == "" {
		tokenFile = lib.BusTokenPath
	}
	return lib.KSATokenNATSOptions(tokenFile, natsUser)
}

// fileSource is a dispatch.Source read from a JSON file and re-read on
// every refresh, so the caps follow the config without a restart. A file
// that stops parsing keeps the last good snapshot.
type fileSource struct {
	*dispatch.StaticSource
	path string

	mu      sync.Mutex
	modTime time.Time
}

func newFileSource(path string) (*fileSource, error) {
	snap, mod, err := readSnapshot(path)
	if err != nil {
		return nil, err
	}
	return &fileSource{StaticSource: dispatch.NewStaticSource(snap), path: path, modTime: mod}, nil
}

func readSnapshot(path string) (dispatch.Snapshot, time.Time, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return dispatch.Snapshot{}, time.Time{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return dispatch.Snapshot{}, time.Time{}, err
	}
	if st.Size() > maxProfilesFileBytes {
		return dispatch.Snapshot{}, time.Time{}, fmt.Errorf("%s is %d bytes, over %d", path, st.Size(), maxProfilesFileBytes)
	}
	var snap dispatch.Snapshot
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&snap); err != nil {
		return dispatch.Snapshot{}, time.Time{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := checkSnapshot(snap); err != nil {
		return dispatch.Snapshot{}, time.Time{}, fmt.Errorf("%s: %w", path, err)
	}
	return snap, st.ModTime(), nil
}

// checkSnapshot refuses a config the dispatcher can't act on safely.
func checkSnapshot(s dispatch.Snapshot) error {
	seen := map[string]bool{}
	for _, p := range s.Profiles {
		switch {
		case !lib.ValidSubjectToken(p.Name):
			return fmt.Errorf("profile name %q is not a subject token", p.Name)
		case seen[p.Name]:
			return fmt.Errorf("profile %s is listed twice", p.Name)
		case p.Concurrency < 1:
			return fmt.Errorf("profile %s: concurrency must be at least 1", p.Name)
		case p.ActiveDeadlineSeconds < 1:
			return fmt.Errorf("profile %s: activeDeadlineSeconds must be at least 1", p.Name)
		}
		seen[p.Name] = true
	}
	return nil
}

func (s *fileSource) watch(ctx context.Context, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.reload(); err != nil {
				log.Warn("profile config not reloaded; keeping the last good one", "path", s.path, "err", err)
			}
		}
	}
}

func (s *fileSource) reload() error {
	st, err := os.Stat(s.path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	unchanged := st.ModTime().Equal(s.modTime)
	s.mu.Unlock()
	if unchanged {
		return nil
	}
	snap, mod, err := readSnapshot(s.path)
	if err != nil {
		return err
	}
	s.Set(snap)
	s.mu.Lock()
	s.modTime = mod
	s.mu.Unlock()
	return nil
}

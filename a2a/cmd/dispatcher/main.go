// The profile dispatcher (#3655): the only fetcher and holder of profile
// submissions. The work is in a2a/dispatch; this program watches its config
// and its worker pools through the API, connects as the `dispatcher` bus
// identity and runs it. PR 2 of #3655 renders its Deployment, identity,
// grants, Role and the dispatch ConfigMap.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/dispatch"
	"github.com/gke-labs/kube-agents/a2a/lib"
)

// natsUser is the principal this program authenticates as and its inbox
// owner: the contract's static identity-map entry, whose grants include
// _INBOX.dispatcher.>.
const natsUser = "dispatcher"

const (
	// envNamespace is the dispatcher's own namespace, where its ConfigMap is.
	envNamespace = "POD_NAMESPACE"
	// envConfigMap names the dispatch ConfigMap (<agent>-a2a-dispatch).
	// PROPOSED: PR 2 renders it.
	envConfigMap = "DISPATCH_CONFIGMAP"

	// configWait bounds the wait for the first config at startup: past it
	// the process exits and the kubelet's restart says so.
	configWait = 2 * time.Minute
	// namespaceCheckInterval paces the check that the profile namespace the
	// pool watches is still the config's.
	namespaceCheckInterval = 10 * time.Second

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

// errProfileNamespaceMoved ends the process when the config's profile
// namespace changes under the pool's watch, so the restart re-watches it.
var errProfileNamespaceMoved = errors.New("the profile namespace changed; restarting to watch the new one")

func main() { os.Exit(run(os.Getenv)) }

func run(getenv func(string) string) int {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)
	settings, err := readSettings(getenv)
	if err != nil {
		log.Error("config", "err", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		log.Error("in-cluster config", "err", err)
		return 1
	}
	kube, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Error("kubernetes client", "err", err)
		return 1
	}
	if err := serve(ctx, log, settings, kube); err != nil {
		log.Error("dispatcher stopped", "err", err)
		return 1
	}
	log.Info("stopped")
	return 0
}

// settings is what the environment gives the process.
type settings struct {
	natsURL, namespace, configMap string
	auth                          []nats.Option
}

func readSettings(getenv func(string) string) (settings, error) {
	s := settings{natsURL: getenv("NATS_URL"), namespace: getenv(envNamespace), configMap: getenv(envConfigMap)}
	switch {
	case s.natsURL == "":
		return s, errors.New("NATS_URL is required")
	case s.namespace == "":
		return s, fmt.Errorf("%s is required", envNamespace)
	case s.configMap == "":
		return s, fmt.Errorf("%s is required", envConfigMap)
	}
	auth, err := authOptions(getenv)
	if err != nil {
		return s, fmt.Errorf("bus credentials: %w", err)
	}
	s.auth = auth
	return s, nil
}

// serve watches the config, waits for the first one, watches the worker
// pools in its profile namespace, then dispatches until ctx ends.
func serve(ctx context.Context, log *slog.Logger, s settings, kube kubernetes.Interface) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	store := dispatch.NewConfigStore(log)
	go func() { _ = store.WatchConfigMap(ctx, kube, s.namespace, s.configMap, lib.DispatchConfigKey) }()
	if err := store.WaitForConfig(ctx, configWait); err != nil {
		return err
	}
	profileNS := store.Config().ProfileNamespace
	pool := dispatch.NewPodPool(ctx, kube, profileNS, log)
	go pool.Run(ctx)
	go watchProfileNamespace(ctx, cancel, store, profileNS)

	client, err := lib.Connect(ctx, s.natsURL, lib.WithName(clientName), lib.WithLogger(log),
		lib.WithAgreementPolicy(lib.AgreementPolicy{Submitter: dispatch.DefaultSubmitter}),
		lib.WithNATSOptions(append(s.auth, nats.ReconnectWait(reconnectWait), nats.ReconnectJitter(reconnectJitter, reconnectJitter))...))
	if err != nil {
		return fmt.Errorf("bus connect: %w", err)
	}
	defer client.Close()
	d, err := dispatch.New(dispatch.DefaultConfig(), client, store, pool, unavailableVerifier{}, log)
	if err != nil {
		return err
	}
	log.Info("dispatching", "configmap", s.configMap, "profileNamespace", profileNS)
	if err := d.Run(ctx); err != nil {
		return err
	}
	if cause := context.Cause(ctx); errors.Is(cause, errProfileNamespaceMoved) {
		return cause
	}
	return nil
}

func watchProfileNamespace(ctx context.Context, cancel context.CancelCauseFunc, src dispatch.Source, watched string) {
	t := time.NewTicker(namespaceCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if ns := src.Config().ProfileNamespace; ns != watched {
				cancel(errProfileNamespaceMoved)
				return
			}
		}
	}
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

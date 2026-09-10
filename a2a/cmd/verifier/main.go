// The a2a capability verifier: the only component that reads the `cap` bucket.
//
// A broker holding a capability reference cannot resolve it — no broker has
// read on the bucket, by design (09 §4). It asks here, on a subject named for
// itself, and gets back one boolean and a sentence. The chain walk, the
// attenuation rules and the verb table live in a2a/capability; this program is
// the workload that runs them.
//
// It is its own Deployment rather than a library or a sidecar, and that is the
// design decision 09 forces. Folding it into the auth callout would stack the
// read on the seed 09 already names the largest concentration of authority in
// the deployment. Folding it into the gateway would put minting and reading in
// one process, which is the separation the whole scheme rests on.
//
// It is on the request path: while it is down, no task starts. That is the
// cost of the control, it is deliberate, and it is why this runs two replicas
// with a rollout that never drops below the running count — the same shape the
// auth callout uses for the same reason.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/lib"
)

// natsUser is the principal this program authenticates as. It is also its
// inbox prefix, and it must equal the `user` the operator renders for the
// verifier identity: the JetStream API replies every store read depends on
// land in _INBOX.<user>.>, and a mismatch authenticates fine and then times
// out on every get — the failure shape this deployment has found twice.
const natsUser = "verifier"

const (
	// reconnectJitter matches the callout's, and for the same reason: a bus
	// restart brings every client back at once (NR-6).
	reconnectJitter    = 500 * time.Millisecond
	reconnectJitterTLS = 2 * time.Second

	readyPort    = ":8080"
	readyTimeout = 5 * time.Second
)

func main() { os.Exit(run()) }

func run() int {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)

	url := os.Getenv("NATS_URL")
	if url == "" {
		log.Error("NATS_URL is required")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := connect(ctx, log, url)
	if err != nil {
		log.Error("bus connect", "err", err)
		return 1
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("jetstream", "err", err)
		return 1
	}
	// The store binds the bucket, which is a $JS.API.STREAM.INFO read. If
	// the grants are wrong this is where it shows, at boot, rather than as
	// every task in the deployment being refused one at a time.
	store, err := capability.NewStore(ctx, js)
	if err != nil {
		log.Error("bind the capability bucket; the verifier cannot answer anything without it",
			"bucket", capability.Bucket, "err", err)
		return 1
	}

	svc := &capability.Service{
		Resolver: &capability.Resolver{Store: store},
		Log:      log,
	}
	sub, err := svc.Subscribe(ctx, nc)
	if err != nil {
		log.Error("subscribe", "subject", capability.VerifySubscribe, "err", err)
		return 1
	}
	log.Info("verifying", "subject", capability.VerifySubscribe, "queue", capability.VerifyQueue,
		"bucket", capability.Bucket)

	srv := serveReady(log, nc)

	<-ctx.Done()
	// Drain rather than Unsubscribe: a request already in flight gets its
	// answer, because the alternative is a broker reading the shutdown as a
	// refusal and rejecting a task that was fine.
	if err := sub.Drain(); err != nil {
		log.Warn("drain", "err", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("stopped")
	return 0
}

// serveReady answers the readiness probe. Ready means connected to the bus:
// a verifier that cannot reach the bus cannot answer, and taking it out of
// the queue group's endpoints is better than having it silently not receive.
func serveReady(log *slog.Logger, nc *nats.Conn) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !nc.IsConnected() {
			http.Error(w, "not connected to the bus", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: readyPort, Handler: mux, ReadHeaderTimeout: readyTimeout}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("readiness listener", "err", err)
		}
	}()
	return srv
}

func connect(ctx context.Context, log *slog.Logger, url string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("a2a-cap-verifier"),
		// Retry forever rather than exiting. While this is disconnected no
		// task in the deployment can start, so crash-looping to get a
		// fresh connection is strictly worse than reconnecting.
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectJitter(reconnectJitter, reconnectJitterTLS),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("disconnected from the bus; no task can start until this recovers", "err", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Info("reconnected to the bus", "url", c.ConnectedUrl())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, s *nats.Subscription, err error) {
			// Permission violations are asynchronous and land here. On
			// this component they are the whole diagnosis: a missing
			// grant on $JS.API means every get times out with no error
			// of its own.
			subject := ""
			if s != nil {
				subject = s.Subject
			}
			log.Error("bus error", "subject", subject, "err", err)
		}),
	}
	// The projected ServiceAccount token, the same contract session pods
	// use. No shared password exists for this principal anywhere: the
	// component that reads every capability in flight should not be
	// reachable by whoever can read a Secret.
	tokenFile := os.Getenv(lib.EnvBusTokenFile)
	if tokenFile == "" {
		tokenFile = lib.BusTokenPath
	}
	switch user := os.Getenv("NATS_USER"); {
	case user != "":
		// The static path, for a run by hand against a bus with no
		// callout in front of it. The deployment does not take it.
		log.Warn("authenticating with a static password; the deployment uses a projected token", "user", user)
		opts = append(opts,
			nats.UserInfo(user, os.Getenv("NATS_PASSWORD")),
			nats.CustomInboxPrefix("_INBOX."+user))
	default:
		tokenOpts, err := lib.KSATokenNATSOptions(tokenFile, natsUser)
		if err != nil {
			return nil, err
		}
		opts = append(opts, tokenOpts...)
	}
	_ = ctx
	return nats.Connect(url, opts...)
}

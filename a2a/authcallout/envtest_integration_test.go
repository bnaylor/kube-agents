package authcallout

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// The same suite as integration_test.go, against a real Kubernetes API server.
//
// What the fake-clientset suite cannot reach, and this can:
//
//   - **The audience binding, non-circularly.** The property that stops every
//     readable ServiceAccount token in the cluster from being a bus credential
//     is currently asserted against a stub that this package also wrote, which
//     proves the validator agrees with itself. Here the tokens are minted by
//     the API server's TokenRequest endpoint and validated by its TokenReview
//     endpoint, so "a token for another audience is refused" is a fact about
//     Kubernetes rather than about the stub.
//   - **The real TokenReview request shape** — that the audiences field is
//     honoured, and that the username comes back in the form the map is keyed
//     on rather than the form this package assumed.
//   - **The informer's real path.** The reflector prefers the streaming
//     WatchList protocol, which the fake clientset does not implement, so the
//     other suite disables it and tests the fallback. A real API server serves
//     it, so this is the only place the production path runs.
//   - **The callout's own RBAC**, exercised as the callout rather than as an
//     admin: it does the TokenReview through its own ServiceAccount.
//
// A pod is a token delivery mechanism here, not the thing under test: what is
// under test in this file is token to TokenReview to grants to a publish that
// is accepted or refused, and none of that needs a kubelet. The pod itself
// became load-bearing with claim narrowing, and is measured separately in
// envtest_session_test.go.
//
// Skipped without KUBEBUILDER_ASSETS; `make test` in k8s-operator installs the
// binaries, and the a2a workflow runs without them.

const (
	envtestNamespace = "kubeagents-system"
	busAudience      = "a2a-bus"

	// The ServiceAccounts the operator's rendered map is keyed on. Taken from
	// the fixture rather than invented, so a rename in the render fails here.
	agentSAName     = "agent"
	provisionSAName = "agent-a2a-provision"
	strangerSAName  = "not-in-the-map"
)

type liveHarness struct {
	k8s       kubernetes.Interface
	namespace string
	nats      *natsserver.Server
	store     *Store
	status    *httptest.Server
}

func startLiveHarness(t *testing.T) *liveHarness {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset; run with the envtest binaries (make -C k8s-operator setup-envtest)")
	}

	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting the API server: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	admin, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	ctx := context.Background()

	if _, err := admin.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: envtestNamespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	for _, sa := range []string{agentSAName, provisionSAName, sessionSAName, strangerSAName, "a2a-callout"} {
		if _, err := admin.CoreV1().ServiceAccounts(envtestNamespace).Create(ctx,
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: sa}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("serviceaccount %s: %v", sa, err)
		}
	}

	// The identity map, as the operator renders it, in the object the callout
	// watches.
	rawMap, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("reading the rendered identity map: %v", err)
	}
	if _, err := admin.CoreV1().ConfigMaps(envtestNamespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-a2a-authmap", Namespace: envtestNamespace},
		Data:       map[string]string{"identities.json": string(rawMap)},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("identity map ConfigMap: %v", err)
	}

	// The callout's real grants, so the TokenReview below is authorized as the
	// callout rather than as an admin.
	if _, err := admin.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "a2a-callout-tokenreview"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "system:auth-delegator"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "a2a-callout", Namespace: envtestNamespace}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("callout ClusterRoleBinding: %v", err)
	}
	if _, err := admin.RbacV1().Roles(envtestNamespace).Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "a2a-callout", Namespace: envtestNamespace},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"configmaps"},
			ResourceNames: []string{"agent-a2a-authmap"}, Verbs: []string{"get", "list", "watch"},
		}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("callout Role: %v", err)
	}
	if _, err := admin.RbacV1().RoleBindings(envtestNamespace).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "a2a-callout", Namespace: envtestNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "a2a-callout"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "a2a-callout", Namespace: envtestNamespace}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("callout RoleBinding: %v", err)
	}

	// The callout's own client, impersonating its ServiceAccount so its RBAC
	// is what authorizes the TokenReview and the informer.
	asCallout := rest.CopyConfig(cfg)
	asCallout.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + envtestNamespace + ":a2a-callout",
	}
	calloutClient, err := kubernetes.NewForConfig(asCallout)
	if err != nil {
		t.Fatalf("callout client: %v", err)
	}

	// The bus, from the operator's rendered config.
	issuerKP, _ := nkeys.CreateAccount()
	issuerSeed, _ := issuerKP.Seed()
	issuerPub, _ := issuerKP.PublicKey()
	xkeyKP, _ := nkeys.CreateCurveKeys()
	xkeySeed, _ := xkeyKP.Seed()
	xkeyPub, _ := xkeyKP.PublicKey()

	rendered, err := os.ReadFile("testdata/rendered-nats.conf")
	if err != nil {
		t.Fatalf("reading the rendered nats.conf: %v", err)
	}
	conf := replaceConfKey(string(rendered), "issuer: ", issuerPub)
	conf = replaceConfKey(conf, "xkey: ", xkeyPub)

	confPath := t.TempDir() + "/nats.conf"
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatalf("writing nats.conf: %v", err)
	}
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatalf("the rendered nats.conf was refused: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true
	opts.Port, opts.Websocket.Port = -1, -1
	opts.StoreDir = t.TempDir()
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		t.Fatal("nats-server not ready")
	}
	t.Cleanup(srv.Shutdown)

	// The callout, against the real API server, with the real informer — no
	// WatchList feature gate disabled here, unlike the fake-client suite.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := NewStore(log)
	ctxWatch, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() {
		_ = store.WatchConfigMap(ctxWatch, calloutClient, envtestNamespace, "agent-a2a-authmap", "identities.json")
	}()
	if err := store.WaitForMap(ctx, 30*time.Second); err != nil {
		t.Fatalf("the informer never delivered the map from a real API server: %v", err)
	}

	validator, err := NewTokenValidator(calloutClient, busAudience)
	if err != nil {
		t.Fatalf("NewTokenValidator: %v", err)
	}
	svc, err := NewService(store, validator, Config{
		IssuerSeed: string(issuerSeed),
		XKeySeed:   string(xkeySeed),
	}, log)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svcConn, err := nats.Connect(srv.ClientURL(), nats.UserInfo(svcUser, svcPass), nats.Name("auth-callout"))
	if err != nil {
		t.Fatalf("the callout could not connect: %v", err)
	}
	t.Cleanup(svcConn.Close)
	if _, err := svc.Subscribe(svcConn); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	status := httptest.NewServer(StatusHandler(store))
	t.Cleanup(status.Close)

	return &liveHarness{k8s: admin, namespace: envtestNamespace, nats: srv, store: store, status: status}
}

// mintToken asks the API server for a real, signed, audience-bound token for a
// ServiceAccount — the same object a projected volume would deliver into a pod.
func (h *liveHarness) mintToken(t *testing.T, sa string, audiences ...string) string {
	t.Helper()
	tr, err := h.k8s.CoreV1().ServiceAccounts(h.namespace).CreateToken(context.Background(), sa,
		&authnv1.TokenRequest{Spec: authnv1.TokenRequestSpec{Audiences: audiences}},
		metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("minting a token for %s: %v", sa, err)
	}
	return tr.Status.Token
}

func (h *liveHarness) connect(t *testing.T, user, token string) (*nats.Conn, chan error) {
	t.Helper()
	violations := make(chan error, 16)
	nc, err := nats.Connect(h.nats.ClientURL(),
		nats.Token(token),
		nats.CustomInboxPrefix("_INBOX."+user),
		nats.Name(user),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { violations <- e }),
	)
	if err != nil {
		t.Fatalf("%s could not connect with a real ServiceAccount token: %v", user, err)
	}
	t.Cleanup(nc.Close)
	return nc, violations
}

// DoD, with a token the cluster actually minted and actually validated.
func TestLiveARealServiceAccountTokenGetsItsMappedGrants(t *testing.T) {
	h := startLiveHarness(t)
	nc, violations := h.connect(t, "agent", h.mintToken(t, agentSAName, busAudience))

	if publishRefused(t, nc, violations, "a2a.topics.shared.blueprint") {
		t.Error("the agent was refused a topic its map entry grants")
	}
	if !publishRefused(t, nc, violations, "a2a.tasks.platform.t1.in") {
		t.Error("the agent reached the task plane; its principal grants none of it")
	}
	if !nc.IsConnected() {
		t.Error("the connection closed on a permissions violation")
	}
}

func TestLiveTwoServiceAccountsGetDifferentGrants(t *testing.T) {
	h := startLiveHarness(t)
	agent, agentViolations := h.connect(t, "agent", h.mintToken(t, agentSAName, busAudience))
	provision, provisionViolations := h.connect(t, "provision", h.mintToken(t, provisionSAName, busAudience))

	// Both are granted the provisioned topics.
	if publishRefused(t, agent, agentViolations, "a2a.topics.shared.annotations") {
		t.Error("the agent was refused a granted topic")
	}
	if publishRefused(t, provision, provisionViolations, "a2a.topics.shared.annotations") {
		t.Error("provision was refused a granted topic")
	}
	// Only the agent publishes heartbeats. Two distinct grant sets, resolved
	// from two distinct cluster identities.
	if publishRefused(t, agent, agentViolations, "agents.hb.claude-code.owner.session") {
		t.Error("the agent was refused the heartbeat subject its entry grants")
	}
	if !publishRefused(t, provision, provisionViolations, "agents.hb.claude-code.owner.session") {
		t.Error("provision reached the heartbeat plane; the two grant sets are not distinct")
	}
}

// The audience binding, proven against Kubernetes rather than against a stub
// this package also wrote.
//
// Without it, a TokenReview validates against the API server's own audience,
// which every ordinary pod's default ServiceAccount token carries — so any
// token readable anywhere in the cluster would authenticate to the bus as
// whoever it belongs to. This is the single assertion that says otherwise, and
// until it ran against a real issuer it was circular.
func TestLiveATokenForAnotherAudienceIsRefused(t *testing.T) {
	h := startLiveHarness(t)

	cases := []struct {
		name      string
		audiences []string
	}{
		{
			// THE case. Minting with no audience yields the API server's
			// own default — which is exactly what every ordinary pod's
			// default ServiceAccount token carries. If the bus accepted
			// this, any token readable anywhere in the cluster would be a
			// bus credential for whoever it belongs to.
			//
			// It has to be discovered from the server rather than written
			// down: an earlier version of this test named a plausible
			// default as a literal, which envtest does not use, so the
			// token was refused for the wrong reason and the test passed
			// with the audience binding deleted outright. Asking for the
			// default is what makes it a fact about Kubernetes.
			name:      "the API server's own default audience, as every pod's default token carries",
			audiences: nil,
		},
		{
			name:      "an unrelated service's audience",
			audiences: []string{"some-other-service"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := h.mintToken(t, agentSAName, tc.audiences...)
			nc, err := nats.Connect(h.nats.ClientURL(), nats.Token(token), nats.Name("wrong-audience"))
			if err == nil {
				nc.Close()
				t.Fatalf("a token minted for audiences %v authenticated to the bus", tc.audiences)
			}
			if !strings.Contains(err.Error(), "Authorization Violation") {
				t.Errorf("connect error = %v, want an Authorization Violation", err)
			}
		})
	}

	// The control: the same ServiceAccount, correctly audienced, connects. It
	// is what stops the two cases above passing because the identity is
	// unusable for some unrelated reason.
	t.Run("the control: the same identity with the right audience connects", func(t *testing.T) {
		nc, err := nats.Connect(h.nats.ClientURL(),
			nats.Token(h.mintToken(t, agentSAName, busAudience)),
			nats.CustomInboxPrefix("_INBOX.agent"), nats.Name("agent"))
		if err != nil {
			t.Fatalf("the correctly-audienced token was refused: %v", err)
		}
		nc.Close()
	})
}

// A real, valid, correctly-audienced token for a ServiceAccount this deployment
// has no entry for. The cluster vouches for it and the bus still refuses it.
func TestLiveAnUnmappedServiceAccountIsRefused(t *testing.T) {
	h := startLiveHarness(t)
	token := h.mintToken(t, strangerSAName, busAudience)

	nc, err := nats.Connect(h.nats.ClientURL(), nats.Token(token), nats.Name("stranger"))
	if err == nil {
		nc.Close()
		t.Fatal("a ServiceAccount absent from the identity map connected")
	}
	if !strings.Contains(err.Error(), "Authorization Violation") {
		t.Errorf("connect error = %v, want an Authorization Violation", err)
	}
}

// The map version, read off the running callout rather than off the render.
func TestLiveTheServedMapVersionIsObservable(t *testing.T) {
	h := startLiveHarness(t)

	resp, err := http.Get(h.status.URL + StatusPath)
	if err != nil {
		t.Fatalf("GET %s: %v", StatusPath, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// The version the operator rendered into the fixture, which is what the
	// operator would be comparing against.
	rendered, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("reading the rendered map: %v", err)
	}
	want, err := ParseIdentityMap(rendered)
	if err != nil {
		t.Fatalf("parsing the rendered map: %v", err)
	}
	if !strings.Contains(string(body), want.Version) {
		t.Errorf("status does not report the rendered version %q:\n%s", want.Version, body)
	}
	for _, user := range want.Users() {
		if !strings.Contains(string(body), user) {
			t.Errorf("status does not name the served user %q", user)
		}
	}

	ready, err := http.Get(h.status.URL + ReadyPath)
	if err != nil {
		t.Fatalf("GET %s: %v", ReadyPath, err)
	}
	defer ready.Body.Close()
	if ready.StatusCode != http.StatusOK {
		t.Errorf("readiness = %d while serving a map, want 200", ready.StatusCode)
	}
	readyBody, _ := io.ReadAll(ready.Body)
	if !strings.Contains(string(readyBody), want.Version) {
		t.Errorf("readiness body %q does not name the served version", readyBody)
	}
}

// The informer against a real API server, on the WatchList path the fake
// clientset could not serve: a map edited in the API server reaches the callout,
// and the grants a new connection receives change with it.
func TestLiveAMapEditReachesTheCalloutAndChangesNewGrants(t *testing.T) {
	h := startLiveHarness(t)
	before := h.store.Version()

	cm, err := h.k8s.CoreV1().ConfigMaps(h.namespace).Get(context.Background(), "agent-a2a-authmap", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get map: %v", err)
	}
	// Narrow the agent: drop its heartbeat publish.
	cm.Data["identities.json"] = strings.Replace(cm.Data["identities.json"],
		"\"agents.hb.>\",\n", "", 1)
	cm.Data["identities.json"] = strings.Replace(cm.Data["identities.json"],
		before, before+"-narrowed", 1)
	if _, err := h.k8s.CoreV1().ConfigMaps(h.namespace).Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update map: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for h.store.Version() == before {
		if time.Now().After(deadline) {
			t.Fatalf("the informer did not deliver the edit; still serving %q", before)
		}
		time.Sleep(50 * time.Millisecond)
	}

	nc, violations := h.connect(t, "agent", h.mintToken(t, agentSAName, busAudience))
	if !publishRefused(t, nc, violations, "agents.hb.claude-code.owner.session") {
		t.Error("a connection made after the narrowing still holds the removed grant")
	}
}

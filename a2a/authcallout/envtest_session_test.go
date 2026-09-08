package authcallout

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// The measurement the whole of claim narrowing rests on, taken against a real
// API server rather than assumed from the documentation.
//
// Everything in session.go is derived from one claim: that a token minted into
// a pod's projected volume comes back from TokenReview carrying that pod's name
// and UID, written by the authenticator and not by the client, and that the
// claim stops being honoured when the pod goes. Neither half is observable
// against a fake clientset — the fake returns whatever this package's own stub
// puts in Extra, which proves only that the stub and the parser agree.
//
// It also fixes the number the design has to live with. Deletion is not
// instant: the API server caches successful authentications, and measured here
// on 1.36 the token keeps working for about 10 seconds after its pod is gone
// (the refusal then reads "service account token has been invalidated"). That
// is the window in which a reaped session still holds a working bus credential,
// and a design that assumed it was zero would be wrong about its own
// revocation story. It is bounded and it is small, but it is not nothing, and
// it is why reaping a pod is a revocation with a delay rather than a fence.
//
// Skipped without KUBEBUILDER_ASSETS, like the rest of the envtest suite.

// podClaimHarness is deliberately smaller than liveHarness: no NATS, no map, no
// callout. What is under test here is Kubernetes' behaviour, and adding a bus
// to the picture would mean a failure could be either party's.
type podClaimHarness struct {
	k8s       kubernetes.Interface
	namespace string
	validator *TokenValidator
}

func startPodClaimHarness(t *testing.T) *podClaimHarness {
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
	if _, err := admin.CoreV1().ServiceAccounts(envtestNamespace).Create(ctx,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: sessionSAName}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("serviceaccount: %v", err)
	}

	v, err := NewTokenValidator(admin, busAudience)
	if err != nil {
		t.Fatalf("NewTokenValidator: %v", err)
	}
	return &podClaimHarness{k8s: admin, namespace: envtestNamespace, validator: v}
}

const sessionSAName = "agent-a2a-session"

// createPod makes the object a token can be bound to. envtest runs no kubelet,
// so the pod stays Pending forever — which is all this needs, because the bound
// object reference is checked against the API object and never against a
// running container.
func (h *podClaimHarness) createPod(t *testing.T, name string) *corev1.Pod {
	t.Helper()
	pod, err := h.k8s.CoreV1().Pods(h.namespace).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.namespace},
		Spec: corev1.PodSpec{
			ServiceAccountName: sessionSAName,
			Containers:         []corev1.Container{{Name: "adapter", Image: "worker-adapter:test"}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating pod %s: %v", name, err)
	}
	return pod
}

// mintPodToken asks for exactly what the spawner's projected volume asks for:
// an audience-bound token whose bound object is this pod.
func (h *podClaimHarness) mintPodToken(t *testing.T, pod *corev1.Pod) string {
	t.Helper()
	tr, err := h.k8s.CoreV1().ServiceAccounts(h.namespace).CreateToken(context.Background(), sessionSAName,
		&authnv1.TokenRequest{Spec: authnv1.TokenRequestSpec{
			Audiences: []string{busAudience},
			BoundObjectRef: &authnv1.BoundObjectReference{
				Kind: "Pod", APIVersion: "v1", Name: pod.Name, UID: pod.UID,
			},
		}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("minting a pod-bound token: %v", err)
	}
	return tr.Status.Token
}

// The claim itself. If this fails, session.go's entire derivation is built on a
// field the API server does not send, and no amount of grant enumeration helps.
func TestLiveAPodBoundTokenCarriesThePodClaim(t *testing.T) {
	h := startPodClaimHarness(t)
	pod := h.createPod(t, "chat-otter-1a2b")

	att, err := h.validator.Validate(context.Background(), h.mintPodToken(t, pod))
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if want := ServiceAccountPrefix + envtestNamespace + ":" + sessionSAName; att.ServiceAccount != want {
		t.Errorf("serviceAccount = %q, want %q", att.ServiceAccount, want)
	}
	if att.PodName != pod.Name {
		t.Errorf("pod name = %q, want %q", att.PodName, pod.Name)
	}
	if att.PodUID != string(pod.UID) {
		t.Errorf("pod uid = %q, want %q", att.PodUID, pod.UID)
	}

	// The grants that name is turned into, so this test also pins that the
	// real attested name survives into a real subject rather than into an
	// escaped or truncated one.
	if got := sessionGrants(att.PodName); !containsString(got.Publish, "a2a.tasks.chat-otter-1a2b.*.events") {
		t.Errorf("grants derived from the attested pod = %+v, want the pod's own events subject", got)
	}
}

// The negative control, and the reason the callout can refuse rather than guess:
// an unbound token for the same ServiceAccount authenticates perfectly well and
// carries no pod at all. Without this, "no pod claim" could equally mean "this
// build does not read the field".
func TestLiveATokenBoundToNoPodCarriesNoPodClaim(t *testing.T) {
	h := startPodClaimHarness(t)

	tr, err := h.k8s.CoreV1().ServiceAccounts(h.namespace).CreateToken(context.Background(), sessionSAName,
		&authnv1.TokenRequest{Spec: authnv1.TokenRequestSpec{Audiences: []string{busAudience}}},
		metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("minting an unbound token: %v", err)
	}
	att, err := h.validator.Validate(context.Background(), tr.Status.Token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if att.PodName != "" || att.PodUID != "" {
		t.Errorf("an unbound token reported pod %q/%q; the binding is not what it appears to be", att.PodName, att.PodUID)
	}
	// And the callout refuses it, which is what stops a session pod from
	// being handed an unbound token and getting an empty grant set.
	if err := validSessionName(att.PodName); err == nil {
		t.Error("a claim-less token passed the session name check")
	}
}

// The binding is the API server's, not the caller's: a bound object reference
// naming the right pod with the wrong UID is refused at mint. This is what
// makes a pod name unforgeable rather than merely attested.
func TestLiveAPodBoundTokenWithAMismatchedUIDIsRefusedAtMint(t *testing.T) {
	h := startPodClaimHarness(t)
	pod := h.createPod(t, "chat-badger-9f9f")

	_, err := h.k8s.CoreV1().ServiceAccounts(h.namespace).CreateToken(context.Background(), sessionSAName,
		&authnv1.TokenRequest{Spec: authnv1.TokenRequestSpec{
			Audiences: []string{busAudience},
			BoundObjectRef: &authnv1.BoundObjectReference{
				Kind: "Pod", APIVersion: "v1", Name: pod.Name, UID: "00000000-0000-0000-0000-000000000000",
			},
		}}, metav1.CreateOptions{})
	if err == nil {
		t.Fatal("the API server minted a token bound to a pod UID that does not exist")
	}
}

// Revocation, and the number the design has to live with.
//
// Deleting the pod must stop its token authenticating — that is what makes a
// reaped session's credential worthless, and it is the property that lets the
// spawner hand out a 3600s token without that being a 3600s exposure. The API
// server caches successful authentications briefly, so the refusal is not
// instant; this measures the window rather than asserting it away.
func TestLiveDeletingThePodStopsItsTokenAuthenticating(t *testing.T) {
	h := startPodClaimHarness(t)
	pod := h.createPod(t, "chat-heron-3c3c")
	token := h.mintPodToken(t, pod)

	if _, err := h.validator.Validate(context.Background(), token); err != nil {
		t.Fatalf("the token did not work before the pod was deleted: %v", err)
	}

	// Zero grace: a session pod is reaped, not drained, and envtest has no
	// kubelet to confirm a graceful delete anyway.
	zero := int64(0)
	if err := h.k8s.CoreV1().Pods(h.namespace).Delete(context.Background(), pod.Name,
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatalf("deleting the pod: %v", err)
	}

	start := time.Now()
	deadline := start.Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := h.validator.Validate(context.Background(), token); err != nil {
			lastErr = err
			t.Logf("the token stopped authenticating %v after the pod was deleted: %v", time.Since(start).Round(time.Millisecond), err)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr == nil {
		t.Fatalf("the token still authenticated %v after its pod was deleted; a reaped session keeps a working bus credential", time.Since(start))
	}
	if !strings.Contains(lastErr.Error(), "not authenticated") {
		t.Errorf("refusal was %v, want it to come from the authenticator rather than from a call failure", lastErr)
	}
}

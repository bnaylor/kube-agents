package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// RouteSession is the DefaultAddressee sentinel that switches a conversation
// from a fixed addressee (the Hermes bridge's "platform") to a session pod
// of its own. Flipping to it — plus A2A_SPAWN_SESSIONS=true — is the W4
// switch; a setting, not surgery (retarget 8/26).
const RouteSession = "session"

// spawner is the session-pod half of the lifecycle. It stays dark behind
// SpawnSessions until W4's worker image exists; the gateway pod gets its
// service-account token with that change, not before.
type spawner interface {
	// Spawn creates the session pod for a task and returns the pod name.
	Spawn(ctx context.Context, rec *SessionRecord, taskID, primer string) (string, error)
	// Delete removes a pod (reap).
	Delete(ctx context.Context, podName string) error
	// TerminalOrphans lists pods in a terminal phase, with the task identity
	// their annotations carry (sweep's scan).
	TerminalOrphans(ctx context.Context) ([]orphanPod, error)
}

type orphanPod struct {
	PodName       string
	SessionKey    string
	Addressee     string
	TaskID        string
	ContextID     string
	CorrelationID string
}

// mintSessionName gives an incarnation its bus session name,
// <profile>-<animal> per the house ruling, minted FRESH per incarnation —
// reaping and respawning changes the pod and the bus session name;
// contextId is what persists (gateway design). The suffix is wide enough
// that two conversations can't plausibly collide onto one addressee. W5
// owns the canonical animal list (stolen from the demo); this short one
// keeps the dark path honest until integration.
func mintSessionName(profile string) string {
	animals := []string{"otter", "badger", "heron", "lynx", "marten", "puffin", "stoat", "vole"}
	return fmt.Sprintf("%s-%s-%s", profile, animals[int(time.Now().UnixNano())%len(animals)], randHex(4))
}

const (
	labelPartOf = "app.kubernetes.io/part-of"
	partOfValue = "a2a-next"
	labelRole   = "app.kubernetes.io/component"
	sessionRole = "a2a-session"
	annoTask    = "a2a.kubeagents.dev/task-id"
	annoContext = "a2a.kubeagents.dev/context-id"
	annoCorr    = "a2a.kubeagents.dev/correlation-id"
	annoAddr    = "a2a.kubeagents.dev/addressee"
	annoConvo   = "a2a.kubeagents.dev/session-key"
	annoPrimer  = "a2a.kubeagents.dev/rehydration-primer"
)

func activeCorrelation(rec *SessionRecord) string {
	if rec.ActiveTask != nil {
		return rec.ActiveTask.CorrelationID
	}
	return ""
}

// podSpawner is the client-go implementation.
type podSpawner struct {
	cfg    *Config
	client kubernetes.Interface
	log    *slog.Logger
}

func newPodSpawner(cfg *Config, log *slog.Logger) (*podSpawner, error) {
	rc, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, err
	}
	return &podSpawner{cfg: cfg, client: cs, log: log}, nil
}

// Spawn creates the session pod: the demo's reference worker shape — no
// ambient k8s credentials, scratch on emptyDir, 250m/512Mi requests —
// running the headless harness behind the worker adapter (W4's image).
// Model auth is Workload Identity against Vertex; no per-pod API keys.
func (s *podSpawner) Spawn(ctx context.Context, rec *SessionRecord, taskID, primer string) (string, error) {
	name := rec.BusSession
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.cfg.Namespace,
			Labels: map[string]string{
				labelPartOf: partOfValue,
				labelRole:   sessionRole,
			},
			Annotations: map[string]string{
				annoTask:    taskID,
				annoContext: rec.ContextID,
				annoCorr:    activeCorrelation(rec),
				annoAddr:    rec.Addressee,
				annoConvo:   rec.Key,
				// The rehydration primer rides the pod until W4's adapter
				// grows a first-input path for it; bounded well under the
				// object annotation budget.
				annoPrimer: truncateRunes(primer, 8192),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: ptr.To(false),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptr.To(true),
				RunAsUser:    ptr.To(int64(1000)),
			},
			Containers: []corev1.Container{{
				Name:  "worker",
				Image: s.cfg.WorkerImage,
				Env: []corev1.EnvVar{
					// The worker env contract (launch-card constants):
					// TASK_ID/PROFILE/NATS_URL. PROFILE names the
					// AgentProfile — the addressee is the bus session name,
					// which is not a profile. Bus creds ride alongside; how
					// W4 wants them delivered is its call to revise.
					{Name: "TASK_ID", Value: taskID},
					{Name: "PROFILE", Value: rec.Profile},
					{Name: "NATS_URL", Value: s.cfg.NATSURL},
					{Name: "NATS_USER", Value: "worker"},
					{Name: "NATS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: s.cfg.NATSCredsSecret},
						Key:                  "worker-password",
					}}},
					{Name: "A2A_SESSION", Value: rec.BusSession},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("250m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "scratch", MountPath: "/scratch"}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
			Volumes: []corev1.Volume{{
				Name:         "scratch",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	}
	created, err := s.client.CoreV1().Pods(s.cfg.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		// AlreadyExists included: session names are minted per incarnation,
		// so a name collision means the mint raced a terminating predecessor
		// — surface it rather than adopt a pod that is about to vanish.
		return "", err
	}
	return created.Name, nil
}

func (s *podSpawner) Delete(ctx context.Context, podName string) error {
	err := s.client.CoreV1().Pods(s.cfg.Namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (s *podSpawner) TerminalOrphans(ctx context.Context) ([]orphanPod, error) {
	pods, err := s.client.CoreV1().Pods(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", labelPartOf, partOfValue, labelRole, sessionRole),
	})
	if err != nil {
		return nil, err
	}
	var out []orphanPod
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			continue
		}
		out = append(out, orphanPod{
			PodName:       p.Name,
			SessionKey:    p.Annotations[annoConvo],
			Addressee:     p.Annotations[annoAddr],
			TaskID:        p.Annotations[annoTask],
			ContextID:     p.Annotations[annoContext],
			CorrelationID: p.Annotations[annoCorr],
		})
	}
	return out, nil
}

// ensureSessionPod spawns (or rehydrates) the session's incarnation for a
// new task. Only called on session-addressed routes.
func (g *Gateway) ensureSessionPod(ctx context.Context, rec *SessionRecord, taskID string) {
	if rec.PodName != "" {
		return
	}
	primer := g.buildRehydrationPrimer(ctx, rec)
	podName, err := g.spawner.Spawn(ctx, rec, taskID, primer)
	if err != nil {
		g.log.Error("session pod spawn failed", "session", rec.Key, "err", err)
		return
	}
	rec.PodName = podName
	g.log.Info("spawned session pod", "session", rec.Key, "pod", podName, "task", taskID)
}

// sweepLoop is the gateway's half of the orphaned-task answer: it is the
// supervisor for sessions it spawned. A pod in a terminal phase whose task
// never emitted a final event gets a terminal failed published by the
// gateway, then the pod is deleted. The synthesized event carries the
// gateway's identity in from, so replay always distinguishes "the worker
// said failed" from "the supervisor declared it dead". (The dispatcher's
// janitor is the other half, for profile-addressed tasks — stage 3.)
func (g *Gateway) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.sweepOnce(ctx)
		}
	}
}

func (g *Gateway) sweepOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	orphans, err := g.spawner.TerminalOrphans(ctx)
	if err != nil {
		g.log.Error("sweep: pod list failed", "err", err)
		return
	}
	for _, o := range orphans {
		if o.TaskID == "" || o.Addressee == "" {
			g.log.Warn("sweep: terminal pod without task annotations; deleting", "pod", o.PodName)
			_ = g.spawner.Delete(ctx, o.PodName)
			g.releaseIncarnation(ctx, o)
			continue
		}
		task, err := g.client.TasksGet(ctx, o.Addressee, o.TaskID)
		if err == nil && task.Final {
			_ = g.spawner.Delete(ctx, o.PodName) // clean exit; nothing owed
			g.releaseIncarnation(ctx, o)
			continue
		}
		if err := g.publishSupervisorFailed(ctx, o); err != nil {
			g.log.Error("sweep: terminal failed publish failed", "task", o.TaskID, "err", err)
			continue // keep the pod as evidence until the event lands
		}
		g.log.Warn("sweep: declared orphaned task failed", "task", o.TaskID, "pod", o.PodName)
		_ = g.spawner.Delete(ctx, o.PodName)
		g.releaseIncarnation(ctx, o)
	}
}

// releaseIncarnation clears the session record's pod binding after sweep
// removes a dead pod — otherwise ensureSessionPod sees a PodName forever
// and an active conversation (which keeps resetting the idle clock, so reap
// never fires) has no executor and no way to get one.
func (g *Gateway) releaseIncarnation(ctx context.Context, o orphanPod) {
	if o.SessionKey == "" {
		return
	}
	l := g.lockSession(o.SessionKey)
	l.Lock()
	defer l.Unlock()
	rec, err := g.reg.Get(ctx, o.SessionKey)
	if err != nil || rec == nil || rec.PodName != o.PodName {
		return
	}
	rec.PodName = ""
	if err := g.reg.Put(ctx, rec); err != nil {
		g.log.Error("sweep: record release failed", "session", o.SessionKey, "err", err)
	}
}

// publishSupervisorFailed writes the terminal failed for a task whose
// executor died without one.
func (g *Gateway) publishSupervisorFailed(ctx context.Context, o orphanPod) error {
	corr := o.CorrelationID
	if corr == "" {
		// The annotation should always carry it; a missing one still gets a
		// terminal event, honestly labeled.
		corr = "corr-supervisor-" + randHex(6)
	}
	payload, err := json.Marshal(lib.StatusUpdate{
		TaskID:    o.TaskID,
		ContextID: o.ContextID,
		Status: lib.TaskStatus{
			State: lib.StateFailed,
			Message: &lib.Message{
				Role:      "agent",
				MessageID: "msg-" + randHex(8),
				Parts:     []lib.Part{{Kind: "text", Text: "session pod exited without a terminal event; declared failed by its supervisor"}},
			},
		},
		Final: true,
	})
	if err != nil {
		return err
	}
	env, err := lib.NewStatusUpdateEnvelope(gatewayParty, o.TaskID, o.ContextID, corr, payload)
	if err != nil {
		return err
	}
	return g.client.Publish(ctx, lib.TaskEventsSubject(o.Addressee, o.TaskID), env)
}

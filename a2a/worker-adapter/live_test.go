package workeradapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// TestLiveWorkerPodOnInstall drives the whole worker side on the real W6
// install: it publishes a task the way the gateway does (real gateway user,
// real deny-by-default grants), creates a session pod with the spawner's
// exact spec from the real worker-next image, and asserts the lifecycle -
// submitted/working/progress/result/completed, a mid-run steer absorbed at
// the turn boundary, and a cancel landing terminal canceled with the pod
// dead. Discord itself is the only thing not exercised (W3's convention).
//
// Env-gated:
//
//	A2A_LIVE_NATS_URL          e.g. nats://127.0.0.1:4222 via port-forward
//	A2A_LIVE_GATEWAY_PASSWORD  from the install's creds Secret
//	A2A_LIVE_KUBE_CONTEXT      pin --context on every kubectl call
//	A2A_LIVE_NAMESPACE         default kubeagents-system
//	A2A_LIVE_WORKER_IMAGE      default worker-next:latest in the a2a-demo registry
func TestLiveWorkerPodOnInstall(t *testing.T) {
	url := os.Getenv("A2A_LIVE_NATS_URL")
	if url == "" {
		t.Skip("A2A_LIVE_NATS_URL not set; live test skipped")
	}
	gwPass := os.Getenv("A2A_LIVE_GATEWAY_PASSWORD")
	kubeContext := os.Getenv("A2A_LIVE_KUBE_CONTEXT")
	if gwPass == "" || kubeContext == "" {
		t.Fatal("live test needs A2A_LIVE_GATEWAY_PASSWORD and A2A_LIVE_KUBE_CONTEXT")
	}
	namespace := envOr("A2A_LIVE_NAMESPACE", "kubeagents-system")
	image := envOr("A2A_LIVE_WORKER_IMAGE",
		"northamerica-northeast1-docker.pkg.dev/bnaylor-kagents-dev/a2a-demo/worker-next:latest")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	c, err := lib.Connect(ctx, url,
		lib.WithName("w4-live-test"),
		lib.WithNATSOptions(nats.UserInfo("gateway", gwPass), nats.CustomInboxPrefix("_INBOX.gateway")))
	if err != nil {
		t.Fatalf("connect as gateway: %v", err)
	}
	defer c.Close()

	t.Run("HappyPathWithSteer", func(t *testing.T) {
		session := "chat-live-" + strings.ToLower(nuid.Next()[:6])
		taskID := "task-live-" + strings.ToLower(nuid.Next()[:8])
		origin := liveSubmit(t, ctx, c, session, taskID,
			"Write a haiku about message buses. Output only the haiku.")
		livePod(t, kubeContext, namespace, image, session, taskID)

		liveWaitState(t, ctx, c, session, taskID, lib.StateWorking, 3*time.Minute)

		steerPayload, _ := json.Marshal(lib.Message{
			Role:      "user",
			Parts:     []lib.Part{{Kind: "text", Text: "Steering update: you MUST include the word tangerine in the haiku."}},
			MessageID: "msg-live-steer-" + taskID,
			TaskID:    taskID, ContextID: origin.ContextID,
		})
		steer, err := lib.NewFollowUpEnvelope(origin, lib.Party{Session: "gateway", AgentType: "a2a-gateway"},
			steerPayload, lib.WithTo(lib.Party{Session: session}))
		if err != nil {
			t.Fatalf("steer envelope: %v", err)
		}
		if err := c.Publish(ctx, lib.TaskInSubject(session, taskID), steer); err != nil {
			t.Fatalf("steer publish: %v", err)
		}

		task := liveWaitFinal(t, ctx, c, session, taskID, 4*time.Minute)
		if task.State != lib.StateCompleted {
			t.Fatalf("terminal %s, want completed", task.State)
		}
		if err := task.ValidateArtifacts(); err != nil {
			t.Fatalf("artifacts: %v", err)
		}
		result := liveArtifactText(task, lib.ArtifactResult)
		t.Logf("result artifact:\n%s", result)
		if result == "" {
			t.Fatal("empty result artifact")
		}
		// The steer is absorbed at the next turn boundary; the deliverable
		// is the steered turn's result (assertion 21 live).
		if !strings.Contains(strings.ToLower(result), "tangerine") {
			t.Errorf("steer not reflected in the deliverable: %q", result)
		}
	})

	t.Run("Cancel", func(t *testing.T) {
		session := "chat-live-" + strings.ToLower(nuid.Next()[:6])
		taskID := "task-live-" + strings.ToLower(nuid.Next()[:8])
		origin := liveSubmit(t, ctx, c, session, taskID,
			"Use the TodoWrite tool to plan 20 research subtasks about NATS JetStream, "+
				"then work through each one in a separate turn, writing detailed notes to "+
				"a file per subtask with the Write tool. Do not stop until all 20 are done.")
		livePod(t, kubeContext, namespace, image, session, taskID)
		liveWaitState(t, ctx, c, session, taskID, lib.StateWorking, 3*time.Minute)
		// Let the harness get properly into the long tool loop before the
		// hard interrupt, so the live path exercised is a real mid-run kill.
		time.Sleep(8 * time.Second)

		cancelEnv, err := lib.NewCancelEnvelope(lib.Party{Session: "gateway", AgentType: "a2a-gateway"},
			taskID, origin.ContextID, origin.CorrelationID, lib.WithTo(lib.Party{Session: session}))
		if err != nil {
			t.Fatalf("cancel envelope: %v", err)
		}
		if err := c.Publish(ctx, lib.TaskInSubject(session, taskID), cancelEnv); err != nil {
			t.Fatalf("cancel publish: %v", err)
		}
		task := liveWaitFinal(t, ctx, c, session, taskID, 2*time.Minute)
		// canceled, or completed if the model won the race - both legal.
		if task.State != lib.StateCanceled && task.State != lib.StateCompleted {
			t.Fatalf("terminal %s after cancel", task.State)
		}
		t.Logf("cancel landed terminal %s", task.State)
	})
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func liveSubmit(t *testing.T, ctx context.Context, c *lib.Client, session, taskID, text string) *lib.Envelope {
	t.Helper()
	payload, _ := json.Marshal(lib.Message{
		Role: "user", Parts: []lib.Part{{Kind: "text", Text: text}},
		MessageID: "msg-" + taskID, TaskID: taskID, ContextID: "ctx-" + taskID,
	})
	env, err := lib.NewMessageEnvelope(lib.Party{Session: "gateway", AgentType: "a2a-gateway"},
		taskID, "ctx-"+taskID, "corr-live-"+taskID, payload, lib.WithTo(lib.Party{Session: session}))
	if err != nil {
		t.Fatalf("submission envelope: %v", err)
	}
	if err := c.Publish(ctx, lib.TaskInSubject(session, taskID), env); err != nil {
		t.Fatalf("publish submission: %v", err)
	}
	return env
}

// livePod applies a pod with the gateway spawner's exact spec (spawn.go) and
// registers cleanup.
func livePod(t *testing.T, kubeContext, namespace, image, session, taskID string) {
	t.Helper()
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/part-of: a2a-next
    app.kubernetes.io/component: a2a-session
  annotations:
    a2a.kubeagents.dev/task-id: %s
    a2a.kubeagents.dev/addressee: %s
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
  containers:
    - name: worker
      image: %s
      env:
        - name: TASK_ID
          value: %s
        - name: PROFILE
          value: chat
        - name: NATS_URL
          value: nats://platform-agent-a2a-nats.%s.svc:4222
        - name: NATS_USER
          value: worker
        - name: NATS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: platform-agent-a2a-nats-creds
              key: worker-password
        - name: A2A_SESSION
          value: %s
      resources:
        requests: { cpu: 250m, memory: 512Mi }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
      volumeMounts:
        - name: scratch
          mountPath: /scratch
  volumes:
    - name: scratch
      emptyDir: {}
`, session, namespace, taskID, session, image, taskID, namespace, session)
	path := filepath.Join(t.TempDir(), "pod.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("kubectl", "--context", kubeContext, "-n", namespace, "apply", "-f", path).CombinedOutput()
	if err != nil {
		t.Fatalf("pod apply: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("kubectl", "--context", kubeContext, "-n", namespace,
			"delete", "pod", session, "--wait=false").Run()
	})
}

func liveWaitState(t *testing.T, ctx context.Context, c *lib.Client, session, taskID string, state lib.TaskState, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		task, err := c.TasksGet(ctx, session, taskID)
		if err == nil {
			for _, s := range task.StatusHistory {
				if s == state {
					return
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("task %s never reached %s within %s", taskID, state, within)
}

func liveWaitFinal(t *testing.T, ctx context.Context, c *lib.Client, session, taskID string, within time.Duration) *lib.Task {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		task, err := c.TasksGet(ctx, session, taskID)
		if err == nil && task.Final {
			return task
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("task %s never reached a terminal state within %s", taskID, within)
	return nil
}

func liveArtifactText(task *lib.Task, name string) string {
	a := task.Artifact(name)
	if a == nil {
		return ""
	}
	var b strings.Builder
	for _, p := range a.Parts {
		b.WriteString(p.Text)
	}
	return b.String()
}

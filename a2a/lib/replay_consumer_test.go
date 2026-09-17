package lib

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The consumer TasksGet creates on TASKS must be gone when TasksGet returns,
// and where the principal cannot delete it, gone within
// EphemeralConsumerInactiveThreshold (#1739). Both are measured on a real
// server against a gateway-shaped permission set, because the answer depends
// on the grant: the delete is a $JS.API.CONSUMER.DELETE.TASKS.<name> publish,
// and a principal without it gets a refusal and no reply.
const (
	// replayDeleteWithin is how long the explicit delete gets to land. It is
	// well inside EphemeralConsumerInactiveThreshold so that a pass here is
	// the delete and not the threshold.
	replayDeleteWithin = 2 * time.Second
	// thresholdReapWithin is how long the server gets to reap a consumer by
	// its inactive threshold once the client is gone: the threshold itself
	// plus the server's scan interval and slack.
	thresholdReapWithin = 3 * EphemeralConsumerInactiveThreshold
	// replayBurst is how many tasks/get calls the burst case makes.
	replayBurst = 20

	// The TASKS stream as the provision script creates it, for the flags
	// that matter here: allow_direct (the replay horizon is DIRECT.GET, the
	// subject the grant names, not STREAM.MSG.GET), limits retention with
	// discard old, and the 64-consumer floor the leak counts against.
	replayStreamSubjects     = "a2a.tasks.>"
	replayStreamMaxAge       = 72 * time.Hour
	replayStreamMaxConsumers = 64
	// replayAdminTimeout bounds each admin-side call (provision, list).
	replayAdminTimeout = 5 * time.Second

	replayAdminUser    = "admin"
	replayGatewayUser  = "gateway"
	replayNoDeleteUser = "gateway-nodelete"
	replayPassword     = "pw"
)

// replayGrant is a gateway-shaped grant: the JetStream API subjects TasksGet
// emits on TASKS, enumerated per stream and verb the way the operator renders
// the worker's grant today and gke-labs/kube-agents#1672 proposes for the
// gateway, plus the ack, flow-control and inbox subjects beside them in the
// identity. withDelete adds the one subject that proposal withholds and this
// change needs.
func replayGrant(user string, withDelete bool) *natsserver.Permissions {
	publish := []string{
		"$JS.API.STREAM.INFO.TASKS",
		"$JS.API.CONSUMER.CREATE.TASKS.>",
		"$JS.API.CONSUMER.MSG.NEXT.TASKS.*",
		"$JS.API.DIRECT.GET.TASKS.>",
		"$JS.ACK.TASKS.>",
		"$JS.FC.>",
		"_INBOX." + user + ".>",
	}
	if withDelete {
		publish = append(publish, "$JS.API.CONSUMER.DELETE.TASKS.*")
	}
	return &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: publish},
		Subscribe: &natsserver.SubjectPermission{Allow: []string{"_INBOX." + user + ".>"}},
	}
}

// startPermissionedServer runs a JetStream server whose users carry the grants
// above. admin is unrestricted: it provisions the stream, publishes the task
// and reads the consumer list back, the way seed and web do on an install.
func startPermissionedServer(t *testing.T) *natsserver.Server {
	t.Helper()
	s := runJetStreamServer(t, -1, t.TempDir(), func(o *natsserver.Options) {
		o.Users = []*natsserver.User{
			{Username: replayAdminUser, Password: replayPassword},
			{Username: replayGatewayUser, Password: replayPassword, Permissions: replayGrant(replayGatewayUser, true)},
			{Username: replayNoDeleteUser, Password: replayPassword, Permissions: replayGrant(replayNoDeleteUser, false)},
		}
	})
	t.Cleanup(s.Shutdown)
	return s
}

// adminJetStream connects as the unrestricted admin, the way seed and web
// reach the bus on an install, for provisioning and observation.
func adminJetStream(t *testing.T, url string) (jetstream.JetStream, context.Context) {
	t.Helper()
	nc, err := nats.Connect(url, nats.UserInfo(replayAdminUser, replayPassword))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), replayAdminTimeout)
	t.Cleanup(cancel)
	return js, ctx
}

// provisionTasksStreamAsProvisioned creates TASKS with the provision script's
// flags (the replayStream* constants above). It is not testutil's
// provisionTasksStream because that one leaves allow_direct off, which
// routes the horizon read through STREAM.MSG.GET -- a subject no rendered
// grant carries.
func provisionTasksStreamAsProvisioned(t *testing.T, url string) {
	t.Helper()
	js, ctx := adminJetStream(t, url)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:         TasksStream,
		Subjects:     []string{replayStreamSubjects},
		Retention:    jetstream.LimitsPolicy,
		Discard:      jetstream.DiscardOld,
		MaxAge:       replayStreamMaxAge,
		MaxConsumers: replayStreamMaxConsumers,
		AllowDirect:  true,
	}); err != nil {
		t.Fatalf("create TASKS stream: %v", err)
	}
}

// tasksConsumers lists the consumers on TASKS as admin.
func tasksConsumers(t *testing.T, url string) []*jetstream.ConsumerInfo {
	t.Helper()
	js, ctx := adminJetStream(t, url)
	st, err := js.Stream(ctx, TasksStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var out []*jetstream.ConsumerInfo
	lister := st.ListConsumers(ctx)
	for info := range lister.Info() {
		out = append(out, info)
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	return out
}

func describeConsumers(infos []*jetstream.ConsumerInfo) string {
	var b strings.Builder
	for _, info := range infos {
		b.WriteString(info.Name)
		b.WriteString(" inactive_threshold=")
		b.WriteString(info.Config.InactiveThreshold.String())
		b.WriteString("; ")
	}
	return b.String()
}

// lockedBuffer is a bytes.Buffer the connection's async error handler can
// write to from its own goroutine.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// replayAs connects a reader as user and folds taskID once.
func replayAs(t *testing.T, url, user, taskID string, log *slog.Logger) *Task {
	t.Helper()
	ctx := testCtx(t)
	opts := []ClientOption{WithName(user + "-reader"), WithUserPassword(user, replayPassword)}
	if log != nil {
		opts = append(opts, WithLogger(log))
	}
	reader, err := Connect(ctx, url, opts...)
	if err != nil {
		t.Fatalf("Connect as %s: %v", user, err)
	}
	t.Cleanup(reader.Close)
	task, err := reader.TasksGet(ctx, replayAddressee(taskID), taskID)
	if err != nil {
		t.Fatalf("TasksGet as %s: %v", user, err)
	}
	return task
}

// A principal holding CONSUMER.DELETE on TASKS leaves nothing behind: the
// consumer is deleted by name when TasksGet returns, well inside the
// threshold, so a burst of replays costs slots for the replays in flight and
// not for the last five minutes of them.
func TestTasksGet_DeletesItsReplayConsumer(t *testing.T) {
	s := startPermissionedServer(t)
	url := clientURL(s)
	provisionTasksStreamAsProvisioned(t, url)
	const taskID = "task-replay-delete"
	replayFixture(t, url, taskID, []TaskState{StateSubmitted, StateWorking, StateCompleted},
		WithUserPassword(replayAdminUser, replayPassword))
	if n := len(tasksConsumers(t, url)); n != 0 {
		t.Fatalf("test bug: %d consumers on TASKS before the replay", n)
	}

	task := replayAs(t, url, replayGatewayUser, taskID, nil)
	if task.State != StateCompleted || !task.Final {
		t.Fatalf("fold = %s final=%v, want completed final", task.State, task.Final)
	}
	after := tasksConsumers(t, url)
	t.Logf("consumers on TASKS the instant TasksGet returned: %d %s", len(after), describeConsumers(after))
	waitFor(t, replayDeleteWithin, "the replay consumer to be deleted", func() bool {
		return len(tasksConsumers(t, url)) == 0
	})
	t.Logf("consumers on TASKS within %s of return: 0", replayDeleteWithin)
}

// A principal WITHOUT CONSUMER.DELETE on TASKS -- the rendered worker grant,
// and the gateway's under #1672 as opened -- has its delete refused without a
// reply. The consumer then lingers, but only until the threshold TasksGet set
// on it, not nats.go's five-minute default; and the refusal is visible in
// the client's own log rather than silent.
func TestTasksGet_ReplayConsumerFallsBackToTheInactiveThreshold(t *testing.T) {
	s := startPermissionedServer(t)
	url := clientURL(s)
	provisionTasksStreamAsProvisioned(t, url)
	const taskID = "task-replay-nodelete"
	replayFixture(t, url, taskID, []TaskState{StateSubmitted, StateCompleted},
		WithUserPassword(replayAdminUser, replayPassword))

	var logs lockedBuffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	start := time.Now()
	task := replayAs(t, url, replayNoDeleteUser, taskID, log)
	if task.State != StateCompleted {
		t.Fatalf("fold = %s, want completed", task.State)
	}
	if took := time.Since(start); took > replayDeleteWithin {
		// The refused delete must not be on the caller's path: a
		// synchronous delete under this grant would hold every tasks/get
		// for replayConsumerDeleteTimeout.
		t.Fatalf("TasksGet took %s under a grant without CONSUMER.DELETE; the delete is holding the caller", took)
	}

	lingering := tasksConsumers(t, url)
	t.Logf("consumers on TASKS the instant TasksGet returned: %d %s", len(lingering), describeConsumers(lingering))
	if len(lingering) != 1 {
		t.Fatalf("consumers on TASKS after return = %d, want the one the refused delete left", len(lingering))
	}
	if got := lingering[0].Config.InactiveThreshold; got != EphemeralConsumerInactiveThreshold {
		t.Fatalf("lingering consumer inactive_threshold = %s, want %s (nats.go's default is 5m)", got, EphemeralConsumerInactiveThreshold)
	}

	waitFor(t, replayDeleteWithin, "the client to log the refused delete", func() bool {
		out := logs.String()
		return strings.Contains(out, "Permissions Violation") && strings.Contains(out, "$JS.API.CONSUMER.DELETE.TASKS.")
	})
	waitFor(t, thresholdReapWithin, "the inactive threshold to reap the consumer", func() bool {
		return len(tasksConsumers(t, url)) == 0
	})
	t.Logf("consumers on TASKS %s after return: 0 (reaped by the %s threshold)", time.Since(start).Round(100*time.Millisecond), EphemeralConsumerInactiveThreshold)
	// The delete's own deadline is replayConsumerDeleteTimeout, on the same
	// clock as the reap above, so the outcome line can land just after it.
	waitFor(t, replayDeleteWithin, "the client to log the delete's outcome", func() bool {
		return strings.Contains(logs.String(), "a2a replay consumer not deleted")
	})
}

// Twenty replays in a row leave twenty consumers on upstream/main, one per
// call, each for five minutes. Here they leave none.
func TestTasksGet_BurstLeavesNoReplayConsumers(t *testing.T) {
	s := startPermissionedServer(t)
	url := clientURL(s)
	provisionTasksStreamAsProvisioned(t, url)
	const taskID = "task-replay-burst"
	replayFixture(t, url, taskID, []TaskState{StateSubmitted, StateWorking, StateCompleted},
		WithUserPassword(replayAdminUser, replayPassword))

	ctx := testCtx(t)
	reader, err := Connect(ctx, url, WithName("burst-reader"), WithUserPassword(replayGatewayUser, replayPassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	for i := 0; i < replayBurst; i++ {
		if _, err := reader.TasksGet(ctx, replayAddressee(taskID), taskID); err != nil {
			t.Fatalf("TasksGet #%d: %v", i+1, err)
		}
	}
	after := tasksConsumers(t, url)
	t.Logf("consumers on TASKS the instant the %d-call burst returned: %d %s", replayBurst, len(after), describeConsumers(after))
	if len(after) > replayBurst {
		t.Fatalf("%d consumers after %d calls: more than the calls that could be in flight", len(after), replayBurst)
	}
	waitFor(t, replayDeleteWithin, "every replay consumer to be deleted", func() bool {
		return len(tasksConsumers(t, url)) == 0
	})
	t.Logf("consumers on TASKS within %s of the burst: 0", replayDeleteWithin)
}

// The delete runs on its own context, not the caller's. TasksGet's caller may
// be canceled or past its deadline by the time the defer runs -- a tasks/get
// that timed out mid-replay -- and that is precisely when the consumer it
// created must still be removed. deleteReplayConsumer takes no context at
// all, so this drives it directly with a consumer created the way TasksGet
// creates one, and TestTasksGet_ContextCanceled in replay_test.go keeps the
// cancellation path itself honest.
func TestDeleteReplayConsumer_NeedsNoLiveCallerContext(t *testing.T) {
	s := startPermissionedServer(t)
	url := clientURL(s)
	provisionTasksStreamAsProvisioned(t, url)
	const taskID = "task-replay-orphan"

	ctx := testCtx(t)
	c, err := Connect(ctx, url, WithName("orphan-reader"), WithUserPassword(replayGatewayUser, replayPassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	_, js := c.conn()
	cons, err := js.OrderedConsumer(ctx, TasksStream, jetstream.OrderedConsumerConfig{
		FilterSubjects:    TaskReplaySubjects(replayAddressee(taskID), taskID),
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: EphemeralConsumerInactiveThreshold,
	})
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}
	if n := len(tasksConsumers(t, url)); n != 1 {
		t.Fatalf("consumers on TASKS after create = %d, want 1", n)
	}

	c.deleteReplayConsumer(js, cons, taskID)
	waitFor(t, replayDeleteWithin, "the orphaned replay consumer to be deleted", func() bool {
		return len(tasksConsumers(t, url)) == 0
	})
}

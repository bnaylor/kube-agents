"""Offline tests for the Slack live-check harness (hack/slack-live-check/harness.py).

A local HTTP server stands in for the Slack Web API, the GKE metadata server and
Secret Manager, and plays a minimal gateway: a DM to the bot or a mention of it is a
turn, a reply in a thread the bot has answered in is a turn, a listed sender gets the
status line and an answer, and anyone else gets the refusal notice once. The harness
runs end to end through its real urllib transport against it. The clock and sleep
are fakes, so the bounded polls time out without waiting.
"""

import base64
import contextlib
import importlib.util
import io
import json
import pathlib
import re
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = pathlib.Path(__file__).resolve().parent.parent
HARNESS_PATH = REPO / "hack" / "slack-live-check" / "harness.py"

spec = importlib.util.spec_from_file_location("slack_live_check_harness", HARNESS_PATH)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)

PROJECT = "test-project"
LISTED_SECRET = "slack-test-user-listed"
UNLISTED_SECRET = "slack-test-user-unlisted"
LISTED_TOKEN = "xoxp-1111-listed-user-token-value"
UNLISTED_TOKEN = "xoxp-2222-unlisted-user-token-value"
# Deliberately not token-shaped: only registration with the redactor can cut it.
GCP_ACCESS = "gcp-access-value-0b9e5d"
LISTED_ID = "U0LISTED1"
UNLISTED_ID = "U0UNLIST1"
BOT_ID = "U0BOTKAGE"
BOT_APP_BOT_ID = "B0BOTKAGE"
CHANNEL_ID = "C0KATEST1"
CHANNEL_NAME = "ka-test"
HOME_ID = "C0HOME001"
PRIVATE_ID = "G0PRIVATE1"
PRIVATE_NAME = "ka-private"
TEAM_ID = "T0TEAM001"
REFUSAL = ("⛔ I can't verify who you are on slack (id {id}), so I can't take asks from you yet — "
           "an admin has to add you to the allowed users list and the principal map.")
ALL_TOKENS = (LISTED_TOKEN, UNLISTED_TOKEN, GCP_ACCESS)
# The notices the gateway posts in place of startTask for a turn it will not run,
# rendered as they reach Slack (test_not_started_grammar_matches_the_gateway_source).
NOT_STARTED_NOTICES = (
    "🚦 not started: 10 session workers are already running (cap 10). Wait for one to finish or `stop` one you started; "
    "an operator can raise the cap (A2A_MAX_SESSIONS / spec.harness.tuning.maxSessions).",
    "🚦 not started: 1 session worker is already running (cap 1). Wait for one to finish or `stop` one you started; "
    "an operator can raise the cap (A2A_MAX_SESSIONS / spec.harness.tuning.maxSessions).",
    "⚠️ not started: can't count the running session workers right now — try again in a moment",
    "⚠️ not started: could not mint this task's capability",
    "⚠️ not started: could not close the previous task on the bus; try again in a moment",
)
# Far above any timeout a test sets: a poll that passes it is unbounded.
SLEEP_BUDGET_SECONDS = 10_000


class FakeWorld:
    """Slack, the metadata server, Secret Manager, and a tiny gateway, in one object."""

    def __init__(self):
        self.lock = threading.Lock()
        self.ts_counter = 100
        self.secrets = {LISTED_SECRET: LISTED_TOKEN, UNLISTED_SECRET: UNLISTED_TOKEN}
        self.tokens = {LISTED_TOKEN: LISTED_ID, UNLISTED_TOKEN: UNLISTED_ID}
        self.listed = {LISTED_ID}
        self.channels = {CHANNEL_ID: [], HOME_ID: []}
        self.dms = {}
        self.session_threads = set()
        self.notified = set()
        # Behaviour switches the tests flip.
        self.mode = "answer"  # answer | silent | refuse-all | answer-everyone | top-level | status-only | fail
        self.user_posts_carry_bot_id = False
        self.user_post_subtype = ""
        self.user_posts_without_user = False
        # A task answer that lands this many reads later, with a fresh ts, the way a real
        # answer arrives after the user has moved on.
        self.answer_after_reads = 0
        self.deferred = []
        self.ignore_unmentioned_thread_replies = False
        self.steer_thread_replies = False
        self.slack_error_override = {}
        self.secret_error_body = None
        self.reply_delay_reads = 0
        self.reads = 0
        self.not_in_channel = set()
        self.user_post_user_override = ""
        self.history_hides_latest = False
        self.refusal = REFUSAL
        self.extra_members = []
        self.bot_ids = {BOT_ID}
        self.metadata_token = GCP_ACCESS
        # The deliverable's text: the gateway posts the agent's result verbatim.
        self.answer_text = "PONG"
        # next_cursor on every users.list and conversations.list page: a workspace
        # larger than the lookups' page cap.
        self.list_cursor = ""
        # A token without groups:read: conversations.list refuses private_channel with missing_scope.
        self.lacks_groups_read = False
        self.conversation_list_types = []

    def next_ts(self):
        self.ts_counter += 1
        return f"1700000000.{self.ts_counter:06d}"

    def dm_for(self, a, b):
        key = frozenset({a, b})
        if key not in self.dms:
            self.dms[key] = f"D0{len(self.dms):07d}"
            self.channels[self.dms[key]] = []
        return self.dms[key]

    def is_dm_with_bot(self, channel):
        return any(c == channel and BOT_ID in key for key, c in self.dms.items())

    def bot_post(self, channel, text, thread_ts=""):
        msg = {"type": "message", "user": BOT_ID, "bot_id": BOT_APP_BOT_ID, "text": text, "ts": self.next_ts(),
               "visible_at": self.reads + self.reply_delay_reads}
        if thread_ts:
            msg["thread_ts"] = thread_ts
        self.channels[channel].append(msg)
        return msg

    def finish(self, channel, status, text, reply_thread, terminal):
        """relay.go relayTerminal: the deliverable or failure is posted, then the status line is edited."""
        self.bot_post(channel, text, reply_thread)
        status["text"] = terminal

    def gateway_turn(self, channel, poster, msg):
        if self.mode == "silent":
            return
        text = msg["text"]
        thread_ts = msg.get("thread_ts", "")
        if self.is_dm_with_bot(channel):
            reply_thread = ""
        elif thread_ts and thread_ts != msg["ts"]:
            if f"<@{BOT_ID}>" not in text and (
                    (channel, thread_ts) not in self.session_threads or self.ignore_unmentioned_thread_replies):
                return
            reply_thread = thread_ts
        elif f"<@{BOT_ID}>" in text:
            reply_thread = msg["ts"]
        else:
            return
        if self.mode == "top-level":
            reply_thread = ""
        admitted = (poster in self.listed or self.mode == "answer-everyone") and self.mode != "refuse-all"
        if not admitted:
            if poster not in self.notified:
                self.notified.add(poster)
                self.bot_post(channel, self.refusal.format(id=poster), reply_thread)
            return
        if self.steer_thread_replies and thread_ts and thread_ts != msg["ts"]:
            self.bot_post(channel, "✏️ steering sent — the worker picks it up at its next turn boundary", reply_thread)
            return
        if reply_thread:
            self.session_threads.add((channel, reply_thread))
        status = self.bot_post(channel, "⏳ submitted…", reply_thread)
        if self.mode == "status-only":
            return
        status["text"] = "⚙️ *working*"
        if self.mode == "fail":
            self.finish(channel, status, "❌ failed: the executor is down", reply_thread, "❌ *failed*")
            return
        if self.answer_after_reads:
            self.deferred.append((self.reads + self.answer_after_reads, channel, status, reply_thread))
            return
        self.finish(channel, status, self.answer_text, reply_thread, "✅ *completed*")

    def materialize(self):
        due = [d for d in self.deferred if d[0] <= self.reads]
        self.deferred = [d for d in self.deferred if d[0] > self.reads]
        for _, channel, status, thread in due:
            self.finish(channel, status, self.answer_text, thread, "✅ *completed*")

    def visible(self, msgs):
        return [{k: v for k, v in m.items() if k != "visible_at"} for m in msgs if m.get("visible_at", 0) <= self.reads]

    def slack(self, method, user, params):
        if method in self.slack_error_override:
            return {"ok": False, "error": self.slack_error_override[method]}
        if method == "auth.test":
            return {"ok": True, "user_id": user, "team_id": TEAM_ID}
        if method == "users.list":
            return {"ok": True, "members": [
                {"id": LISTED_ID, "name": "listed", "is_bot": False},
                {"id": BOT_ID, "name": "kage", "is_bot": True, "profile": {"display_name": "kage"}},
                *self.extra_members,
            ], "response_metadata": {"next_cursor": self.list_cursor}}
        if method == "users.info":
            return {"ok": True, "user": {"id": params["user"], "is_bot": params["user"] in self.bot_ids}}
        if method == "conversations.list":
            types = params.get("types", "public_channel").split(",")
            self.conversation_list_types.append(params.get("types", ""))
            if "private_channel" in types and self.lacks_groups_read:
                return {"ok": False, "error": "missing_scope", "needed": "groups:read"}
            channels = [{"id": CHANNEL_ID, "name": CHANNEL_NAME}, {"id": HOME_ID, "name": "home"}]
            if "private_channel" in types:
                channels.append({"id": PRIVATE_ID, "name": PRIVATE_NAME, "is_private": True})
            return {"ok": True, "channels": channels, "response_metadata": {"next_cursor": self.list_cursor}}
        if method == "conversations.open":
            return {"ok": True, "channel": {"id": self.dm_for(user, params["users"])}}
        if method == "chat.postMessage":
            channel = params["channel"]
            if channel in self.not_in_channel and user in self.not_in_channel:
                return {"ok": False, "error": "not_in_channel"}
            msg = {"type": "message", "user": user, "text": params["text"], "ts": self.next_ts()}
            if params.get("thread_ts"):
                msg["thread_ts"] = params["thread_ts"]
            if self.user_posts_carry_bot_id:
                msg["bot_id"] = "B0USERAPP"
                msg["app_id"] = "A0USERAPP"
            if self.user_post_subtype:
                msg["subtype"] = self.user_post_subtype
            if self.user_posts_without_user:
                del msg["user"]
            if self.user_post_user_override:
                msg["user"] = self.user_post_user_override
            self.channels[channel].append(msg)
            self.gateway_turn(channel, user, msg)
            return {"ok": True, "channel": channel, "ts": msg["ts"]}
        if method == "conversations.history":
            self.reads += 1
            self.materialize()
            msgs = [m for m in self.channels[params["channel"]] if not m.get("thread_ts") or m["thread_ts"] == m["ts"]]
            if "latest" in params:
                msgs = [] if self.history_hides_latest else [m for m in msgs if m["ts"] == params["latest"]]
            elif "oldest" in params:
                msgs = [m for m in msgs if float(m["ts"]) > float(params["oldest"])]
            return {"ok": True, "messages": list(reversed(self.visible(msgs)))}
        if method == "conversations.replies":
            self.reads += 1
            self.materialize()
            root = params["ts"]
            msgs = [m for m in self.channels[params["channel"]] if m["ts"] == root or m.get("thread_ts") == root]
            return {"ok": True, "messages": self.visible(msgs)}
        return {"ok": False, "error": "unknown_method"}


class Handler(BaseHTTPRequestHandler):
    world: FakeWorld = None

    def log_message(self, *args):
        pass

    def reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        world = self.world
        if self.path.startswith("/metadata/token"):
            if self.headers.get("Metadata-Flavor") != "Google":
                return self.reply(403, {"error": "missing flavor"})
            return self.reply(200, {"access_token": world.metadata_token, "expires_in": 3599})
        if self.path.startswith("/sm/projects/"):
            if self.headers.get("Authorization") != f"Bearer {GCP_ACCESS}":
                return self.reply(401, {"error": {"message": "unauthenticated"}})
            if world.secret_error_body is not None:
                return self.reply(403, world.secret_error_body)
            secret = self.path.split("/secrets/")[1].split("/")[0]
            if secret not in world.secrets:
                return self.reply(404, {"error": {"message": f"Secret [{secret}] not found"}})
            data = base64.b64encode((world.secrets[secret] + "\n").encode()).decode()
            return self.reply(200, {"payload": {"data": data}})
        return self.reply(404, {"error": "no route"})

    def do_POST(self):
        world = self.world
        length = int(self.headers.get("Content-Length", "0"))
        params = dict(urllib.parse.parse_qsl(self.rfile.read(length).decode()))
        auth = self.headers.get("Authorization", "")
        user = world.tokens.get(auth.removeprefix("Bearer "))
        if not self.path.startswith("/api/"):
            return self.reply(404, {})
        if user is None:
            return self.reply(200, {"ok": False, "error": "invalid_auth"})
        with world.lock:
            payload = world.slack(self.path[len("/api/"):], user, params)
        return self.reply(200, payload)


class Unbounded(BaseException):
    """A poll that outran every timeout. A BaseException, so harness.main's
    `except Exception` cannot turn it into an ERROR line and an exit code."""


class FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.slept = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        if seconds <= 0 or len(self.slept) > SLEEP_BUDGET_SECONDS:
            raise Unbounded(f"a poll slept {seconds}s after {len(self.slept)} sleeps; it is not bounded")
        self.slept.append(seconds)
        self.now += seconds
        if self.now - 1000.0 > SLEEP_BUDGET_SECONDS:
            raise Unbounded("a poll kept sleeping past every timeout the test set")

    def wall(self):
        return 1700000000.0 + (self.now - 1000.0)


class HarnessTestCase(unittest.TestCase):
    def setUp(self):
        self.world = FakeWorld()
        handler = type("BoundHandler", (Handler,), {"world": self.world})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()
        base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.endpoint_args = [
            "--slack-api-base", base + "/api/",
            "--metadata-token-url", base + "/metadata/token",
            "--secret-manager-base", base + "/sm/",
            "--project", PROJECT,
            "--poll-interval", "5",
            "--reply-timeout", "60",
            "--quiet-window", "20",
            "--run-id", "testrun",
            "--bot-name", "kage",
        ]
        self.fake = FakeClock()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def run_harness(self, *extra):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = harness.main([*self.endpoint_args, *extra], clock=self.fake.clock, sleep=self.fake.sleep, wall=self.fake.wall)
        text = out.getvalue() + err.getvalue()
        for value in ALL_TOKENS:
            self.assertNotIn(value, text, "a credential reached the output")
        return code, text

    def line(self, text, prefix):
        found = [ln for ln in text.splitlines() if ln.startswith(prefix)]
        self.assertTrue(found, f"no line starting {prefix!r} in:\n{text}")
        return found[0]

    def evidence(self, text, check):
        for ln in text.splitlines():
            if ln.startswith("EVIDENCE "):
                ev = json.loads(ln[len("EVIDENCE "):])
                if ev["check"] == check:
                    return ev
        self.fail(f"no evidence for {check} in:\n{text}")


class PreflightTest(HarnessTestCase):
    def test_preflight_passes_and_reports_fields(self):
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertIn("PASS preflight-listed: user=U0LISTED1, no bot_id", text)
        ev = self.evidence(text, "preflight-listed")
        self.assertEqual(ev["bot_id"], "")
        self.assertEqual(ev["user"], LISTED_ID)

    def test_preflight_fails_on_bot_id_and_reports_app_id(self):
        self.world.user_posts_carry_bot_id = True
        code, text = self.run_harness("--checks", "dm,mention", "--keep-going")
        self.assertEqual(code, harness.EXIT_FAIL)
        fail = self.line(text, "FAIL preflight-listed")
        self.assertIn("bot_id=B0USERAPP", fail)
        self.assertIn("app_id=A0USERAPP (the gateway's filter ignores app_id)", fail)
        # A failed preflight stops the run even under --keep-going.
        self.assertNotIn(" dm:", text)
        self.assertIn("SUMMARY pass=0 fail=1", text)

    def test_preflight_fails_on_non_turn_subtype(self):
        self.world.user_post_subtype = "bot_message"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("subtype 'bot_message' is not a turn", self.line(text, "FAIL preflight-listed"))

    def test_preflight_fails_without_a_user_field(self):
        self.world.user_posts_without_user = True
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("it has no user field", self.line(text, "FAIL preflight-listed"))

    def test_preflight_fails_when_the_post_reads_back_under_another_user(self):
        self.world.user_post_user_override = "U0OTHER01"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("its user U0OTHER01 is not the token's user U0LISTED1", self.line(text, "FAIL preflight-listed"))

    def test_preflight_fails_when_the_post_cannot_be_read_back(self):
        self.world.history_hides_latest = True
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("could not be read back", self.line(text, "FAIL preflight-listed"))

    def test_a_slack_error_in_the_preflight_is_its_fail(self):
        self.world.slack_error_override["conversations.history"] = "internal_error"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL, text)
        self.assertIn("error: Slack conversations.history failed: internal_error", self.line(text, "FAIL preflight-listed"))
        self.assertIn("SUMMARY pass=0 fail=1", text)
        self.assertNotIn("setup failed", text)

    def test_preflight_accepts_thread_broadcast(self):
        self.world.user_post_subtype = "thread_broadcast"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_OK, text)

    def test_preflight_falls_back_to_self_dm_outside_the_channel(self):
        self.world.not_in_channel = {CHANNEL_ID, UNLISTED_ID}
        code, text = self.run_harness("--checks", "unlisted")
        self.assertEqual(code, harness.EXIT_OK, text)
        ev = self.evidence(text, "preflight-unlisted")
        self.assertTrue(ev["channel"].startswith("D"), ev)

    def test_preflight_runs_for_the_unlisted_token_only_when_needed(self):
        _, text = self.run_harness("--checks", "dm")
        self.assertNotIn("preflight-unlisted", text)


class ListedChecksTest(HarnessTestCase):
    def test_dm_mention_thread_pass(self):
        code, text = self.run_harness("--checks", "dm,mention,thread")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertIn("PASS dm:", text)
        mention = self.evidence(text, "mention")
        thread = self.evidence(text, "thread")
        self.assertEqual(mention["reply_thread_ts"], mention["sent_ts"])
        self.assertEqual(thread["thread_ts"], mention["sent_ts"])
        self.assertEqual(thread["reply_thread_ts"], mention["sent_ts"])
        self.assertIn("SUMMARY pass=4 fail=0", text)

    def test_dm_fails_on_silence_after_a_bounded_poll(self):
        self.world.mode = "silent"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("no reply from the bot within 60.0s", self.line(text, "FAIL dm"))
        # Bounded: the fake clock advanced by the timeout and no further.
        self.assertAlmostEqual(sum(self.fake.slept), 60.0)
        self.assertTrue(all(s <= 5 for s in self.fake.slept))

    def test_dm_reply_after_a_few_polls_passes(self):
        self.world.reply_delay_reads = 3
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertGreaterEqual(len(self.fake.slept), 2)

    def test_dm_fails_when_the_listed_user_is_refused(self):
        self.world.mode = "refuse-all"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("got the refusal notice", self.line(text, "FAIL dm"))

    def test_wait_answer_waits_past_the_status_line(self):
        code, text = self.run_harness("--checks", "dm", "--wait-answer")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertEqual(self.evidence(text, "dm")["reply_text"], "PONG")

    def test_wait_answer_fails_when_only_the_status_line_arrives(self):
        self.world.mode = "status-only"
        code, text = self.run_harness("--checks", "dm", "--wait-answer")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("no answer (--wait-answer)", self.line(text, "FAIL dm"))
        self.assertEqual(self.evidence(text, "dm")["last_bot_text"], "⏳ submitted…")

    def test_wait_answer_fails_on_a_failed_task(self):
        self.world.mode = "fail"
        code, text = self.run_harness("--checks", "dm", "--wait-answer")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("the task the turn started failed", text)

    def test_an_answer_that_opens_with_a_gateway_icon_is_the_answer(self):
        # The gateway posts the agent's result verbatim, so its first character is the
        # agent's to choose. Only the status line's state says the answer is in.
        for answer in ("✅ PONG", "⚠️ PONG, with a caveat", "ℹ️ PONG", "❓ PONG?", "❌ PONG", "🛑 PONG", "⏳ PONG"):
            with self.subTest(answer=answer):
                self.world.answer_text = answer
                code, text = self.run_harness("--checks", "dm,mention,thread", "--wait-answer")
                self.assertEqual(code, harness.EXIT_OK, text)
                for check in ("dm", "mention", "thread"):
                    self.assertEqual(self.evidence(text, check)["reply_text"], answer, check)
                    self.assertEqual(self.evidence(text, check)["reply_kind"], harness.KIND_ANSWER, check)

    def test_thread_settles_on_the_status_line_whatever_the_answer_says(self):
        self.world.answer_text = "✅ PONG"
        code, text = self.run_harness("--checks", "mention,thread")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertEqual(sum(self.fake.slept), 0, "the settle wait sat out a timeout on a finished task")

    def test_a_status_card_before_the_turn_is_not_the_answer(self):
        # healActiveTask (gateway.go) posts formatTaskStatus's card ahead of the new
        # turn's status line: it is the gateway's, not the answer.
        world = self.world
        original = world.gateway_turn

        def card_then_turn(channel, poster, msg):
            world.bot_post(channel, "🔎 task `t0` is *completed*", "")
            original(channel, poster, msg)

        world.gateway_turn = card_then_turn
        code, text = self.run_harness("--checks", "dm", "--wait-answer")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertEqual(self.evidence(text, "dm")["reply_text"], "PONG")

    def test_a_status_card_before_the_turn_is_not_the_first_reply(self):
        # The same card under the default reading, read before the new turn's status
        # line is visible: it is not the reply, and the turn's reply is its status line.
        world = self.world
        original = world.gateway_turn

        def card_then_turn(channel, poster, msg):
            world.bot_post(channel, "🔎 task `t0` is *completed*", "")
            world.reply_delay_reads = 2
            original(channel, poster, msg)
            world.reply_delay_reads = 0

        world.gateway_turn = card_then_turn
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertEqual(self.evidence(text, "dm")["reply_kind"], harness.KIND_TASK_LINE)
        self.assertNotIn("🔎", self.evidence(text, "dm")["reply_text"])

    def test_wait_answer_waits_for_the_status_line_to_turn_terminal(self):
        # The answer is posted before the status line's terminal edit; until that
        # edit lands the task is still running and nothing after the line is final.
        world = self.world
        world.mode = "status-only"
        original = world.gateway_turn

        def answer_without_terminal(channel, poster, msg):
            original(channel, poster, msg)
            world.bot_post(channel, "PONG", "")

        world.gateway_turn = answer_without_terminal
        code, text = self.run_harness("--checks", "dm", "--wait-answer")
        self.assertEqual(code, harness.EXIT_FAIL, text)
        self.assertIn("no answer (--wait-answer)", self.line(text, "FAIL dm"))

    def test_mention_fails_when_the_reply_is_not_threaded(self):
        self.world.mode = "top-level"
        code, text = self.run_harness("--checks", "mention")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("no reply from the bot", self.line(text, "FAIL mention"))

    def test_thread_fails_without_a_root_and_first_fail_stops(self):
        self.world.mode = "silent"
        code, text = self.run_harness("--checks", "mention,thread")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("FAIL mention", text)
        self.assertNotIn("thread:", text)

    def test_keep_going_runs_thread_after_a_failed_mention(self):
        self.world.mode = "silent"
        code, text = self.run_harness("--checks", "mention,thread", "--keep-going")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("no thread to reply in", self.line(text, "FAIL thread"))
        self.assertIn("SUMMARY pass=1 fail=2", text)

    def test_thread_fails_when_the_bot_ignores_the_reply(self):
        code, text = self.run_harness("--checks", "mention")
        root = self.evidence(text, "mention")["sent_ts"]
        self.world.session_threads.clear()
        code, text = self.run_harness("--checks", "thread", "--thread-ts", root)
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("FAIL thread", text)

    def test_thread_is_not_fooled_by_the_mention_answer_landing_late(self):
        # The mention passes on its status line; its answer arrives later. A gateway
        # that drops the unmentioned follow-up must fail the thread check, not pass it
        # on the mention's late answer.
        self.world.answer_after_reads = 3
        self.world.ignore_unmentioned_thread_replies = True
        code, text = self.run_harness("--checks", "mention,thread", "--keep-going")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("PASS mention", text)
        self.assertIn("no reply from the bot", self.line(text, "FAIL thread"))

    def test_thread_waits_for_the_mention_task_to_settle(self):
        self.world.answer_after_reads = 3
        code, text = self.run_harness("--checks", "mention,thread")
        self.assertEqual(code, harness.EXIT_OK, text)
        thread = self.evidence(text, "thread")
        self.assertEqual(thread["settled_kind"], harness.KIND_ANSWER)
        self.assertGreater(harness.ts_value(thread["reply_ts"]), harness.ts_value(thread["sent_ts"]))

    def test_thread_fails_when_the_follow_up_is_taken_as_a_steer(self):
        self.world.steer_thread_replies = True
        code, text = self.run_harness("--checks", "mention,thread")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("taken as a steer", self.line(text, "FAIL thread"))

    def test_thread_fails_on_a_steer_under_wait_answer_too(self):
        # A steering gateway posts the ack and then the running task's answer; under
        # --wait-answer the answer must not stand in for a new turn.
        world = self.world
        world.steer_thread_replies = True
        original = world.bot_post

        def ack_then_answer(channel, text, thread_ts=""):
            msg = original(channel, text, thread_ts)
            if text.startswith("✏️"):
                original(channel, "PONG", thread_ts)
            return msg

        world.bot_post = ack_then_answer
        code, text = self.run_harness("--checks", "mention,thread", "--wait-answer")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("taken as a steer", self.line(text, "FAIL thread"))

    def test_a_warning_ahead_of_the_status_line_is_not_the_reply(self):
        world = self.world
        original = world.gateway_turn

        def warn_then_turn(channel, poster, msg):
            world.bot_post(channel, "⚠️ task `t` has produced nothing on its event stream in 5m, so this conversation is released", "")
            original(channel, poster, msg)

        world.gateway_turn = warn_then_turn
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_OK, text)
        # The status line, read after the task finished: the same message, edited.
        self.assertEqual(self.evidence(text, "dm")["reply_text"], "✅ *completed*")
        self.assertEqual(self.evidence(text, "dm")["reply_kind"], harness.KIND_TASK_LINE)

    def post_instead_of_a_task(self, notice):
        """The gateway answers the turn with notice and starts no task: no status line follows."""
        world = self.world

        def notice_only(channel, poster, msg):
            thread = "" if world.is_dm_with_bot(channel) else msg.get("thread_ts", msg["ts"])
            world.bot_post(channel, notice, thread)

        world.gateway_turn = notice_only

    def test_a_turn_the_gateway_did_not_start_fails_at_once(self):
        # refuseAtSessionCap (spawn.go) and the other "not started:" refusals answer the
        # turn in place of startTask, so no status line ever follows them.
        for notice in NOT_STARTED_NOTICES:
            for check, extra in (("dm", ()), ("restart", ()), ("mention", ()), ("dm", ("--wait-answer",))):
                with self.subTest(notice=notice, check=check, extra=extra):
                    self.post_instead_of_a_task(notice)
                    slept = len(self.fake.slept)
                    code, text = self.run_harness("--checks", check, *extra)
                    self.assertEqual(code, harness.EXIT_FAIL, text)
                    line = self.line(text, f"FAIL {check}")
                    self.assertIn("the gateway did not start a task", line)
                    self.assertIn(notice[:40], line)
                    self.assertEqual(self.evidence(text, check)["reply_kind"], harness.KIND_NOT_STARTED)
                    self.assertEqual(self.fake.slept[slept:], [], "a refusal sat out the reply timeout")

    def test_a_notice_with_no_status_line_after_it_fails_and_quotes_it(self):
        # Not the status line and not a refusal the harness knows: under the default
        # reading it is no reply, and the FAIL names what the bot did post.
        for notice in ("🤷 nothing is running", "ℹ️ this conversation is already a session",
                       "⚠️ task `t` has produced nothing on its event stream in 5m"):
            with self.subTest(notice=notice):
                self.post_instead_of_a_task(notice)
                code, text = self.run_harness("--checks", "dm")
                self.assertEqual(code, harness.EXIT_FAIL, text)
                line = self.line(text, "FAIL dm")
                self.assertIn("no status line", line)
                self.assertIn(notice, line)
                self.assertEqual(self.evidence(text, "dm")["last_bot_text"], notice)

    def test_the_quoted_notice_is_redacted(self):
        self.post_instead_of_a_task(f"🤷 echoing {LISTED_TOKEN}")
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_FAIL, text)
        self.assertNotIn(LISTED_TOKEN, text)
        self.assertIn(harness.REDACTED, self.line(text, "FAIL dm"))

    def test_dm_fails_on_a_steer(self):
        world = self.world

        def steer(channel, poster, msg):
            if world.is_dm_with_bot(channel):
                world.bot_post(channel, "✏️ steering sent — the worker picks it up", "")

        world.gateway_turn = steer
        code, text = self.run_harness("--checks", "dm", "--wait-answer")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("taken as a steer", self.line(text, "FAIL dm"))

    def test_thread_fails_when_the_first_task_never_settles(self):
        self.world.mode = "status-only"
        code, text = self.run_harness("--checks", "mention,thread", "--keep-going")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("did not settle", self.line(text, "FAIL thread"))

    def test_a_slack_error_in_a_check_is_its_fail_and_keep_going_carries_on(self):
        # The listed user is not in the channel: the preflight falls back to the
        # self-DM, dm passes, and the mention's post raises not_in_channel.
        self.world.not_in_channel = {CHANNEL_ID, LISTED_ID}
        code, text = self.run_harness("--checks", "dm,mention,thread", "--keep-going")
        self.assertEqual(code, harness.EXIT_FAIL, text)
        self.assertIn("PASS dm:", text)
        self.assertIn("error: Slack chat.postMessage failed: not_in_channel", self.line(text, "FAIL mention"))
        self.assertIn("no thread to reply in", self.line(text, "FAIL thread"))
        self.assertIn("SUMMARY pass=2 fail=2", text)
        self.assertNotIn("setup failed", text)
        code, text = self.run_harness("--checks", "dm,mention,thread")
        self.assertEqual(code, harness.EXIT_FAIL, text)
        self.assertNotIn("thread:", text)
        self.assertIn("SUMMARY pass=2 fail=1", text)

    def test_restart_is_a_dm_under_its_own_name(self):
        code, text = self.run_harness("--checks", "restart")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertIn("PASS restart:", text)
        self.assertEqual(self.evidence(text, "restart")["author"], LISTED_ID)


class UnlistedTest(HarnessTestCase):
    def test_unlisted_dm_gets_the_notice(self):
        code, text = self.run_harness("--checks", "unlisted", "--unlisted-repeat")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertIn("PASS unlisted: refusal notice", text)
        self.assertIn("PASS unlisted-repeat", text)

    def test_unlisted_mention_gets_the_notice_in_the_thread(self):
        code, text = self.run_harness("--checks", "unlisted", "--unlisted-via", "mention")
        self.assertEqual(code, harness.EXIT_OK, text)
        ev = self.evidence(text, "unlisted")
        self.assertEqual(ev["reply_thread_ts"], ev["sent_ts"])

    def test_unlisted_fails_when_answered(self):
        self.world.mode = "answer-everyone"
        code, text = self.run_harness("--checks", "unlisted")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("was answered", self.line(text, "FAIL unlisted"))

    def test_unlisted_silence_fails_unless_accepted(self):
        self.world.notified.add(UNLISTED_ID)
        code, text = self.run_harness("--checks", "unlisted")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("once per sender per gateway process", self.line(text, "FAIL unlisted"))
        code, text = self.run_harness("--checks", "unlisted", "--refusal-silence-ok")
        self.assertEqual(code, harness.EXIT_OK, text)

    def test_unlisted_repeat_fails_when_the_notice_repeats(self):
        world = self.world
        original = world.gateway_turn

        def forgetful(channel, poster, msg):
            world.notified.discard(poster)
            original(channel, poster, msg)

        world.gateway_turn = forgetful
        code, text = self.run_harness("--checks", "unlisted", "--unlisted-repeat")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("FAIL unlisted-repeat", text)

    def test_unlisted_checks_count_any_bot_message_as_a_reply(self):
        # The unlisted checks pass on the refusal or on silence, so a notice that is not
        # the status line must still count as the bot answering the unlisted user.
        world = self.world
        original = world.gateway_turn

        def notice_on_the_second(channel, poster, msg):
            if harness.CHECK_UNLISTED_REPEAT in msg["text"]:
                world.bot_post(channel, "🤷 nothing is running", "")
            else:
                original(channel, poster, msg)

        world.gateway_turn = notice_on_the_second
        code, text = self.run_harness("--checks", "unlisted", "--unlisted-repeat")
        self.assertEqual(code, harness.EXIT_FAIL, text)
        self.assertIn(f"the bot replied to the second message ({harness.KIND_NOTICE})", self.line(text, "FAIL unlisted-repeat"))
        world.notified.clear()
        world.gateway_turn = lambda channel, poster, msg: world.bot_post(channel, "🤷 nothing is running", "")
        code, text = self.run_harness("--checks", "unlisted")
        self.assertEqual(code, harness.EXIT_FAIL, text)
        self.assertIn(f"answered ({harness.KIND_NOTICE})", self.line(text, "FAIL unlisted"))

    def test_unlisted_repeat_by_mention_reaches_the_gateway(self):
        world = self.world
        original = world.gateway_turn

        def forgetful(channel, poster, msg):
            world.notified.discard(poster)
            original(channel, poster, msg)

        world.gateway_turn = forgetful
        code, text = self.run_harness("--checks", "unlisted", "--unlisted-via", "mention", "--unlisted-repeat")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("FAIL unlisted-repeat", text)

    def test_unlisted_fails_when_the_notice_does_not_name_the_sender(self):
        self.world.refusal = REFUSAL.replace("(id {id})", "(id someone)")
        code, text = self.run_harness("--checks", "unlisted")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("does not name the unlisted user's member id", self.line(text, "FAIL unlisted"))

    def test_same_user_on_both_tokens_is_a_setup_error(self):
        self.world.tokens[UNLISTED_TOKEN] = LISTED_ID
        code, text = self.run_harness("--checks", "unlisted")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("belong to the same Slack user", text)


class HomeTest(HarnessTestCase):
    def test_home_passes_on_a_bot_post(self):
        self.world.bot_post(HOME_ID, "good morning from kage")
        code, text = self.run_harness("--checks", "home", "--home-channel", "home", "--home-since", "1600000000",
                                      "--home-match", "morning")
        self.assertEqual(code, harness.EXIT_OK, text)

    def test_home_times_out(self):
        code, text = self.run_harness("--checks", "home", "--home-channel", HOME_ID, "--home-timeout", "30")
        self.assertEqual(code, harness.EXIT_FAIL)
        self.assertIn("no bot post in C0HOME001 within 30.0s", text)
        self.assertAlmostEqual(sum(self.fake.slept), 30.0)


class SetupAndRedactionTest(HarnessTestCase):
    def test_bot_lookup_by_flag_must_be_a_bot(self):
        code, text = self.run_harness("--checks", "dm", "--bot-user-id", LISTED_ID)
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("is not a bot user", text)

    def test_bot_lookup_by_unknown_name(self):
        code, text = self.run_harness("--checks", "dm", "--bot-name", "nobody")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("no bot user named 'nobody'", text)

    def test_two_bots_with_the_name_are_a_setup_error(self):
        self.world.extra_members.append({"id": "U0BOTTWO1", "name": "kage", "is_bot": True})
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("more than one bot user is named 'kage' (U0BOTKAGE, U0BOTTWO1)", text)

    def test_the_bot_cannot_be_a_test_user(self):
        self.world.bot_ids.add(LISTED_ID)
        code, text = self.run_harness("--checks", "dm", "--bot-user-id", LISTED_ID)
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("the bot user id is one of the test users", text)

    def test_a_token_that_is_not_a_user_token_is_refused(self):
        self.world.secrets[LISTED_SECRET] = "not-a-slack-token"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("does not hold a Slack user token (xoxp-)", text)

    def test_a_metadata_answer_without_a_token_is_refused(self):
        self.world.metadata_token = ""
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("answered without an access token", text)

    def test_an_unknown_channel_name_is_a_setup_error(self):
        code, text = self.run_harness("--checks", "dm", "--channel", "nope")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("no channel named #nope", text)

    def test_a_private_channel_resolves_by_name_with_groups_read(self):
        code, text = self.run_harness("--checks", "home", "--channel", PRIVATE_NAME, "--home-channel", PRIVATE_NAME,
                                      "--home-timeout", "5")
        self.assertIn(f"channel={PRIVATE_ID}", text)
        self.assertEqual(self.world.conversation_list_types, ["public_channel,private_channel"] * 2)

    def test_without_groups_read_the_lookup_falls_back_to_public_channels(self):
        # Live run 2026-10-07: the minted user tokens carry channels:read but not groups:read.
        self.world.lacks_groups_read = True
        self.world.bot_post(HOME_ID, "good morning")
        code, text = self.run_harness("--checks", "home", "--home-channel", "home", "--home-since", "1600000000")
        self.assertEqual(code, harness.EXIT_OK, text)
        self.assertIn(f"channel={CHANNEL_ID}", text)
        # --channel, then --home-channel: each tries both types once, then public only.
        self.assertEqual(self.world.conversation_list_types,
                         ["public_channel,private_channel", "public_channel"] * 2)

    def test_without_groups_read_a_private_channel_name_says_what_it_needs(self):
        self.world.lacks_groups_read = True
        code, text = self.run_harness("--checks", "dm", "--channel", PRIVATE_NAME)
        self.assertEqual(code, harness.EXIT_SETUP, text)
        self.assertIn(f"no public channel named #{PRIVATE_NAME} visible to the listed user, and its token lacks groups:read", text)
        self.assertIn("a private channel needs groups:read on the token or the channel's C or G id", text)
        self.assertEqual(self.world.conversation_list_types, ["public_channel,private_channel", "public_channel"])

    def test_another_conversations_list_error_is_reported_as_is(self):
        self.world.slack_error_override["conversations.list"] = "invalid_auth"
        code, text = self.run_harness("--checks", "dm", "--channel", CHANNEL_NAME)
        self.assertEqual(code, harness.EXIT_SETUP, text)
        self.assertIn("Slack conversations.list failed: invalid_auth", text)

    def test_a_dm_id_is_refused_for_the_channel(self):
        code, text = self.run_harness("--checks", "dm", "--channel", "D0ABCDEF1")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("D0ABCDEF1 is a DM, not a channel", text)
        self.assertNotIn("preflight-listed", text)
        self.assertEqual(harness.resolve_channel(None, "G0PRIVATE1"), "G0PRIVATE1")

    def test_a_non_object_error_body_keeps_the_status(self):
        self.world.secret_error_body = ["denied"]
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("Secret Manager refused test-project/slack-test-user-listed (HTTP 403): [\"denied\"]", text)
        self.assertNotIn("AttributeError", text)
        self.assertEqual(harness._error_detail(b"null"), "null")

    def test_an_unbounded_poll_is_not_swallowed_by_main(self):
        self.world.mode = "silent"
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(Unbounded):
            harness.main([*self.endpoint_args, "--checks", "dm", "--poll-interval", "0"],
                         clock=self.fake.clock, sleep=self.fake.sleep, wall=self.fake.wall)

    def test_bot_token_in_the_secret_is_refused_without_echoing_it(self):
        self.world.secrets[LISTED_SECRET] = "xoxb-9999-a-bot-token-value"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("holds a bot token, not a user token", text)
        self.assertNotIn("xoxb-9999-a-bot-token-value", text)

    def test_secret_manager_error_echoing_credentials_is_scrubbed(self):
        self.world.secret_error_body = {"error": {"message": f"denied for {GCP_ACCESS} and {LISTED_TOKEN}"}}
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("Secret Manager refused", text)
        self.assertIn(harness.REDACTED, text)

    def test_slack_error_echoing_the_token_is_scrubbed(self):
        self.world.slack_error_override["auth.test"] = f"token_revoked:{LISTED_TOKEN}"
        code, text = self.run_harness("--checks", "dm")
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("Slack auth.test failed: token_revoked:[redacted]", text)

    def test_unexpected_exception_carrying_the_token_is_scrubbed(self):
        class Exploding:
            def request(self, method, url, headers, body):
                raise ValueError(f"boom with {headers.get('Authorization', '')} {LISTED_TOKEN}")

        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            code = harness.main([*self.endpoint_args, "--checks", "dm"], transport=Exploding(),
                                clock=self.fake.clock, sleep=self.fake.sleep, wall=self.fake.wall)
        self.assertEqual(code, harness.EXIT_SETUP)
        self.assertIn("ERROR unexpected ValueError", out.getvalue())
        self.assertNotIn(LISTED_TOKEN, out.getvalue())

    def test_a_token_with_a_control_character_is_refused_by_name(self):
        for inner in ("\n", "\r", "\t", " ", "\x00", "\u2028"):
            with self.subTest(inner=repr(inner)):
                self.world.secrets[LISTED_SECRET] = f"xoxp-1111-head{inner}tail-secret-part"
                code, text = self.run_harness("--checks", "dm")
                self.assertEqual(code, harness.EXIT_SETUP, text)
                self.assertIn("the listed token source slack-test-user-listed holds a whitespace or control character", text)
                self.assertNotIn("tail-secret-part", text)
                self.assertNotIn("1111-head", text)

    def test_page_caps_say_the_lookup_was_cut_off(self):
        self.world.list_cursor = "more"
        code, text = self.run_harness("--checks", "dm", "--bot-name", "nobody")
        self.assertEqual(code, harness.EXIT_SETUP, text)
        self.assertIn(f"stopped after {harness.LIST_MAX_PAGES} pages of users.list", text)
        self.assertIn("pass --bot-user-id", text)
        code, text = self.run_harness("--checks", "dm", "--bot-user-id", BOT_ID, "--channel", "nope")
        self.assertEqual(code, harness.EXIT_SETUP, text)
        self.assertIn(f"stopped after {harness.LIST_MAX_PAGES} pages of conversations.list", text)
        self.assertIn("C or G id", text)
        # A channel found inside the cap still resolves.
        code, text = self.run_harness("--checks", "dm", "--bot-user-id", BOT_ID)
        self.assertEqual(code, harness.EXIT_OK, text)

    def test_rate_limit_is_retried(self):
        calls = []

        class Limited:
            def request(self, method, url, headers, body):
                calls.append(url)
                if len(calls) == 1:
                    return 429, {"Retry-After": "2"}, b""
                return 200, {}, b'{"ok": true, "user_id": "U1"}'

        client = harness.SlackClient(LISTED_TOKEN, Limited(), "http://x/", self.fake.sleep)
        self.assertEqual(client.call("auth.test")["user_id"], "U1")
        self.assertEqual(self.fake.slept, [2.0])

    def test_retry_after_is_clamped(self):
        default = harness.RATE_LIMIT_DEFAULT_WAIT_SECONDS
        for value, want in (("-3", default), ("nan", default), ("inf", default), ("x", default),
                            ("100", harness.RATE_LIMIT_MAX_WAIT_SECONDS), ("2", 2.0)):
            self.assertEqual(harness._retry_after({"Retry-After": value}), want, value)


class UnitTest(unittest.TestCase):
    def test_redactor_cuts_registered_and_token_shaped_values(self):
        redactor = harness.Redactor()
        redactor.add("plain-registered-value")
        out = redactor.redact("a plain-registered-value b xoxp-1-2-3 c ya29.abc-def d xapp-1-A-2")
        self.assertEqual(out, "a [redacted] b [redacted] c [redacted] d [redacted]")

    def test_a_reply_outside_the_expected_thread_fails(self):
        session = harness.Session(args=harness.parse_args(["--bot-name", "kage"]), listed=None, listed_user_id=LISTED_ID, bot_user_id=BOT_ID,
                                  channel_id=CHANNEL_ID, run_id="r", clock=None, sleep=None)
        reply = {"ts": "1.3", "thread_ts": "1.2", "text": "PONG", "user": BOT_ID}
        result = harness.judge_listed_reply(session, "mention", CHANNEL_ID, "1.1", reply, harness.KIND_ANSWER, reply, expect_thread="1.1")
        self.assertFalse(result.passed)
        self.assertIn("not threaded under 1.1", result.detail)
        reply["thread_ts"] = "1.1"
        self.assertTrue(harness.judge_listed_reply(session, "mention", CHANNEL_ID, "1.1", reply, harness.KIND_ANSWER, reply,
                                                   expect_thread="1.1").passed)

    def test_classify(self):
        self.assertEqual(harness.classify(REFUSAL.format(id="U1")), harness.KIND_REFUSAL)
        self.assertEqual(harness.classify("⏳ submitted…"), harness.KIND_TASK_LINE)
        self.assertEqual(harness.classify("⚙️ *working* — reading"), harness.KIND_TASK_LINE)
        self.assertEqual(harness.classify("❓ *input-required*"), harness.KIND_TASK_LINE)
        self.assertEqual(harness.classify("✅ *completed*"), harness.KIND_TASK_LINE)
        self.assertEqual(harness.classify("⚠️ could not send that to the running task; it is still working on the original instruction"),
                         harness.KIND_STEER)
        for notice in NOT_STARTED_NOTICES:
            self.assertEqual(harness.classify(notice), harness.KIND_NOT_STARTED, notice)
        self.assertEqual(harness.classify("✏️ steering sent — the worker picks it up"), harness.KIND_STEER)
        for failure in ("🚫 *rejected*", "❌ *failed* — the pod died", "🛑 *canceled*", "❌ could not reach the bus; try again",
                        "❌ failed: the executor is down", "❌ the task failed", "🛑 canceled",
                        "🚫 the executor rejected the task", "🚫 the executor rejected the task: no capability"):
            self.assertEqual(harness.classify(failure), harness.KIND_FAILURE, failure)
        # Not the gateway's grammar: the agent's own text, whatever it opens with.
        for answer in ("PONG", "✅ PONG", "✅ *done*", "⚙️ PONG", "❌ PONG", "🚫 rejected", "✏️ PONG", "🔎 task `t` is *completed*",
                       "ℹ️ PONG", "x ⛔ I can't verify who you are on slack"):
            self.assertEqual(harness.classify(answer), harness.KIND_ANSWER, answer)

    def test_status_grammar_matches_the_gateway_source(self):
        gateway = (REPO / "a2a" / "gateway" / "gateway.go").read_text()
        relay = (REPO / "a2a" / "gateway" / "relay.go").read_text()
        self.assertIn(f'g.adapter.Post(rec.Key, "{harness.STATUS_PLACEHOLDER}")', gateway)
        self.assertIn(f'"{harness.STATUS_BUS_FAILURE}"', gateway)
        self.assertIn(f'g.post(rec.Key, "{harness.STEER_FAILED_NOTICE}")', gateway)
        self.assertIn('g.post(rec.Key, "✏️ steering sent — ', gateway)
        self.assertIn('line := fmt.Sprintf("%s **%s**", icon, label)', relay)
        self.assertIn('line := fmt.Sprintf("%s **%s**", icon, state)', relay)
        self.assertEqual(relay.count('line += " — " + progress'), 2)
        for state, icon in harness.STATUS_ICONS.items():
            const = "lib.State" + "".join(part.capitalize() for part in state.split("-"))
            self.assertRegex(relay, rf'{const}:\s+"{icon}",', state)
        for post in ('"❌ failed: "+reason', '"❌ the task failed"', '"🛑 canceled"', '"🚫 the executor rejected the task: "+reason',
                     '"🚫 the executor rejected the task"'):
            self.assertIn(f"g.post(rec.Key, {post})", relay)

    def test_not_started_grammar_matches_the_gateway_source(self):
        # Every "not started:" the gateway can post, read off the source, is one the
        # harness reads as a refusal; and the test's renderings come from those formats.
        sources = {path.name: path.read_text() for path in (REPO / "a2a" / "gateway").glob("*.go")
                   if not path.name.endswith("_test.go")}
        formats = [fmt for src in sources.values() for fmt in re.findall(r'"((?:[^"\\]|\\.)*not started:(?:[^"\\]|\\.)*)"', src)]
        self.assertGreaterEqual(len(formats), 4, formats)
        rendered = set()
        for fmt in formats:
            self.assertTrue(harness.NOT_STARTED_PATTERN.match(fmt), fmt)
            literal = re.escape(fmt).replace("%d", "%s").replace("%s", ".+")
            matching = [n for n in NOT_STARTED_NOTICES if re.fullmatch(literal, n, re.DOTALL)]
            self.assertTrue(matching, f"no test rendering of {fmt!r}")
            rendered.update(matching)
        self.assertEqual(rendered, set(NOT_STARTED_NOTICES))
        # The session cap's count phrase, both forms.
        self.assertIn('workers := fmt.Sprintf("%d session workers are", live)', sources["spawn.go"])
        self.assertIn('workers = "1 session worker is"', sources["spawn.go"])
        # And each one is posted in place of a task: refuseAtSessionCap returns true
        # after both, and the retire refusal is the one passed to retireIncarnation.
        self.assertIn('retireRefusalNotStarted = "⚠️ not started: ', sources["gateway.go"])
        self.assertIn('g.post(rec.Key, "⚠️ not started: can\'t count the running session workers', sources["spawn.go"])
        self.assertIn('g.post(rec.Key, "⚠️ not started: could not mint this task\'s capability")', sources["gateway.go"])

    def test_redactor_cuts_the_escaped_forms_of_a_registered_value(self):
        redactor = harness.Redactor()
        redactor.add("head\ntail-é")
        for printed in (repr("head\ntail-é"), repr("head\ntail-é".encode()), "head%0Atail-%C3%A9"):
            self.assertNotIn("tail", redactor.redact(f"x {printed} y"), printed)

    def test_refusal_marker_matches_the_gateway_source(self):
        gateway = (REPO / "a2a" / "gateway" / "gateway.go").read_text()
        self.assertIn('"⛔ ' + harness.REFUSAL_NOTICE_MARKER.replace("slack", '"+backend+'), gateway)

    def test_turn_subtypes_match_the_gateway_source(self):
        slack = (REPO / "a2a" / "gateway" / "slack.go").read_text()
        self.assertIn('var slackTurnSubtypes = map[string]bool{"": true, "thread_broadcast": true, "file_share": true}', slack)
        self.assertEqual(harness.TURN_SUBTYPES, {"", "thread_broadcast", "file_share"})

    def test_poll_returns_early_and_times_out(self):
        fake = FakeClock()
        answers = iter([None, None, "got"])
        self.assertEqual(harness.poll(lambda: next(answers), 60, 5, fake.clock, fake.sleep), "got")
        self.assertEqual(fake.slept, [5, 5])
        fake = FakeClock()
        self.assertIsNone(harness.poll(lambda: None, 12, 5, fake.clock, fake.sleep))
        self.assertEqual(fake.slept, [5, 5, 2])

    def test_parse_checks(self):
        self.assertEqual(harness.parse_checks("thread,dm,mention"), ["dm", "mention", "thread"])
        self.assertEqual(harness.parse_checks("all"), list(harness.CHECKS_ALL))
        with self.assertRaises(Exception):
            harness.parse_checks("nope")

    def test_abbreviated_flags_are_refused(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            harness.parse_args(["--check", "home"])

    def test_time_budget_covers_every_wait(self):
        args = harness.parse_args(["--checks", "all,home", "--home-channel", "c", "--unlisted-repeat", "--bot-name", "kage"])
        self.assertEqual(harness.time_budget(args), 180 + 180 + 360 + 180 + 30 + 300)

    def test_an_invalid_home_match_is_refused_at_parse_time(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            harness.parse_args(["--checks", "home", "--home-channel", "c", "--home-match", "(", "--bot-name", "kage"])

    def test_non_finite_timeouts_are_refused(self):
        for flag in ("--reply-timeout", "--poll-interval", "--quiet-window", "--home-timeout", "--home-since"):
            for value in ("inf", "-inf", "nan", "infinity"):
                with self.subTest(flag=flag, value=value), contextlib.redirect_stderr(io.StringIO()) as err, \
                        self.assertRaises(SystemExit):
                    harness.parse_args([f"{flag}={value}", "--bot-name", "kage"])
                self.assertIn("finite", err.getvalue())

    def test_home_needs_a_channel(self):
        with contextlib.redirect_stderr(io.StringIO()) as err, self.assertRaises(SystemExit):
            harness.parse_args(["--checks", "home", "--bot-name", "kage"])
        self.assertIn("the home check needs --home-channel", err.getvalue())

    def test_the_bot_needs_a_name_or_an_id(self):
        # No default name: one workspace's bot name fails every other workspace.
        with contextlib.redirect_stderr(io.StringIO()) as err, self.assertRaises(SystemExit):
            harness.parse_args(["--checks", "dm"])
        self.assertIn("pass --bot-name <your bot's name> or --bot-user-id <its member id>", err.getvalue())
        self.assertEqual(harness.parse_args(["--checks", "dm", "--bot-name", "troisbocaux"]).bot_name, "troisbocaux")
        self.assertEqual(harness.parse_args(["--checks", "dm", "--bot-user-id", BOT_ID]).bot_user_id, BOT_ID)


if __name__ == "__main__":
    unittest.main()

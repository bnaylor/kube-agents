#!/usr/bin/env python3
"""Live checks for the A2A gateway's Slack backend, run inside the cluster as a Job.

Two Slack user accounts drive the gateway's bot the way a person would: one on
spec.integration.slack.allowedUsers (the "listed" user) and one not on it (the
"unlisted" user). Each check posts through the Slack Web API with that user's
token and polls for the bot's reply, then prints one PASS or FAIL line and one
EVIDENCE line of JSON (timestamps, channel, truncated reply text; never a token).

The user tokens are read at run time from Secret Manager over the GKE metadata
server's Workload Identity token. They stay in this process's memory: nothing here puts them in the environment, on disk, or in
output, and every line printed goes through redact() first.

Standard library only, so it runs in any image that has a python3; the launcher
(launch.py) mounts it from a ConfigMap into the agent-sandbox image. Never run in
CI. hack/slack-live-check/README.md has the setup and the run order.
"""

import argparse
import base64
import json
import math
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

SLACK_API_BASE = "https://slack.com/api/"
METADATA_TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
METADATA_FLAVOR_HEADER = "Metadata-Flavor"
METADATA_FLAVOR_VALUE = "Google"
SECRET_MANAGER_BASE = "https://secretmanager.googleapis.com/v1/"
SECRET_ACCESS_PATH_FORMAT = "projects/{project}/secrets/{secret}/versions/latest:access"

DEFAULT_PROJECT = "bnaylor-kagents-dev"
DEFAULT_LISTED_SECRET = "slack-test-user-listed"
DEFAULT_UNLISTED_SECRET = "slack-test-user-unlisted"
DEFAULT_CHANNEL = "ka-test"
DEFAULT_PROMPT = "Reply with the single word PONG."
DEFAULT_FOLLOWUP = "Once more, please: reply with the single word PONG."

DEFAULT_REPLY_TIMEOUT_SECONDS = 180
DEFAULT_POLL_INTERVAL_SECONDS = 5
DEFAULT_QUIET_WINDOW_SECONDS = 30
DEFAULT_HOME_TIMEOUT_SECONDS = 300
HTTP_TIMEOUT_SECONDS = 20
RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_DEFAULT_WAIT_SECONDS = 5
RATE_LIMIT_MAX_WAIT_SECONDS = 30
HTTP_OK = 200
HTTP_TOO_MANY_REQUESTS = 429
LIST_PAGE_LIMIT = 200
LIST_MAX_PAGES = 25
# conversations.list types: private channels need groups:read, which a minted test token may lack.
CHANNEL_TYPES_ALL = "public_channel,private_channel"
CHANNEL_TYPES_PUBLIC = "public_channel"
SLACK_MISSING_SCOPE = "missing_scope"
HISTORY_PAGE_LIMIT = 100
EVIDENCE_TEXT_LIMIT = 160
RUN_ID_HEX_CHARS = 8
ERROR_BODY_LIMIT = 200

# a2a/gateway/slack.go, slackTurnSubtypes: the subtypes inbound() takes as a user turn.
TURN_SUBTYPES = frozenset({"", "thread_broadcast", "file_share"})
# a2a/gateway/gateway.go, verifySender: the notice an unverified sender gets, once per
# sender per gateway process. The text after "on slack" names the sender's id.
REFUSAL_NOTICE_MARKER = "I can't verify who you are on slack"
REFUSAL_NOTICE_PREFIX = "⛔ " + REFUSAL_NOTICE_MARKER
# The gateway's own lines are told from the agent's answer by their whole text,
# never by a leading emoji: the answer is the agent's result posted verbatim
# (relay.go relayTerminal), so its first character is the agent's to choose.
#
# The rolling status line is one message per task: startTask (gateway.go) posts the
# placeholder before the task can produce anything, and relay.go edits that same
# message to statusLine's and then terminalLine's "<icon> **<state>**[ — <progress>]",
# which reaches Slack as mrkdwn "*<state>*". The deliverable, or the failure notice,
# is posted as a new message before the terminal edit. So the status line's current
# state is what says the answer is in; wait_for_bot reads the messages after it by
# that state, not by their own text.
STATUS_PLACEHOLDER = "⏳ submitted…"
# gateway.go startTask: the placeholder's edit when the publish fails.
STATUS_BUS_FAILURE = "❌ could not reach the bus; try again"
# relay.go statusLine and terminalLine. statusLine draws an unknown state with ⏳.
STATUS_ICONS = {
    "submitted": "⏳", "working": "⚙️", "input-required": "❓",
    "completed": "✅", "failed": "❌", "canceled": "🛑", "rejected": "🚫",
}
STATUS_LINE_PATTERN = re.compile(r"(\S+) \*([a-z-]+)\*(?: — .*)?", re.DOTALL)
STATE_SUBMITTED = "submitted"
STATE_COMPLETED = "completed"
FAILED_STATES = frozenset({"failed", "canceled", "rejected"})
# relay.go relayTerminal: the notices for a task that did not complete.
FAILURE_POST_PATTERN = re.compile(r"❌ failed: .*|❌ the task failed|🛑 canceled|🚫 the executor rejected the task(?:: .*)?", re.DOTALL)
# gateway.go steerTask: the steer acknowledgements, and the notice when the steer
# could not be published. Either one means the turn started no task.
STEER_ACK_PREFIX = "✏️ steering sent — "
STEER_FAILED_NOTICE = "⚠️ could not send that to the running task; it is still working on the original instruction"
# The gateway's warnings (gateway.go, spawn.go) all open with this. Read by prefix
# only ahead of the status line, where nothing is the agent's.
WARNING_PREFIX = "⚠️ "
KIND_REFUSAL = "refusal"
KIND_TASK_LINE = "task-line"
KIND_STEER = "steer-ack"
KIND_FAILURE = "failure"
KIND_WARNING = "warning"
KIND_ANSWER = "answer"
# What wait_for_bot waits for: the first reply (the status line, normally), the
# answer itself, or the task's end whatever it posted.
WAIT_FIRST = "first"
WAIT_ANSWER = "answer"
WAIT_SETTLED = "settled"

USER_TOKEN_PREFIXES = ("xoxp-", "xoxe.xoxp-")
NON_USER_TOKEN_KINDS = {"xoxb-": "a bot token", "xapp-": "an app-level token"}
# Anything shaped like a Slack or Google access token is cut from output even if
# it was never registered with the redactor (a token echoed back in an error body).
TOKEN_SHAPE_PATTERNS = (
    re.compile(r"xox[a-z]-[A-Za-z0-9-]+"),
    re.compile(r"xoxe\.[A-Za-z0-9.-]+"),
    re.compile(r"xapp-[A-Za-z0-9-]+"),
    re.compile(r"ya29\.[A-Za-z0-9._-]+"),
)
REDACTED = "[redacted]"
# Channel ids only. A D... id is a DM, where every message is a turn (inbound() in
# a2a/gateway/slack.go), so the preflight post there would start a task.
CHANNEL_ID_PATTERN = re.compile(r"^[CG][A-Z0-9]{6,}$")
DM_ID_PATTERN = re.compile(r"^D[A-Z0-9]{6,}$")
SLACK_NOT_IN_CHANNEL_ERRORS = frozenset({"not_in_channel", "channel_not_found"})

CHECK_DM = "dm"
CHECK_MENTION = "mention"
CHECK_THREAD = "thread"
CHECK_UNLISTED = "unlisted"
CHECK_RESTART = "restart"
CHECK_HOME = "home"
CHECK_PREFLIGHT = "preflight"
CHECK_UNLISTED_REPEAT = "unlisted-repeat"
# Run order: thread needs the thread mention roots.
CHECK_ORDER = (CHECK_DM, CHECK_MENTION, CHECK_THREAD, CHECK_UNLISTED, CHECK_RESTART, CHECK_HOME)
CHECKS_ALL_ALIAS = "all"
CHECKS_ALL = (CHECK_DM, CHECK_MENTION, CHECK_THREAD, CHECK_UNLISTED)
UNLISTED_VIA_DM = "dm"
UNLISTED_VIA_MENTION = "mention"

HARNESS_PROG = "harness.py"

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_SETUP = 2


class HarnessError(Exception):
    """A setup problem: the run cannot start, which is not a check failing."""


class SlackAPIError(HarnessError):
    """Slack answered ok:false, or a non-200 status. Carries Slack's error code only."""

    def __init__(self, method: str, error: str) -> None:
        super().__init__(f"Slack {method} failed: {error}")
        self.error = error


class Redactor:
    """Cuts every registered credential, and anything token-shaped, out of a string."""

    def __init__(self) -> None:
        self._values: set[str] = set()

    def add(self, secret_payload: str) -> None:
        if not secret_payload:
            return
        # The value, and the encodings an error message prints it in: repr() of the
        # str and of its bytes (http.client's "Invalid header value %r"), and URL
        # quoting. load_user_token refuses a token these differ for; this covers
        # whatever else is registered.
        self._values.update({
            secret_payload,
            repr(secret_payload)[1:-1],
            repr(secret_payload.encode("utf-8", "backslashreplace"))[2:-1],
            urllib.parse.quote(secret_payload, safe=""),
            urllib.parse.quote_plus(secret_payload, safe=""),
        })

    def redact(self, text: str) -> str:
        # Longest first, so a value that contains another is cut whole.
        for value in sorted(self._values, key=len, reverse=True):
            text = text.replace(value, REDACTED)
        for pattern in TOKEN_SHAPE_PATTERNS:
            text = pattern.sub(REDACTED, text)
        return text


REDACTOR = Redactor()


def say(line: str) -> None:
    """The one way this module writes output."""
    print(REDACTOR.redact(line), flush=True)


def truncate(text: str, limit: int = EVIDENCE_TEXT_LIMIT) -> str:
    return text if len(text) <= limit else text[:limit] + "…"


class UrllibTransport:
    """Plain HTTPS. Returns (status, headers, body) for any HTTP status."""

    def request(self, method: str, url: str, headers: dict, body: Optional[bytes]) -> tuple[int, dict, bytes]:
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:  # noqa: S310 -- fixed https/metadata URLs
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read() or b""
        except urllib.error.URLError as exc:
            host = urllib.parse.urlsplit(url).netloc
            raise HarnessError(f"cannot reach {host}: {exc.reason}") from None


def _error_detail(body: bytes) -> str:
    try:
        payload = json.loads(body)
    except ValueError:
        return truncate(body.decode("utf-8", "replace"), ERROR_BODY_LIMIT)
    if not isinstance(payload, dict):
        return truncate(body.decode("utf-8", "replace"), ERROR_BODY_LIMIT)
    err = payload.get("error")
    if isinstance(err, dict):
        return truncate(str(err.get("message") or err.get("status") or ""), ERROR_BODY_LIMIT)
    return truncate(str(err or payload.get("error_description") or ""), ERROR_BODY_LIMIT)


class SecretManagerReader:
    """Reads a secret's latest version over REST with the pod's Workload Identity token."""

    def __init__(self, transport, project: str, token_url: str = METADATA_TOKEN_URL, base: str = SECRET_MANAGER_BASE) -> None:
        self.transport = transport
        self.project = project
        self.token_url = token_url
        self.base = base

    def _access_oauth_token(self) -> str:
        status, _, body = self.transport.request(
            "GET", self.token_url, {METADATA_FLAVOR_HEADER: METADATA_FLAVOR_VALUE}, None
        )
        if status != HTTP_OK:
            raise HarnessError(
                f"the metadata server refused a token (HTTP {status}): {_error_detail(body)}. "
                "Is the Job's ServiceAccount bound to the GSA by Workload Identity?"
            )
        oauth_token = json.loads(body).get("access_token", "")
        REDACTOR.add(oauth_token)
        if not oauth_token:
            raise HarnessError("the metadata server answered without an access token")
        return oauth_token

    def read(self, name: str) -> str:
        oauth_token = self._access_oauth_token()
        url = self.base + SECRET_ACCESS_PATH_FORMAT.format(project=self.project, secret=name)
        status, _, body = self.transport.request("GET", url, {"Authorization": f"Bearer {oauth_token}"}, None)
        if status != HTTP_OK:
            raise HarnessError(f"Secret Manager refused {self.project}/{name} (HTTP {status}): {_error_detail(body)}")
        data = json.loads(body).get("payload", {}).get("data", "")
        secret_payload = base64.b64decode(data).decode("utf-8").strip()
        REDACTOR.add(secret_payload)
        return secret_payload


def load_user_token(reader, name: str, label: str) -> str:
    """Reads one user token and refuses anything that is not a Slack user token."""
    user_oauth_token = reader.read(name)
    if not user_oauth_token:
        raise HarnessError(f"the {label} token source {name} is empty")
    # A token is one printable word. One with a newline or a control character inside
    # it would fail in http.client with an error that prints the value escaped, in a
    # form the redactor's literal match does not see. Name the secret, never the value.
    if any(ch.isspace() or unicodedata.category(ch).startswith("C") for ch in user_oauth_token):
        raise HarnessError(f"the {label} token source {name} holds a whitespace or control character inside the value; "
                           "store the token alone, on one line")
    for prefix, kind in NON_USER_TOKEN_KINDS.items():
        if user_oauth_token.startswith(prefix):
            raise HarnessError(f"the {label} token source {name} holds {kind}, not a user token (xoxp-)")
    if not user_oauth_token.startswith(USER_TOKEN_PREFIXES):
        raise HarnessError(f"the {label} token source {name} does not hold a Slack user token (xoxp-)")
    return user_oauth_token


class SlackClient:
    """The Slack Web API over plain HTTPS, as one user."""

    def __init__(self, oauth_token: str, transport, base: str = SLACK_API_BASE, sleep: Callable[[float], None] = time.sleep) -> None:
        REDACTOR.add(oauth_token)
        self._oauth_token = oauth_token
        self.transport = transport
        self.base = base
        self.sleep = sleep

    def call(self, method: str, **params) -> dict:
        body = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}).encode()
        headers = {
            "Authorization": f"Bearer {self._oauth_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
            status, resp_headers, raw = self.transport.request("POST", self.base + method, headers, body)
            if status != HTTP_TOO_MANY_REQUESTS or attempt == RATE_LIMIT_MAX_RETRIES:
                break
            self.sleep(_retry_after(resp_headers))
        if status != HTTP_OK:
            raise SlackAPIError(method, f"http_{status}")
        try:
            payload = json.loads(raw)
        except ValueError:
            raise SlackAPIError(method, "invalid_json") from None
        if not payload.get("ok"):
            raise SlackAPIError(method, str(payload.get("error", "unknown_error")))
        return payload


def _retry_after(headers: dict) -> float:
    for key, value in headers.items():
        if key.lower() == "retry-after":
            try:
                wait = float(value)
            except ValueError:
                break
            # A negative or non-finite value would make time.sleep raise.
            if not math.isfinite(wait) or wait < 0:
                break
            return min(wait, RATE_LIMIT_MAX_WAIT_SECONDS)
    return RATE_LIMIT_DEFAULT_WAIT_SECONDS


def ts_value(ts: str) -> Decimal:
    try:
        return Decimal(ts)
    except (InvalidOperation, TypeError):
        return Decimal(0)


def poll(fetch: Callable[[], Optional[object]], timeout: float, interval: float,
         clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep) -> Optional[object]:
    """Calls fetch until it returns something, or until timeout seconds have passed."""
    deadline = clock() + timeout
    while True:
        result = fetch()
        if result is not None:
            return result
        remaining = deadline - clock()
        if remaining <= 0:
            return None
        sleep(min(interval, remaining))


def status_state(text: str) -> str:
    """The task state a status line shows, or "" when text is not in the status line's grammar."""
    text = text.strip()
    if text == STATUS_PLACEHOLDER:
        return STATE_SUBMITTED
    if text == STATUS_BUS_FAILURE:
        return "failed"
    match = STATUS_LINE_PATTERN.fullmatch(text)
    if not match:
        return ""
    icon, state = match.groups()
    return state if STATUS_ICONS.get(state, STATUS_ICONS[STATE_SUBMITTED]) == icon else ""


def classify(text: str) -> str:
    """What a bot message's text is, by the gateway's grammar: anything outside it is an answer.

    Text alone cannot tell a warning or a failure notice from an answer that opens
    the same way; read_turn decides those by where the message sits.
    """
    stripped = text.strip()
    if stripped.startswith(REFUSAL_NOTICE_PREFIX):
        return KIND_REFUSAL
    if stripped.startswith(STEER_ACK_PREFIX) or stripped == STEER_FAILED_NOTICE:
        return KIND_STEER
    state = status_state(stripped)
    if state:
        return KIND_FAILURE if state in FAILED_STATES else KIND_TASK_LINE
    if FAILURE_POST_PATTERN.fullmatch(stripped):
        return KIND_FAILURE
    if stripped.startswith(WARNING_PREFIX):
        return KIND_WARNING
    return KIND_ANSWER


def read_turn(bot_msgs: list[dict], mode: str) -> Optional[tuple[dict, str]]:
    """The bot's reply to one turn, from its messages since the turn, oldest first.

    The status line is the first message in its grammar. Everything before it is
    the gateway's own (a refusal, a steer outcome, a warning), so it is read by its
    text. Everything after it is read by the status line's state: the deliverable
    or the failure notice is posted before the terminal edit, so once the line is
    terminal the last message after it is that post, whatever its first character.
    """
    status_at = next((i for i, m in enumerate(bot_msgs) if status_state(m.get("text", ""))), None)
    before = bot_msgs if status_at is None else bot_msgs[:status_at]
    for msg in before:
        kind = classify(msg.get("text", ""))
        if kind in (KIND_REFUSAL, KIND_STEER):
            return msg, kind
    if status_at is None:
        if mode != WAIT_FIRST:
            return None
        replies = [m for m in before if classify(m.get("text", "")) != KIND_WARNING]
        return (replies[0], classify(replies[0].get("text", ""))) if replies else None
    status = bot_msgs[status_at]
    state = status_state(status.get("text", ""))
    after = bot_msgs[status_at + 1:]
    if mode == WAIT_FIRST:
        return status, KIND_FAILURE if state in FAILED_STATES else KIND_TASK_LINE
    if state in FAILED_STATES:
        notices = [m for m in after if FAILURE_POST_PATTERN.fullmatch(m.get("text", "").strip())]
        return (notices[-1] if notices else status), KIND_FAILURE
    if state != STATE_COMPLETED:
        return None
    if after:
        return after[-1], KIND_ANSWER
    # Completed with nothing posted after the line (a post the gateway failed to
    # deliver): the task has ended, but there is no answer to show.
    return (status, KIND_TASK_LINE) if mode == WAIT_SETTLED else None


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    evidence: dict = field(default_factory=dict)


def report(result: CheckResult) -> None:
    verdict = "PASS" if result.passed else "FAIL"
    say(f"{verdict} {result.name}: {result.detail}")
    say("EVIDENCE " + json.dumps({"check": result.name, "passed": result.passed, **result.evidence}, ensure_ascii=False, sort_keys=True))


@dataclass
class Session:
    args: argparse.Namespace
    listed: SlackClient
    listed_user_id: str
    bot_user_id: str
    channel_id: str
    run_id: str
    clock: Callable[[], float]
    sleep: Callable[[float], None]
    unlisted: Optional[SlackClient] = None
    unlisted_user_id: str = ""
    mention_root: str = ""

    def tag(self, check: str) -> str:
        return f"(slack-live-check {self.run_id} {check})"


def history_after(client: SlackClient, channel: str, after_ts: str) -> list[dict]:
    """Top-level messages in channel newer than after_ts, oldest first."""
    resp = client.call("conversations.history", channel=channel, oldest=after_ts, limit=HISTORY_PAGE_LIMIT)
    msgs = [m for m in resp.get("messages", []) if ts_value(m.get("ts", "")) > ts_value(after_ts)]
    return sorted(msgs, key=lambda m: ts_value(m.get("ts", "")))


def replies_after(client: SlackClient, channel: str, root_ts: str, after_ts: str) -> list[dict]:
    """Replies in the thread rooted at root_ts newer than after_ts, oldest first."""
    resp = client.call("conversations.replies", channel=channel, ts=root_ts, oldest=after_ts, limit=HISTORY_PAGE_LIMIT)
    msgs = [m for m in resp.get("messages", []) if ts_value(m.get("ts", "")) > ts_value(after_ts)]
    return sorted(msgs, key=lambda m: ts_value(m.get("ts", "")))


def answer_mode(session: Session) -> str:
    return WAIT_ANSWER if session.args.wait_answer else WAIT_FIRST


def wait_for_bot(session: Session, fetch: Callable[[], list[dict]], mode: str, timeout: float) -> tuple[Optional[dict], str, Optional[dict]]:
    """Polls fetch for the bot's reply (read_turn). Returns (reply, kind, last bot message seen).

    A refusal notice or a steer outcome returns at once in every mode: either one
    means the turn did not start a task.
    """
    seen: dict = {}

    def step():
        bot_msgs = [m for m in fetch() if m.get("user") == session.bot_user_id]
        if not bot_msgs:
            return None
        seen["last"] = bot_msgs[-1]
        return read_turn(bot_msgs, mode)

    found = poll(step, timeout, session.args.poll_interval, session.clock, session.sleep)
    if found is None:
        return None, "", seen.get("last")
    reply, kind = found
    return reply, kind, seen.get("last")


def reply_evidence(channel: str, sent_ts: str, author: str, reply: Optional[dict], kind: str = "") -> dict:
    evidence = {"channel": channel, "sent_ts": sent_ts, "author": author}
    if reply is not None:
        evidence.update({
            "reply_ts": reply.get("ts", ""),
            "reply_thread_ts": reply.get("thread_ts", ""),
            "reply_kind": kind,
            "reply_text": truncate(reply.get("text", "")),
        })
    return evidence


def judge_listed_reply(session: Session, name: str, channel: str, sent_ts: str, reply: Optional[dict], kind: str,
                       last_seen: Optional[dict], expect_thread: str = "") -> CheckResult:
    """The verdict for a listed user's turn: a reply that is not a refusal, in the right place."""
    evidence = reply_evidence(channel, sent_ts, session.listed_user_id, reply, kind)
    timeout = session.args.reply_timeout
    if reply is None:
        if last_seen is not None:
            evidence["last_bot_text"] = truncate(last_seen.get("text", ""))
            missing = "no answer (--wait-answer)" if session.args.wait_answer else "no reply but a warning"
            return CheckResult(name, False, f"the bot posted {missing} within {timeout}s", evidence)
        return CheckResult(name, False, f"no reply from the bot within {timeout}s", evidence)
    if kind == KIND_REFUSAL:
        return CheckResult(name, False, "the listed user got the refusal notice; is the member on allowedUsers, and in the a2a-slack-principal-map Secret where the gateway requires it?", evidence)
    if kind == KIND_STEER:
        return CheckResult(name, False, "the turn was taken as a steer of a task still running, not as a new turn", evidence)
    if kind == KIND_FAILURE:
        return CheckResult(name, False, "the task the turn started failed", evidence)
    if expect_thread and reply.get("thread_ts") != expect_thread:
        return CheckResult(name, False, f"the reply is not threaded under {expect_thread}", evidence)
    where = f"thread {expect_thread}" if expect_thread else channel
    return CheckResult(name, True, f"{kind} in {where} at ts={reply.get('ts')} (sent ts={sent_ts})", evidence)


def open_dm(client: SlackClient, user_id: str) -> str:
    return client.call("conversations.open", users=user_id)["channel"]["id"]


def check_dm(session: Session, name: str = CHECK_DM) -> CheckResult:
    channel = open_dm(session.listed, session.bot_user_id)
    sent = session.listed.call("chat.postMessage", channel=channel, text=f"{session.args.prompt} {session.tag(name)}")
    sent_ts = sent["ts"]
    reply, kind, last = wait_for_bot(session, lambda: history_after(session.listed, channel, sent_ts),
                                     answer_mode(session), session.args.reply_timeout)
    return judge_listed_reply(session, name, channel, sent_ts, reply, kind, last)


def check_mention(session: Session) -> CheckResult:
    text = f"<@{session.bot_user_id}> {session.args.prompt} {session.tag(CHECK_MENTION)}"
    sent = session.listed.call("chat.postMessage", channel=session.channel_id, text=text)
    sent_ts = sent["ts"]
    reply, kind, last = wait_for_bot(session, lambda: replies_after(session.listed, session.channel_id, sent_ts, sent_ts),
                                     answer_mode(session), session.args.reply_timeout)
    result = judge_listed_reply(session, CHECK_MENTION, session.channel_id, sent_ts, reply, kind, last, expect_thread=sent_ts)
    if result.passed:
        session.mention_root = sent_ts
    return result


def check_thread(session: Session) -> CheckResult:
    root = session.mention_root or session.args.thread_ts or ""
    if not root:
        return CheckResult(CHECK_THREAD, False, "no thread to reply in: the mention check did not pass in this run and --thread-ts is unset",
                           {"channel": session.channel_id})
    # Let the task the mention started finish first. Its answer would otherwise
    # land after the follow-up and read as the follow-up's reply, and a reply
    # sent while it runs is a steer, not a turn. Settled is the status line's
    # terminal state, whatever the answer says.
    settled, settled_kind, last = wait_for_bot(session, lambda: replies_after(session.listed, session.channel_id, root, root),
                                               WAIT_SETTLED, session.args.reply_timeout)
    if settled is None:
        evidence = {"channel": session.channel_id, "thread_ts": root}
        if last is not None:
            evidence["last_bot_text"] = truncate(last.get("text", ""))
        return CheckResult(CHECK_THREAD, False, f"the thread's first task did not settle within {session.args.reply_timeout}s, "
                           "so a reply now would be a steer", evidence)
    text = f"{session.args.followup} {session.tag(CHECK_THREAD)}"
    sent = session.listed.call("chat.postMessage", channel=session.channel_id, thread_ts=root, text=text)
    sent_ts = sent["ts"]
    reply, kind, last = wait_for_bot(session, lambda: replies_after(session.listed, session.channel_id, root, sent_ts),
                                     answer_mode(session), session.args.reply_timeout)
    result = judge_listed_reply(session, CHECK_THREAD, session.channel_id, sent_ts, reply, kind, last, expect_thread=root)
    result.evidence["thread_ts"] = root
    result.evidence["settled_kind"] = settled_kind
    return result


def check_unlisted(session: Session) -> list[CheckResult]:
    assert session.unlisted is not None
    client = session.unlisted
    if session.args.unlisted_via == UNLISTED_VIA_MENTION:
        channel = session.channel_id
        text = f"<@{session.bot_user_id}> {session.args.prompt} {session.tag(CHECK_UNLISTED)}"
        sent_ts = client.call("chat.postMessage", channel=channel, text=text)["ts"]

        def fetch():
            return replies_after(client, channel, sent_ts, sent_ts)
    else:
        channel = open_dm(client, session.bot_user_id)
        sent_ts = client.call("chat.postMessage", channel=channel, text=f"{session.args.prompt} {session.tag(CHECK_UNLISTED)}")["ts"]

        def fetch():
            return history_after(client, channel, sent_ts)

    reply, kind, _ = wait_for_bot(session, fetch, WAIT_FIRST, session.args.reply_timeout)
    evidence = reply_evidence(channel, sent_ts, session.unlisted_user_id, reply, kind)
    evidence["via"] = session.args.unlisted_via
    if reply is None:
        if session.args.refusal_silence_ok:
            result = CheckResult(CHECK_UNLISTED, True,
                                 f"no reply within {session.args.reply_timeout}s, accepted under --refusal-silence-ok "
                                 "(the notice is sent once per sender per gateway process)", evidence)
        else:
            result = CheckResult(CHECK_UNLISTED, False,
                                 f"no reply within {session.args.reply_timeout}s. The notice is sent once per sender per "
                                 "gateway process, so a second run against the same gateway is silent; restart the "
                                 "gateway, or pass --refusal-silence-ok to accept silence", evidence)
        return [result]
    if kind != KIND_REFUSAL:
        return [CheckResult(CHECK_UNLISTED, False, f"the unlisted user was answered ({kind}), not refused", evidence)]
    if session.unlisted_user_id not in reply.get("text", ""):
        return [CheckResult(CHECK_UNLISTED, False, "the refusal notice does not name the unlisted user's member id", evidence)]
    results = [CheckResult(CHECK_UNLISTED, True, f"refusal notice at ts={reply.get('ts')} (sent ts={sent_ts})", evidence)]
    if session.args.unlisted_repeat:
        results.append(check_unlisted_repeat(session, client, channel, reply.get("thread_ts", "") or ""))
    return results


def check_unlisted_repeat(session: Session, client: SlackClient, channel: str, thread_ts: str) -> CheckResult:
    """A second message from the unlisted user draws nothing: the notice is once per sender."""
    text = f"{session.args.prompt} {session.tag(CHECK_UNLISTED_REPEAT)}"
    if session.args.unlisted_via == UNLISTED_VIA_MENTION:
        # Unmentioned, a reply in a thread the gateway never started a task in is
        # not a turn at all, and would pass here without reaching verifySender.
        text = f"<@{session.bot_user_id}> {text}"
    if thread_ts:
        sent_ts = client.call("chat.postMessage", channel=channel, thread_ts=thread_ts, text=text)["ts"]

        def fetch():
            return replies_after(client, channel, thread_ts, sent_ts)
    else:
        sent_ts = client.call("chat.postMessage", channel=channel, text=text)["ts"]

        def fetch():
            return history_after(client, channel, sent_ts)

    reply, kind, _ = wait_for_bot(session, fetch, WAIT_FIRST, session.args.quiet_window)
    evidence = reply_evidence(channel, sent_ts, session.unlisted_user_id, reply, kind)
    if reply is not None:
        return CheckResult(CHECK_UNLISTED_REPEAT, False, f"the bot replied to the second message ({kind})", evidence)
    return CheckResult(CHECK_UNLISTED_REPEAT, True, f"no reply to the second message in {session.args.quiet_window}s", evidence)


def check_home(session: Session, home_channel: str, since: float) -> CheckResult:
    pattern = re.compile(session.args.home_match) if session.args.home_match else None
    oldest = f"{since:.6f}"

    def step():
        for msg in history_after(session.listed, home_channel, oldest):
            if msg.get("user") != session.bot_user_id:
                continue
            if pattern is None or pattern.search(msg.get("text", "")):
                return msg
        return None

    msg = poll(step, session.args.home_timeout, session.args.poll_interval, session.clock, session.sleep)
    evidence = {"channel": home_channel, "since": oldest}
    if msg is None:
        return CheckResult(CHECK_HOME, False, f"no bot post in {home_channel} within {session.args.home_timeout}s", evidence)
    evidence.update({"reply_ts": msg.get("ts", ""), "reply_text": truncate(msg.get("text", ""))})
    return CheckResult(CHECK_HOME, True, f"bot post in {home_channel} at ts={msg.get('ts')}", evidence)


def preflight(client: SlackClient, label: str, user_id: str, channel_id: str, run_id: str) -> CheckResult:
    """Posts with a user token and reads the post back: the gateway must see a user turn.

    inbound() in a2a/gateway/slack.go drops any message with a bot_id, a subtype
    outside slackTurnSubtypes, or no user. A user-token post that came back with
    any of those would be thrown away as bot traffic and every later check would
    time out for a reason that has nothing to do with the gateway. The post is not
    addressed to the bot, so the gateway does not take it as a turn.
    """
    name = f"{CHECK_PREFLIGHT}-{label}"
    text = f"slack-live-check {run_id} preflight for the {label} user; not addressed to the bot"
    where = channel_id
    try:
        posted = client.call("chat.postMessage", channel=channel_id, text=text)
    except SlackAPIError as exc:
        if exc.error not in SLACK_NOT_IN_CHANNEL_ERRORS:
            raise
        # Not a member of the test channel: post to the user's own DM instead, which the bot cannot see.
        where = open_dm(client, user_id)
        posted = client.call("chat.postMessage", channel=where, text=text)
    ts = posted["ts"]
    channel = posted.get("channel", where)
    resp = client.call("conversations.history", channel=channel, latest=ts, oldest=ts, inclusive="true", limit=1)
    msg = next((m for m in resp.get("messages", []) if m.get("ts") == ts), None)
    evidence = {"channel": channel, "sent_ts": ts, "author": user_id}
    if msg is None:
        return CheckResult(name, False, f"the post at ts={ts} could not be read back", evidence)
    evidence.update({
        "bot_id": msg.get("bot_id", ""),
        "subtype": msg.get("subtype", ""),
        "user": msg.get("user", ""),
        "app_id": msg.get("app_id", ""),
    })
    problems = []
    if msg.get("bot_id"):
        problems.append(f"it carries bot_id={msg['bot_id']}")
    if msg.get("subtype", "") not in TURN_SUBTYPES:
        problems.append(f"its subtype {msg['subtype']!r} is not a turn")
    if not msg.get("user"):
        problems.append("it has no user field")
    elif msg["user"] != user_id:
        problems.append(f"its user {msg['user']} is not the token's user {user_id}")
    app_note = f"; app_id={msg['app_id']} (the gateway's filter ignores app_id)" if msg.get("app_id") else ""
    if problems:
        return CheckResult(name, False, "the gateway would discard this user-token post as bot traffic: "
                           + "; ".join(problems) + app_note, evidence)
    return CheckResult(name, True, f"user={msg['user']}, no bot_id, subtype {msg.get('subtype', '')!r}{app_note}", evidence)


def resolve_bot(client: SlackClient, bot_user_id: str, bot_name: str) -> str:
    if not bot_user_id:
        matches = []
        cursor = None
        for _ in range(LIST_MAX_PAGES):
            resp = client.call("users.list", limit=LIST_PAGE_LIMIT, cursor=cursor)
            for member in resp.get("members", []):
                profile = member.get("profile", {})
                names = {member.get("name"), member.get("real_name"), profile.get("display_name"), profile.get("real_name")}
                if member.get("is_bot") and not member.get("deleted") and bot_name in names:
                    matches.append(member["id"])
            cursor = resp.get("response_metadata", {}).get("next_cursor") or None
            if not cursor:
                break
        if not matches and cursor:
            raise HarnessError(f"no bot user named {bot_name!r} before the lookup stopped after {LIST_MAX_PAGES} pages of "
                               "users.list, so the workspace may hold it further on; pass --bot-user-id")
        if not matches:
            raise HarnessError(f"no bot user named {bot_name!r} in the workspace; pass --bot-user-id")
        if len(matches) > 1:
            raise HarnessError(f"more than one bot user is named {bot_name!r} ({', '.join(matches)}); pass --bot-user-id")
        bot_user_id = matches[0]
    info = client.call("users.info", user=bot_user_id)["user"]
    if not info.get("is_bot"):
        raise HarnessError(f"{bot_user_id} is not a bot user")
    return bot_user_id


def find_channel(client: SlackClient, name: str, types: str) -> tuple[str, bool]:
    """The id of the channel named name among types, or "", and whether the lookup stopped at the page cap."""
    cursor = None
    for _ in range(LIST_MAX_PAGES):
        resp = client.call("conversations.list", types=types, exclude_archived="true", limit=LIST_PAGE_LIMIT, cursor=cursor)
        for conv in resp.get("channels", []):
            if conv.get("name") == name:
                return conv["id"], False
        cursor = resp.get("response_metadata", {}).get("next_cursor") or None
        if not cursor:
            return "", False
    return "", True


def resolve_channel(client: SlackClient, channel: str) -> str:
    name = channel.lstrip("#")
    if DM_ID_PATTERN.match(name):
        raise HarnessError(f"{name} is a DM, not a channel: in a DM every message is a turn, so the preflight "
                           "post would start a task; pass a channel name or a C/G channel id")
    if CHANNEL_ID_PATTERN.match(name):
        return name
    public_only = False
    try:
        found, truncated = find_channel(client, name, CHANNEL_TYPES_ALL)
    except SlackAPIError as exc:
        # Listing private channels needs groups:read, which a user token minted
        # for the check may not carry. Public channels need only channels:read.
        if exc.error != SLACK_MISSING_SCOPE:
            raise
        public_only = True
        found, truncated = find_channel(client, name, CHANNEL_TYPES_PUBLIC)
    if found:
        return found
    if truncated:
        raise HarnessError(f"no channel named #{name} before the lookup stopped after {LIST_MAX_PAGES} pages of "
                           "conversations.list, so it may be further on; pass the channel's C or G id instead of its name")
    if public_only:
        raise HarnessError(f"no public channel named #{name} visible to the listed user, and its token lacks groups:read, "
                           "so private channels were not searched; a private channel needs groups:read on the token "
                           "or the channel's C or G id instead of its name")
    raise HarnessError(f"no channel named #{name} visible to the listed user")


def parse_checks(value: str) -> list[str]:
    requested: list[str] = []
    for item in (part.strip() for part in value.split(",")):
        if not item:
            continue
        if item == CHECKS_ALL_ALIAS:
            requested.extend(CHECKS_ALL)
        elif item in CHECK_ORDER:
            requested.append(item)
        else:
            raise argparse.ArgumentTypeError(f"unknown check {item!r}; choose from {', '.join(CHECK_ORDER)} or {CHECKS_ALL_ALIAS}")
    if not requested:
        raise argparse.ArgumentTypeError("no checks selected")
    return [check for check in CHECK_ORDER if check in requested]


def regex(value: str) -> str:
    try:
        re.compile(value)
    except re.error as exc:
        raise argparse.ArgumentTypeError(f"not a valid regex: {exc}") from None
    return value


def finite_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {value!r}") from None
    # float() admits inf and nan; neither is a wait, and the launcher's Job deadline
    # cannot be derived from one.
    if not math.isfinite(number):
        raise argparse.ArgumentTypeError(f"must be a finite number of seconds, not {value!r}")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=HARNESS_PROG, description="Live checks for the A2A gateway's Slack backend (run in-cluster by launch.py).",
                                     allow_abbrev=False)
    parser.add_argument("--checks", type=parse_checks, default=list(CHECKS_ALL),
                        help=f"comma list from {', '.join(CHECK_ORDER)}; '{CHECKS_ALL_ALIAS}' is {','.join(CHECKS_ALL)}")
    parser.add_argument("--keep-going", action="store_true", help="run every check after a FAIL")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="Secret Manager project")
    parser.add_argument("--listed-secret", default=DEFAULT_LISTED_SECRET, help="Secret Manager secret holding the listed user's token")
    parser.add_argument("--unlisted-secret", default=DEFAULT_UNLISTED_SECRET, help="Secret Manager secret holding the unlisted user's token")
    parser.add_argument("--bot-user-id", default="", help="the gateway bot's member id; looked up by --bot-name when unset")
    parser.add_argument("--bot-name", default="", help="the gateway bot's name in this workspace; required unless --bot-user-id is given")
    parser.add_argument("--channel", default=DEFAULT_CHANNEL, help="channel name or id for preflight, mention and thread")
    parser.add_argument("--thread-ts", default="", help="thread root for the thread check when mention is not run")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--followup", default=DEFAULT_FOLLOWUP)
    parser.add_argument("--wait-answer", action="store_true", help="wait past the status line for the answer itself")
    parser.add_argument("--reply-timeout", type=finite_float, default=DEFAULT_REPLY_TIMEOUT_SECONDS)
    parser.add_argument("--poll-interval", type=finite_float, default=DEFAULT_POLL_INTERVAL_SECONDS)
    parser.add_argument("--unlisted-via", choices=(UNLISTED_VIA_DM, UNLISTED_VIA_MENTION), default=UNLISTED_VIA_DM)
    parser.add_argument("--unlisted-repeat", action="store_true", help="also send a second message and expect no reply")
    parser.add_argument("--quiet-window", type=finite_float, default=DEFAULT_QUIET_WINDOW_SECONDS)
    parser.add_argument("--refusal-silence-ok", action="store_true", help="accept silence for the unlisted check")
    parser.add_argument("--home-channel", default="", help="channel the home check watches for a bot post")
    parser.add_argument("--home-since", type=finite_float, default=0.0, help="epoch seconds; default is the run's start")
    parser.add_argument("--home-timeout", type=finite_float, default=DEFAULT_HOME_TIMEOUT_SECONDS)
    parser.add_argument("--home-match", type=regex, default="", help="regex the home post's text must match")
    parser.add_argument("--run-id", default="", help="tag for this run's posts; random when unset")
    # Endpoint overrides for the offline tests.
    parser.add_argument("--slack-api-base", default=SLACK_API_BASE, help=argparse.SUPPRESS)
    parser.add_argument("--metadata-token-url", default=METADATA_TOKEN_URL, help=argparse.SUPPRESS)
    parser.add_argument("--secret-manager-base", default=SECRET_MANAGER_BASE, help=argparse.SUPPRESS)
    return parser


def parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.bot_user_id and not args.bot_name:
        # No default: each workspace names its bot, and a default that matches one
        # workspace fails every other one with "no bot user named".
        parser.error("pass --bot-name <your bot's name> or --bot-user-id <its member id>; there is no default bot name")
    if CHECK_HOME in args.checks and not args.home_channel:
        parser.error("the home check needs --home-channel")
    return args


def time_budget(args: argparse.Namespace) -> float:
    """The longest the selected checks can wait on Slack, for the Job's deadline."""
    per_check = {
        CHECK_DM: args.reply_timeout,
        CHECK_MENTION: args.reply_timeout,
        # The mention's task settling, then the reply to the follow-up.
        CHECK_THREAD: 2 * args.reply_timeout,
        CHECK_UNLISTED: args.reply_timeout + (args.quiet_window if args.unlisted_repeat else 0),
        CHECK_RESTART: args.reply_timeout,
        CHECK_HOME: args.home_timeout,
    }
    return sum(per_check[check] for check in args.checks)


def run(args: argparse.Namespace, transport, clock, sleep, wall) -> int:
    started = wall()
    run_id = args.run_id or uuid.uuid4().hex[:RUN_ID_HEX_CHARS]
    reader = SecretManagerReader(transport, args.project, args.metadata_token_url, args.secret_manager_base)
    needs_unlisted = CHECK_UNLISTED in args.checks

    listed = SlackClient(load_user_token(reader, args.listed_secret, "listed"), transport, args.slack_api_base, sleep)
    listed_auth = listed.call("auth.test")
    unlisted = None
    unlisted_user_id = ""
    if needs_unlisted:
        unlisted = SlackClient(load_user_token(reader, args.unlisted_secret, "unlisted"), transport, args.slack_api_base, sleep)
        unlisted_user_id = unlisted.call("auth.test")["user_id"]
        if unlisted_user_id == listed_auth["user_id"]:
            raise HarnessError("the listed and unlisted tokens belong to the same Slack user")
    bot_user_id = resolve_bot(listed, args.bot_user_id, args.bot_name)
    if bot_user_id in (listed_auth["user_id"], unlisted_user_id):
        raise HarnessError("the bot user id is one of the test users")
    channel_id = resolve_channel(listed, args.channel)
    home_channel = resolve_channel(listed, args.home_channel) if CHECK_HOME in args.checks else ""
    say(f"RUN {run_id}: team={listed_auth.get('team_id', '')} listed={listed_auth['user_id']} "
        f"unlisted={unlisted_user_id or '-'} bot={bot_user_id} channel={channel_id} checks={','.join(args.checks)}")

    session = Session(args=args, listed=listed, listed_user_id=listed_auth["user_id"], bot_user_id=bot_user_id,
                      channel_id=channel_id, run_id=run_id, clock=clock, sleep=sleep,
                      unlisted=unlisted, unlisted_user_id=unlisted_user_id)
    results: list[CheckResult] = []

    preflights = [(listed, "listed", session.listed_user_id)]
    if unlisted is not None:
        preflights.append((unlisted, "unlisted", unlisted_user_id))
    for client, label, user_id in preflights:
        try:
            result = preflight(client, label, user_id, channel_id, run_id)
        except Exception as exc:  # noqa: BLE001 -- a check's error is its FAIL; report() redacts it
            result = errored(f"{CHECK_PREFLIGHT}-{label}", exc)
        report(result)
        results.append(result)
        if not result.passed:
            # Every check after a failed preflight would fail for the preflight's reason.
            return summarize(results)

    for check in args.checks:
        try:
            outcome = run_check(session, check, home_channel, args.home_since or started)
        except Exception as exc:  # noqa: BLE001 -- a check's error is its FAIL; report() redacts it
            outcome = [errored(check, exc)]
        for result in outcome:
            report(result)
            results.append(result)
        if not args.keep_going and not all(r.passed for r in outcome):
            break
    return summarize(results)


def errored(name: str, exc: Exception) -> CheckResult:
    """A check that raised: a Slack error, an unreachable host, anything else. It is that check's FAIL."""
    what = str(exc) if isinstance(exc, HarnessError) else f"unexpected {type(exc).__name__}: {exc}"
    return CheckResult(name, False, f"error: {what}", {"error": type(exc).__name__})


def run_check(session: Session, check: str, home_channel: str, home_since: float) -> list[CheckResult]:
    if check == CHECK_DM:
        return [check_dm(session)]
    if check == CHECK_MENTION:
        return [check_mention(session)]
    if check == CHECK_THREAD:
        return [check_thread(session)]
    if check == CHECK_UNLISTED:
        return check_unlisted(session)
    if check == CHECK_RESTART:
        return [check_dm(session, CHECK_RESTART)]
    return [check_home(session, home_channel, home_since)]


def summarize(results: list[CheckResult]) -> int:
    passed = [r.name for r in results if r.passed]
    failed = [r.name for r in results if not r.passed]
    say(f"SUMMARY pass={len(passed)} fail={len(failed)}"
        + (f" passed={','.join(passed)}" if passed else "")
        + (f" failed={','.join(failed)}" if failed else ""))
    return EXIT_FAIL if failed else EXIT_OK


def main(argv: Optional[list[str]] = None, transport=None, clock: Callable[[], float] = time.monotonic,
         sleep: Callable[[float], None] = time.sleep, wall: Callable[[], float] = time.time) -> int:
    args = parse_args(argv)
    try:
        return run(args, transport or UrllibTransport(), clock, sleep, wall)
    except HarnessError as exc:
        say(f"ERROR {exc}")
    except Exception as exc:  # noqa: BLE001 -- the traceback could carry a token; the redacted line is the report
        say(f"ERROR unexpected {type(exc).__name__}: {exc}")
    say("SUMMARY setup failed; no checks ran to completion")
    return EXIT_SETUP


if __name__ == "__main__":
    sys.exit(main())

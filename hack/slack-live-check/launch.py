#!/usr/bin/env python3
"""Workstation launcher for the Slack live check: one Kubernetes Job per run.

It renders manifests/job.yaml.template, ships harness.py in a ConfigMap, applies
both, waits for the Job, prints the harness's PASS/FAIL lines and deletes both.
The checks that need kubectl run here, under the caller's own credentials and
the --context it pins on every call, never in the pod:

- restart: runs --restart-cmd (or trusts --after-restart), waits for the gateway
  rollout, then runs the harness's restart check (a DM) in a second Job.
- legacy-socket: proves the broker holds no Slack Socket Mode connection on a next
  install, from the credential-proxy Deployment's env and the broker's logs.
- --expect-principal: matches the gateway's ingress log line for each listed turn.

Before the first Job it applies manifests/setup.yaml.template (the namespace, the
Workload Identity ServiceAccount and the egress fence), idempotently, and it deletes
the namespace when the run ends; --cleanup deletes it alone. The GSA and its grants
are not the launcher's: --render prints the one binding it relies on, and prints
every manifest without touching the cluster.
Everything after `--` goes to harness.py unchanged. Never run in CI; see README.md.
"""

import argparse
import json
import os
import shlex
import string
import subprocess
import sys
import time
import uuid
from typing import Callable, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import harness  # noqa: E402 -- the sibling module, found through the path line above

HARNESS_PATH = os.path.join(HERE, "harness.py")
HARNESS_FILE_NAME = "harness.py"
SETUP_TEMPLATE = os.path.join(HERE, "manifests", "setup.yaml.template")
JOB_TEMPLATE = os.path.join(HERE, "manifests", "job.yaml.template")

DEFAULT_NAMESPACE = "slack-test"
DEFAULT_SERVICE_ACCOUNT = "slack-test-runner"
GSA_EMAIL_FORMAT = "{name}@{project}.iam.gserviceaccount.com"
DEFAULT_GSA_NAME = "ka-slack-test-runner"
DEFAULT_AGENT_NAMESPACE = "kubeagents-system"
APP_LABEL_KEY = "app.kubernetes.io/name"
APP_LABEL_VALUE = "slack-live-check"
RUN_LABEL_KEY = "slack-live-check/run"
JOB_NAME_PREFIX = "slack-live-check-"
DEFAULT_JOB_TIMEOUT_SECONDS = 900
JOB_TTL_SECONDS = 600
JOB_POLL_INTERVAL_SECONDS = 5
JOB_WAIT_GRACE_SECONDS = 60
NAMESPACE_DELETE_TIMEOUT_SECONDS = 180
ROLLOUT_TIMEOUT_SECONDS = 600
SLACK_CONNECT_WAIT_SECONDS = 120
LOG_SINCE_MARGIN_SECONDS = 120

# The operator's names for what this reads (k8s-operator/internal/controller):
# credentialProxyName and credentialProxyContainerName (credential_proxy_manifests.go),
# a2aGatewayName and the gateway container (platformagent_a2a_manifests.go),
# shellSandboxName (shell_sandbox_manifests.go).
CREDENTIAL_PROXY_SUFFIX = "-credential-proxy"
CREDENTIAL_PROXY_CONTAINER = "envoy-credential-proxy"
GATEWAY_SUFFIX = "-a2a-gateway"
GATEWAY_CONTAINER = "gateway"
SHELL_SANDBOX_SUFFIX = "-shell"
SANDBOX_IMAGE_MARKER = "agent-sandbox"
# The pair that arms the broker's legacy Socket Mode relay: credential_proxy.py,
# serve(), starts SlackRelay only when both are non-empty, and the operator renders
# them on the broker only for the legacy consumer (legacySlackConsumer).
SLACK_PAIR_ENV = ("SLACK_BOT_TOKEN", "SLACK_APP_TOKEN")
# credential_proxy.py, initialize_slack_relay: one of these is logged once the relay
# has been armed, whether or not its connection came up.
LEGACY_RELAY_LOG_MARKERS = ("Slack relay enabled", "Slack relay initialization failed")
# a2a/gateway/slack.go, Run: logged once auth.test succeeds, before Socket Mode connects.
GATEWAY_SLACK_CONNECTED_MSG = "slack connected"
# a2a/gateway/gateway.go, startTask: the plaintext join of chat message id and principal.
GATEWAY_INGRESS_MSG = "ingress"
PRINCIPAL_LISTED_PLACEHOLDER = "{listed}"
PRINCIPAL_CHECKS = (harness.CHECK_DM, harness.CHECK_MENTION, harness.CHECK_THREAD, harness.CHECK_RESTART)

CHECK_LEGACY_SOCKET = "legacy-socket"
LAUNCHER_ONLY_CHECKS = (CHECK_LEGACY_SOCKET,)
ALL_CHECKS = harness.CHECK_ORDER + LAUNCHER_ONLY_CHECKS
PROJECT_FLAG = "--project"
CHECKS_FLAG = "--checks"
RUN_ID_FLAG = "--run-id"
KEEP_GOING_FLAG = "--keep-going"
# Harness flags the launcher sets itself; one after `--` would silently win.
LAUNCHER_OWNED_HARNESS_FLAGS = (CHECKS_FLAG, RUN_ID_FLAG, PROJECT_FLAG, KEEP_GOING_FLAG)
RESULT_PREFIXES = ("PASS ", "FAIL ")
EVIDENCE_PREFIX = "EVIDENCE "

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_SETUP = 2


class LaunchError(Exception):
    """The launcher cannot go on: a kubectl call failed, or the install is not what it expects."""


def say(line: str) -> None:
    harness.say(line)


class Kubectl:
    """kubectl with --context pinned on every call."""

    def __init__(self, context: str, runner: Callable = subprocess.run) -> None:
        self.context = context
        self.runner = runner

    def run(self, args: list[str], stdin: Optional[str] = None) -> str:
        cmd = ["kubectl", "--context", self.context, *args]
        proc = self.runner(cmd, input=stdin, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise LaunchError(f"{' '.join(cmd[:6])}... exited {proc.returncode}: {proc.stderr.strip()[-500:]}")
        return proc.stdout

    def get_json(self, args: list[str]) -> dict:
        return json.loads(self.run([*args, "-o", "json"]))


def render(template_path: str, values: dict) -> str:
    with open(template_path, encoding="utf-8") as handle:
        return string.Template(handle.read()).substitute(values)


def selector_of(deployment: dict) -> str:
    labels = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
    if not labels:
        raise LaunchError(f"{deployment.get('metadata', {}).get('name')} has no matchLabels selector")
    return ",".join(f"{k}={v}" for k, v in sorted(labels.items()))


def container_of(deployment: dict, name: str) -> dict:
    for container in deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
        if container.get("name") == name:
            return container
    raise LaunchError(f"{deployment.get('metadata', {}).get('name')} has no container {name}")


def env_names(container: dict) -> set[str]:
    return {env.get("name", "") for env in container.get("env", []) or []}


def pod_logs(kubectl: Kubectl, namespace: str, selector: str, container: str, since_seconds: int = 0) -> str:
    # --tail=-1: with a selector kubectl otherwise keeps only the last 10 lines per pod.
    args = ["logs", "-n", namespace, "-l", selector, "-c", container, "--tail=-1"]
    if since_seconds:
        args.append(f"--since={since_seconds}s")
    return kubectl.run(args)


def discover_agent(kubectl: Kubectl, namespace: str) -> str:
    items = kubectl.get_json(["get", "platformagents", "-n", namespace]).get("items", [])
    if len(items) != 1:
        raise LaunchError(f"expected one PlatformAgent in {namespace}, found {len(items)}; pass --agent-name")
    return items[0]["metadata"]["name"]


def discover_image(kubectl: Kubectl, namespace: str, agent: str) -> str:
    """The agent-sandbox image this install already pulls: it has python3 and no credentials."""
    sts = kubectl.get_json(["get", "statefulset", agent + SHELL_SANDBOX_SUFFIX, "-n", namespace])
    containers = sts.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    for container in containers:
        if SANDBOX_IMAGE_MARKER in container.get("image", ""):
            return container["image"]
    if containers:
        return containers[0]["image"]
    raise LaunchError(f"statefulset {agent}{SHELL_SANDBOX_SUFFIX} has no containers; pass --image")


def wi_binding_command(project: str, gsa_email: str, namespace: str, service_account: str) -> str:
    """The Workload Identity binding the run relies on: the cluster manager's, printed, never run."""
    return (
        f"gcloud iam service-accounts add-iam-policy-binding {gsa_email} --project={project} "
        f"--role=roles/iam.workloadIdentityUser "
        f"--member='serviceAccount:{project}.svc.id.goog[{namespace}/{service_account}]'"
    )


def job_manifests(namespace: str, service_account: str, image: str, run_id: str, harness_args: list[str],
                  deadline_seconds: int) -> tuple[str, str, str]:
    """Returns (job name, ConfigMap JSON, Job YAML)."""
    job_name = JOB_NAME_PREFIX + run_id
    with open(HARNESS_PATH, encoding="utf-8") as handle:
        source = handle.read()
    labels = {APP_LABEL_KEY: APP_LABEL_VALUE, RUN_LABEL_KEY: run_id}
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": job_name, "namespace": namespace, "labels": labels},
        "data": {HARNESS_FILE_NAME: source},
    }
    job = render(JOB_TEMPLATE, {
        "JOB_NAME": job_name,
        "NAMESPACE": namespace,
        "RUN_ID": run_id,
        "SERVICE_ACCOUNT": service_account,
        "IMAGE": image,
        "CONFIGMAP_NAME": job_name,
        "ARGS_JSON": json.dumps(harness_args),
        "DEADLINE_SECONDS": str(deadline_seconds),
        "TTL_SECONDS": str(JOB_TTL_SECONDS),
    })
    return job_name, json.dumps(configmap), job


def parse_harness_output(text: str) -> tuple[list[tuple[str, bool, str]], list[dict]]:
    """The harness's PASS/FAIL lines as (name, passed, line), and its EVIDENCE objects."""
    results = []
    evidence = []
    for line in text.splitlines():
        if line.startswith(RESULT_PREFIXES):
            verdict, _, rest = line.partition(" ")
            name = rest.split(":", 1)[0]
            results.append((name, verdict == "PASS", line))
        elif line.startswith(EVIDENCE_PREFIX):
            try:
                evidence.append(json.loads(line[len(EVIDENCE_PREFIX):]))
            except ValueError:
                continue
    return results, evidence


class Launcher:
    def __init__(self, args: argparse.Namespace, forwarded: list[str], kubectl: Kubectl,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                 runner: Callable = subprocess.run) -> None:
        self.args = args
        self.forwarded = forwarded
        self.kubectl = kubectl
        self.clock = clock
        self.sleep = sleep
        self.runner = runner
        self.results: list[tuple[str, bool, str]] = []
        self._agent = args.agent_name
        self.namespace_applied = False

    @property
    def agent(self) -> str:
        if not self._agent:
            self._agent = discover_agent(self.kubectl, self.args.agent_namespace)
        return self._agent

    def record(self, name: str, passed: bool, detail: str) -> bool:
        line = f"{'PASS' if passed else 'FAIL'} {name}: {detail}"
        say(line)
        self.results.append((name, passed, line))
        return passed

    def deployment(self, suffix: str) -> dict:
        return self.kubectl.get_json(["get", "deployment", self.agent + suffix, "-n", self.args.agent_namespace])

    # --- legacy-socket -----------------------------------------------------

    def legacy_socket(self) -> bool:
        """The broker's legacy relay holds no Socket Mode connection; the gateway holds the one there is.

        Reads, in order: the credential-proxy Deployment's broker container env
        (the operator renders the Slack pair there only for the legacy consumer),
        the broker's logs since its containers started (the relay logs a line when
        armed), and the gateway Deployment's env and logs as the positive control.
        """
        ns = self.args.agent_namespace
        broker = self.deployment(CREDENTIAL_PROXY_SUFFIX)
        broker_container = container_of(broker, CREDENTIAL_PROXY_CONTAINER)
        armed = sorted(env_names(broker_container) & set(SLACK_PAIR_ENV))
        if armed:
            return self.record(CHECK_LEGACY_SOCKET, False,
                               f"{self.agent}{CREDENTIAL_PROXY_SUFFIX}/{CREDENTIAL_PROXY_CONTAINER} carries {','.join(armed)}: "
                               "the legacy Slack relay is armed")
        if broker_container.get("envFrom"):
            return self.record(CHECK_LEGACY_SOCKET, False,
                               f"{CREDENTIAL_PROXY_CONTAINER} has envFrom, which could supply the Slack pair; inspect it by hand")
        logs = pod_logs(self.kubectl, ns, selector_of(broker), CREDENTIAL_PROXY_CONTAINER)
        hits = [line for line in logs.splitlines() if any(m in line for m in LEGACY_RELAY_LOG_MARKERS)]
        if hits:
            return self.record(CHECK_LEGACY_SOCKET, False, "the broker logged its Slack relay: " + harness.truncate(hits[-1]))
        gateway = self.deployment(GATEWAY_SUFFIX)
        missing = sorted(set(SLACK_PAIR_ENV) - env_names(container_of(gateway, GATEWAY_CONTAINER)))
        if missing:
            return self.record(CHECK_LEGACY_SOCKET, False,
                               f"the gateway lacks {','.join(missing)} too, so this is not a next install with Slack on the gateway")
        gw_logs = pod_logs(self.kubectl, ns, selector_of(gateway), GATEWAY_CONTAINER)
        connected = any(_json_msg(line) == GATEWAY_SLACK_CONNECTED_MSG for line in gw_logs.splitlines())
        if not connected:
            return self.record(CHECK_LEGACY_SOCKET, False,
                               "the broker is clean but the gateway has not logged 'slack connected' either")
        return self.record(CHECK_LEGACY_SOCKET, True,
                           f"broker {CREDENTIAL_PROXY_CONTAINER} has no Slack pair in env and no relay log line "
                           f"({len(logs.splitlines())} lines read); the gateway holds the pair and logged 'slack connected'")

    # --- the Job ------------------------------------------------------------

    def harness_args(self, checks: list[str]) -> tuple[str, list[str]]:
        run_id = time.strftime("%Y%m%d%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:4]
        args = [CHECKS_FLAG, ",".join(checks), RUN_ID_FLAG, run_id, PROJECT_FLAG, self.args.project]
        if self.args.keep_going:
            args.append(KEEP_GOING_FLAG)
        return run_id, args + self.forwarded

    def ensure_namespace(self) -> None:
        """Applies the namespace, ServiceAccount and NetworkPolicy; idempotent."""
        if not self.namespace_applied:
            self.kubectl.run(["apply", "-f", "-"], stdin=setup_manifest(self.args))
            self.namespace_applied = True
            say(f"SETUP namespace {self.args.namespace}, ServiceAccount {self.args.service_account} -> {self.args.gsa}")

    def run_job(self, checks: list[str]) -> list[dict]:
        self.ensure_namespace()
        run_id, hargs = self.harness_args(checks)
        image = self.args.image or discover_image(self.kubectl, self.args.agent_namespace, self.agent)
        job_name, configmap, job = job_manifests(self.args.namespace, self.args.service_account, image, run_id, hargs,
                                                 self.args.job_timeout)
        ns = self.args.namespace
        say(f"JOB {job_name} in {ns}: checks={','.join(checks)} image={image}")
        started = self.clock()
        try:
            self.kubectl.run(["apply", "-f", "-"], stdin=configmap)
            self.kubectl.run(["apply", "-f", "-"], stdin=job)
            state = self.wait_job(job_name)
            logs = ""
            try:
                logs = self.kubectl.run(["logs", "-n", ns, f"job/{job_name}", "-c", "harness", "--tail=-1"])
            except LaunchError as exc:
                say(f"JOB {job_name}: no pod log ({exc})")
            for line in logs.splitlines():
                say(line)
            results, evidence = parse_harness_output(harness.REDACTOR.redact(logs))
            self.results.extend(results)
            if state != "Complete" and not any(not passed for _, passed, _ in results):
                self.record("job", False, f"{job_name} ended {state} with no FAIL line; see the log above")
            for ev in evidence:
                ev["elapsed_seconds"] = int(self.clock() - started)
            return evidence
        finally:
            for kind in ("job", "configmap"):
                try:
                    self.kubectl.run(["delete", kind, job_name, "-n", ns, "--ignore-not-found", "--wait=false"])
                except LaunchError as exc:
                    say(f"CLEANUP {kind}/{job_name} not deleted: {exc}")

    def wait_job(self, job_name: str) -> str:
        deadline = self.clock() + self.args.job_timeout + JOB_WAIT_GRACE_SECONDS
        while True:
            job = self.kubectl.get_json(["get", "job", job_name, "-n", self.args.namespace])
            for cond in job.get("status", {}).get("conditions", []) or []:
                if cond.get("type") in ("Complete", "Failed") and cond.get("status") == "True":
                    return cond["type"]
            if self.clock() >= deadline:
                pods = self.kubectl.run(["get", "pods", "-n", self.args.namespace, "-l", f"job-name={job_name}", "-o", "wide"])
                say(f"JOB {job_name} did not finish in time; pods:\n{pods}")
                return "Timeout"
            self.sleep(JOB_POLL_INTERVAL_SECONDS)

    # --- reuse hooks --------------------------------------------------------

    def check_principals(self, evidence: list[dict]) -> bool:
        """--expect-principal: the gateway's ingress line for each listed turn names the expected principal."""
        turns = [ev for ev in evidence if ev.get("check") in PRINCIPAL_CHECKS and ev.get("passed") and ev.get("sent_ts")]
        if not turns:
            return self.record("principal", False, "no passing listed turn to look up")
        since = max(ev.get("elapsed_seconds", 0) for ev in turns) + LOG_SINCE_MARGIN_SECONDS
        gateway = self.deployment(GATEWAY_SUFFIX)
        logs = pod_logs(self.kubectl, self.args.agent_namespace, selector_of(gateway), GATEWAY_CONTAINER, since)
        principals = {}
        for line in logs.splitlines():
            record = _json_record(line)
            if record.get("msg") == GATEWAY_INGRESS_MSG:
                principals[record.get("backendMessageId", "")] = record.get("principal", "")
        ok = True
        for ev in turns:
            expected = self.args.expect_principal.replace(PRINCIPAL_LISTED_PLACEHOLDER, ev.get("author", ""))
            name = f"principal-{ev['check']}"
            found = principals.get(ev["sent_ts"])
            if found is None:
                ok = self.record(name, False, f"no ingress log line for backendMessageId={ev['sent_ts']}") and ok
            elif found != expected:
                ok = self.record(name, False, f"ingress for {ev['sent_ts']} names principal={found}, expected {expected}") and ok
            else:
                ok = self.record(name, True, f"ingress for {ev['sent_ts']} names principal={found}") and ok
        return ok

    def restart(self) -> bool:
        ns = self.args.agent_namespace
        started = self.clock()
        if not self.args.after_restart:
            cmd = shlex.split(self.args.restart_cmd)
            say(f"RESTART running: {' '.join(cmd)}")
            proc = self.runner(cmd, capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                return self.record("restart-cmd", False, f"exited {proc.returncode}: {proc.stderr.strip()[-500:]}")
        self.kubectl.run(["rollout", "status", f"deployment/{self.agent}{GATEWAY_SUFFIX}", "-n", ns,
                          f"--timeout={ROLLOUT_TIMEOUT_SECONDS}s"])
        gateway = self.deployment(GATEWAY_SUFFIX)

        def connected():
            since = int(self.clock() - started) + LOG_SINCE_MARGIN_SECONDS
            logs = pod_logs(self.kubectl, ns, selector_of(gateway), GATEWAY_CONTAINER, since)
            return True if any(_json_msg(line) == GATEWAY_SLACK_CONNECTED_MSG for line in logs.splitlines()) else None

        if harness.poll(connected, SLACK_CONNECT_WAIT_SECONDS, JOB_POLL_INTERVAL_SECONDS, self.clock, self.sleep) is None:
            return self.record("restart-cmd", False, f"the gateway did not log 'slack connected' within {SLACK_CONNECT_WAIT_SECONDS}s")
        say("RESTART gateway rolled out and logged 'slack connected'")
        return True

    # --- the run ------------------------------------------------------------

    def run(self, checks: list[str]) -> int:
        try:
            self.run_checks(checks)
        finally:
            if self.namespace_applied:
                delete_namespace(self.kubectl, self.args.namespace)
        return self.summarize()

    def run_checks(self, checks: list[str]) -> None:
        keep_going = self.args.keep_going

        def failed() -> bool:
            return any(not passed for _, passed, _ in self.results)

        if CHECK_LEGACY_SOCKET in checks:
            self.legacy_socket()
        pod_checks = [c for c in checks if c in harness.CHECK_ORDER and c != harness.CHECK_RESTART]
        if pod_checks and (keep_going or not failed()):
            evidence = self.run_job(pod_checks)
            if self.args.expect_principal and (keep_going or not failed()):
                self.check_principals(evidence)
        if harness.CHECK_RESTART in checks and (keep_going or not failed()):
            if self.restart():
                evidence = self.run_job([harness.CHECK_RESTART])
                if self.args.expect_principal and (keep_going or not failed()):
                    self.check_principals(evidence)

    def summarize(self) -> int:
        passed = [name for name, ok, _ in self.results if ok]
        failed = [name for name, ok, _ in self.results if not ok]
        say(f"OVERALL pass={len(passed)} fail={len(failed)}" + (f" failed={','.join(failed)}" if failed else ""))
        return EXIT_FAIL if failed or not self.results else EXIT_OK


def delete_namespace(kubectl: Kubectl, namespace: str) -> None:
    """Deletes the run's namespace, and with it the ServiceAccount, the fence and any Job left behind."""
    try:
        kubectl.run(["delete", "namespace", namespace, "--ignore-not-found", "--wait=true",
                     f"--timeout={NAMESPACE_DELETE_TIMEOUT_SECONDS}s"])
        say(f"CLEANUP namespace {namespace} deleted")
    except LaunchError as exc:
        say(f"CLEANUP namespace {namespace} not deleted: {exc}")


def _json_record(line: str) -> dict:
    start = line.find("{")
    if start < 0:
        return {}
    try:
        record = json.loads(line[start:])
    except ValueError:
        return {}
    return record if isinstance(record, dict) else {}


def _json_msg(line: str) -> str:
    return str(_json_record(line).get("msg", ""))


def parse_checks(value: str) -> list[str]:
    requested = []
    for item in (part.strip() for part in value.split(",")):
        if not item:
            continue
        if item == harness.CHECKS_ALL_ALIAS:
            requested.extend(harness.CHECKS_ALL)
        elif item in ALL_CHECKS:
            requested.append(item)
        else:
            raise argparse.ArgumentTypeError(f"unknown check {item!r}; choose from {', '.join(ALL_CHECKS)} or {harness.CHECKS_ALL_ALIAS}")
    if not requested:
        raise argparse.ArgumentTypeError("no checks selected")
    return [c for c in ALL_CHECKS if c in requested]


def parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    forwarded: list[str] = []
    if "--" in argv:
        split = argv.index("--")
        argv, forwarded = argv[:split], argv[split + 1:]
    parser = argparse.ArgumentParser(description="Run the Slack live check as a Job (hack/slack-live-check/README.md).",
                                     epilog="Arguments after -- go to harness.py.")
    parser.add_argument("--context", default="", help="kubectl context, pinned on every call (required unless --render)")
    parser.add_argument(CHECKS_FLAG, type=parse_checks, default=list(harness.CHECKS_ALL),
                        help=f"comma list from {', '.join(ALL_CHECKS)}; '{harness.CHECKS_ALL_ALIAS}' is {','.join(harness.CHECKS_ALL)}")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE, help="where the Job runs")
    parser.add_argument("--service-account", default=DEFAULT_SERVICE_ACCOUNT)
    parser.add_argument(PROJECT_FLAG, default=harness.DEFAULT_PROJECT, help="the project holding the token secrets")
    parser.add_argument("--gsa", default="", help=f"the GSA the ServiceAccount is bound to (default {DEFAULT_GSA_NAME}@<project>)")
    parser.add_argument("--agent-namespace", default=DEFAULT_AGENT_NAMESPACE)
    parser.add_argument("--agent-name", default="", help="the PlatformAgent; discovered when there is one")
    parser.add_argument("--image", default="", help="the Job's image; default is the install's agent-sandbox image")
    parser.add_argument("--job-timeout", type=int, default=DEFAULT_JOB_TIMEOUT_SECONDS, help="the Job's activeDeadlineSeconds")
    parser.add_argument(KEEP_GOING_FLAG, action="store_true")
    restart = parser.add_mutually_exclusive_group()
    restart.add_argument("--restart-cmd", default="", help="command (no shell) the restart check runs first; pin its --context yourself")
    restart.add_argument("--after-restart", action="store_true", help="the restart already happened; just wait for the rollout and DM")
    parser.add_argument("--expect-principal", default="",
                        help=f"expected ingress principal per listed turn; {PRINCIPAL_LISTED_PLACEHOLDER} is the listed member id")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--cleanup", action="store_true", help="delete the run namespace (a run does this itself on exit) and stop")
    mode.add_argument("--render", action="store_true", help="print the manifests and exit; touches nothing")
    args = parser.parse_args(argv)
    owned = [arg for arg in forwarded if arg.split("=", 1)[0] in LAUNCHER_OWNED_HARNESS_FLAGS]
    if owned:
        parser.error(f"{', '.join(owned)} after -- would override the launcher's own; pass the launcher flag instead")
    if not args.context and not args.render:
        parser.error("--context is required")
    if harness.CHECK_RESTART in args.checks and not (args.restart_cmd or args.after_restart) and not (args.cleanup or args.render):
        parser.error("the restart check needs --restart-cmd or --after-restart")
    args.gsa = args.gsa or GSA_EMAIL_FORMAT.format(name=DEFAULT_GSA_NAME, project=args.project)
    return args, forwarded


def setup_manifest(args: argparse.Namespace) -> str:
    return render(SETUP_TEMPLATE, {"NAMESPACE": args.namespace, "SERVICE_ACCOUNT": args.service_account, "GSA_EMAIL": args.gsa})


def main(argv: Optional[list[str]] = None, runner: Callable = subprocess.run,
         clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep) -> int:
    args, forwarded = parse_args(sys.argv[1:] if argv is None else argv)
    binding = wi_binding_command(args.project, args.gsa, args.namespace, args.service_account)
    try:
        if args.render:
            print(setup_manifest(args))
            _, configmap, job = job_manifests(args.namespace, args.service_account, args.image or "<agent-sandbox image>",
                                              "render", [CHECKS_FLAG, ",".join(c for c in args.checks if c in harness.CHECK_ORDER)] + forwarded,
                                              args.job_timeout)
            print("---")
            print(job)
            say("# the ConfigMap carries harness.py verbatim. The Workload Identity binding the run relies on:")
            say("# " + binding)
            return EXIT_OK
        kubectl = Kubectl(args.context, runner)
        if args.cleanup:
            delete_namespace(kubectl, args.namespace)
            return EXIT_OK
        return Launcher(args, forwarded, kubectl, clock, sleep, runner).run(args.checks)
    except (LaunchError, harness.HarnessError) as exc:
        say(f"ERROR {exc}")
        return EXIT_SETUP


if __name__ == "__main__":
    sys.exit(main())

"""Offline tests for the Slack live-check launcher (hack/slack-live-check/launch.py).

kubectl is a recorded fake behind the launcher's runner seam: every call is checked
for the pinned --context, and the fake answers from a small model of a next install
(the PlatformAgent, the broker and gateway Deployments and their logs, the Job).
"""

import contextlib
import importlib.util
import io
import json
import pathlib
import subprocess
import sys
import unittest

import yaml

REPO = pathlib.Path(__file__).resolve().parent.parent
LAUNCH_DIR = REPO / "hack" / "slack-live-check"
sys.path.insert(0, str(LAUNCH_DIR))
spec = importlib.util.spec_from_file_location("slack_live_check_launch", LAUNCH_DIR / "launch.py")
launch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launch)
harness = launch.harness

CONTEXT = "gke_test_ctx"
AGENT = "platform-agent"
AGENT_NS = "kubeagents-system"
SANDBOX_IMAGE = "us-docker.pkg.dev/p/kube-agents/agent-sandbox:v1"
LEAKED = "xoxp-9999-leaked-token-value"
# Far above any wait the launcher sets: a loop that passes it is unbounded.
SLEEP_BUDGET_SECONDS = 100_000


def pod_spec(job):
    return job["spec"]["template"]["spec"]


def deployment(name, container, env_names, labels, env_from=None):
    c = {"name": container, "env": [{"name": n, "valueFrom": {"secretKeyRef": {"name": "s", "key": n}}} for n in env_names]}
    if env_from:
        c["envFrom"] = env_from
    return {"metadata": {"name": name}, "spec": {"selector": {"matchLabels": labels},
                                                 "template": {"spec": {"containers": [c]}}}}


class FakeCluster:
    def __init__(self):
        self.calls = []
        self.applied = []
        self.deleted = []
        self.broker_env = ["CREDENTIAL_PROXY_POLICY"]
        self.broker_env_from = None
        self.broker_logs = "INFO credential proxy listening on unix socket /run/x\n"
        self.gateway_env = ["SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "A2A_SLACK_ALLOWED_USERS"]
        self.gateway_logs = '{"time":"t","level":"INFO","msg":"slack connected","user":"kage","botUserID":"U0BOT"}\n'
        self.job_condition = "Complete"
        self.job_log = "PASS preflight-listed: ok\nPASS dm: answer\nEVIDENCE {\"check\": \"dm\", \"passed\": true, \"sent_ts\": \"1.000101\", \"author\": \"U0LISTED1\"}\nSUMMARY pass=2 fail=0\n"
        self.restart_runs = []
        self.fail_on = None

    def __call__(self, cmd, input=None, capture_output=True, text=True, check=False):
        if cmd[0] != "kubectl" or "restart" in cmd:
            # The operator's --restart-cmd, not the launcher's own kubectl.
            self.restart_runs.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, "", "")
        self.calls.append(cmd)
        assert cmd[1:3] == ["--context", CONTEXT], cmd
        args = cmd[3:]
        out = self.answer(args, input)
        if self.fail_on and self.fail_on in " ".join(args):
            return subprocess.CompletedProcess(cmd, 1, "", "boom")
        return subprocess.CompletedProcess(cmd, 0, out, "")

    def answer(self, args, stdin):
        joined = " ".join(args)
        if args[0] == "apply":
            self.applied.append(stdin)
            return "applied"
        if args[0] == "delete":
            self.deleted.append(args[1:3])
            return ""
        if args[:2] == ["get", "platformagents"]:
            return json.dumps({"items": [{"metadata": {"name": AGENT}}]})
        if args[:2] == ["get", "statefulset"]:
            return json.dumps({"spec": {"template": {"spec": {"containers": [{"name": "sandbox", "image": SANDBOX_IMAGE}]}}}})
        if args[:3] == ["get", "deployment", AGENT + "-credential-proxy"]:
            return json.dumps(deployment(args[2], "envoy-credential-proxy", self.broker_env,
                                         {"app": AGENT + "-credential-proxy"}, self.broker_env_from))
        if args[:3] == ["get", "deployment", AGENT + "-a2a-gateway"]:
            return json.dumps(deployment(args[2], "gateway", self.gateway_env, {"app": AGENT + "-a2a-gateway"}))
        if args[:2] == ["get", "job"]:
            return json.dumps({"status": {"conditions": [{"type": self.job_condition, "status": "True"}]}})
        if args[0] == "logs" and "job/" in joined:
            return self.job_log
        if args[0] == "logs" and "credential-proxy" in joined:
            assert "--tail=-1" in args
            return self.broker_logs
        if args[0] == "logs" and "a2a-gateway" in joined:
            assert "--tail=-1" in args
            return self.gateway_logs
        if args[0] == "rollout":
            return "rolled out"
        if args[:2] == ["get", "pods"]:
            return "pod Pending"
        raise AssertionError(f"unexpected kubectl {args}")


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def clock(self):
        return self.now

    def sleep(self, seconds):
        if seconds <= 0 or self.now > SLEEP_BUDGET_SECONDS:
            raise AssertionError(f"a wait slept {seconds}s at t={self.now}; it is not bounded")
        self.now += seconds


def run_launch(cluster, *argv):
    out = io.StringIO()
    fake = FakeClock()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        code = launch.main(list(argv), runner=cluster, clock=fake.clock, sleep=fake.sleep)
    return code, out.getvalue()


class LegacySocketTest(unittest.TestCase):
    def test_passes_on_a_clean_broker_and_a_connected_gateway(self):
        cluster = FakeCluster()
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket")
        self.assertEqual(code, launch.EXIT_OK, out)
        self.assertIn("PASS legacy-socket: broker envoy-credential-proxy has no Slack pair", out)
        self.assertEqual(cluster.applied, [], "legacy-socket alone must not create anything")

    def test_fails_when_the_broker_carries_the_pair(self):
        cluster = FakeCluster()
        cluster.broker_env.append("SLACK_APP_TOKEN")
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket,dm")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("carries SLACK_APP_TOKEN", out)
        self.assertEqual(cluster.applied, [], "the first FAIL stops the run")

    def test_fails_on_broker_envfrom(self):
        cluster = FakeCluster()
        cluster.broker_env_from = [{"secretRef": {"name": "x"}}]
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("envFrom", out)

    def test_fails_when_the_broker_logged_its_relay(self):
        for line in ("INFO Slack relay enabled workspaces=1", "ERROR Slack relay initialization failed; retrying type=X"):
            cluster = FakeCluster()
            cluster.broker_logs += line + "\n"
            code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket")
            self.assertEqual(code, launch.EXIT_FAIL, line)
            self.assertIn("the broker logged its Slack relay", out)

    def test_fails_when_the_gateway_has_no_pair(self):
        cluster = FakeCluster()
        cluster.gateway_env = []
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("not a next install", out)

    def test_fails_when_the_gateway_never_connected(self):
        cluster = FakeCluster()
        cluster.gateway_logs = '{"msg":"slack auth.test: invalid_auth"}\n'
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("has not logged 'slack connected'", out)


class JobTest(unittest.TestCase):
    def test_job_runs_prints_results_and_cleans_up(self):
        cluster = FakeCluster()
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm", "--", "--wait-answer")
        self.assertEqual(code, launch.EXIT_OK, out)
        self.assertIn("PASS dm: answer", out)
        setup = list(yaml.safe_load_all(cluster.applied[0]))
        configmap = json.loads(cluster.applied[1])
        job = yaml.safe_load(cluster.applied[2])
        self.assertEqual([d["kind"] for d in setup], ["Namespace", "ServiceAccount", "NetworkPolicy"])
        self.assertEqual(setup[1]["metadata"]["name"], "slack-test-runner")
        self.assertEqual(setup[1]["metadata"]["annotations"]["iam.gke.io/gcp-service-account"],
                         "ka-slack-test-runner@bnaylor-kagents-dev.iam.gserviceaccount.com")
        self.assertEqual(job["metadata"]["namespace"], "slack-test")
        self.assertEqual(pod_spec(job)["serviceAccountName"], "slack-test-runner")
        self.assertEqual(configmap["kind"], "ConfigMap")
        self.assertEqual(configmap["data"]["harness.py"], (LAUNCH_DIR / "harness.py").read_text())
        pod = job["spec"]["template"]["spec"]
        container = pod["containers"][0]
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        self.assertEqual(container["image"], SANDBOX_IMAGE)
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertTrue(pod["securityContext"]["runAsNonRoot"])
        self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
        self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])
        self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        # No credential reaches the pod through its spec.
        self.assertEqual([e["name"] for e in container["env"]], ["HOME"])
        self.assertNotIn("envFrom", container)
        args = container["args"]
        self.assertEqual(args[args.index("--checks") + 1], "dm")
        self.assertIn("--wait-answer", args)
        self.assertEqual(sorted(kind for kind, _ in cluster.deleted), ["configmap", "job", "namespace"])
        self.assertEqual(cluster.deleted[-1], ["namespace", "slack-test"], "the namespace goes last")

    def test_job_failure_without_fail_lines_is_reported_and_cleaned_up(self):
        cluster = FakeCluster()
        cluster.job_condition = "Failed"
        cluster.job_log = "ERROR the metadata server refused a token (HTTP 403)\nSUMMARY setup failed\n"
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("FAIL job:", out)
        self.assertEqual(len(cluster.deleted), 3)

    def test_cleanup_runs_when_the_wait_raises(self):
        cluster = FakeCluster()
        cluster.fail_on = "get job"
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm")
        self.assertEqual(code, launch.EXIT_SETUP)
        self.assertEqual(sorted(kind for kind, _ in cluster.deleted), ["configmap", "job", "namespace"])

    def test_pod_log_tokens_are_scrubbed(self):
        cluster = FakeCluster()
        cluster.job_log = f"FAIL dm: odd {LEAKED}\nSUMMARY pass=0 fail=1\n"
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertNotIn(LEAKED, out)

    def test_harness_checks_after_a_launcher_fail_are_skipped_unless_keep_going(self):
        cluster = FakeCluster()
        cluster.broker_env.append("SLACK_BOT_TOKEN")
        code, _ = run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket,dm", "--keep-going")
        self.assertEqual(code, launch.EXIT_FAIL)
        job = yaml.safe_load(cluster.applied[2])
        self.assertIn("--keep-going", job["spec"]["template"]["spec"]["containers"][0]["args"])


class PrincipalTest(unittest.TestCase):
    def ingress(self, ts, principal):
        return json.dumps({"level": "INFO", "msg": "ingress", "backendMessageId": ts, "principal": principal}) + "\n"

    def test_principal_matches_with_listed_placeholder(self):
        cluster = FakeCluster()
        cluster.gateway_logs += self.ingress("1.000101", "slack:U0LISTED1")
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm", "--expect-principal", "slack:{listed}")
        self.assertEqual(code, launch.EXIT_OK, out)
        self.assertIn("PASS principal-dm: ingress for 1.000101 names principal=slack:U0LISTED1", out)

    def test_principal_mismatch_and_missing_fail(self):
        cluster = FakeCluster()
        cluster.gateway_logs += self.ingress("1.000101", "alice@example.com")
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm", "--expect-principal", "slack:{listed}")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("names principal=alice@example.com, expected slack:U0LISTED1", out)
        cluster = FakeCluster()
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm", "--expect-principal", "slack:{listed}")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("no ingress log line for backendMessageId=1.000101", out)


class RestartTest(unittest.TestCase):
    def test_restart_runs_the_command_without_a_shell_then_a_second_job(self):
        cluster = FakeCluster()
        cluster.job_log = "PASS restart: answer\nSUMMARY pass=1 fail=0\n"
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "restart",
                               "--restart-cmd", f"kubectl --context {CONTEXT} -n {AGENT_NS} rollout restart deploy/x")
        self.assertEqual(code, launch.EXIT_OK, out)
        self.assertEqual(cluster.restart_runs, [["kubectl", "--context", CONTEXT, "-n", AGENT_NS, "rollout", "restart", "deploy/x"]])
        self.assertTrue(any(c[3] == "rollout" and c[4] == "status" for c in cluster.calls))
        job = yaml.safe_load(cluster.applied[2])
        args = job["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertEqual(args[args.index("--checks") + 1], "restart")

    def test_after_restart_skips_the_command(self):
        cluster = FakeCluster()
        cluster.job_log = "PASS restart: answer\n"
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm,restart", "--after-restart")
        self.assertEqual(code, launch.EXIT_OK, out)
        self.assertEqual(cluster.restart_runs, [])
        self.assertEqual(len(cluster.applied), 5, "the namespace once, then a ConfigMap and Job before the restart and after")
        self.assertEqual([d for d in cluster.deleted if d[0] == "namespace"], [["namespace", "slack-test"]])

    def test_restart_fails_when_the_gateway_never_reconnects(self):
        cluster = FakeCluster()
        cluster.gateway_logs = ""
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "restart", "--after-restart")
        self.assertEqual(code, launch.EXIT_FAIL)
        self.assertIn("did not log 'slack connected'", out)
        self.assertEqual(cluster.applied, [])


class ModesAndArgsTest(unittest.TestCase):
    def test_cleanup_deletes_the_namespace_alone(self):
        cluster = FakeCluster()
        code, out = run_launch(cluster, "--context", CONTEXT, "--cleanup")
        self.assertEqual(code, launch.EXIT_OK, out)
        self.assertEqual(cluster.deleted, [["namespace", "slack-test"]])
        self.assertEqual(cluster.applied, [])

    def test_namespace_and_names_can_be_overridden(self):
        cluster = FakeCluster()
        code, out = run_launch(cluster, "--context", CONTEXT, "--checks", "dm", "--namespace", "other-ns",
                               "--service-account", "other-ksa", "--gsa", "x@p.iam.gserviceaccount.com")
        self.assertEqual(code, launch.EXIT_OK, out)
        setup = list(yaml.safe_load_all(cluster.applied[0]))
        self.assertEqual(setup[0]["metadata"]["name"], "other-ns")
        self.assertEqual(setup[1]["metadata"]["annotations"]["iam.gke.io/gcp-service-account"], "x@p.iam.gserviceaccount.com")
        self.assertEqual(cluster.deleted[-1], ["namespace", "other-ns"])

    def test_network_policy_fences_ingress_and_private_egress(self):
        cluster = FakeCluster()
        run_launch(cluster, "--context", CONTEXT, "--checks", "dm")
        policy = list(yaml.safe_load_all(cluster.applied[0]))[2]["spec"]
        self.assertEqual(policy["policyTypes"], ["Ingress", "Egress"])
        self.assertEqual(policy["ingress"], [])
        https = [rule for rule in policy["egress"] if rule["ports"][0]["port"] == 443][0]
        self.assertEqual(https["to"][0]["ipBlock"]["except"], ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"])

    def test_legacy_socket_alone_creates_no_namespace(self):
        cluster = FakeCluster()
        run_launch(cluster, "--context", CONTEXT, "--checks", "legacy-socket")
        self.assertEqual(cluster.deleted, [])

    def test_render_prints_the_wi_binding(self):
        cluster = FakeCluster()
        code, out = run_launch(cluster, "--render")
        self.assertEqual(code, launch.EXIT_OK)
        self.assertIn("serviceAccount:bnaylor-kagents-dev.svc.id.goog[slack-test/slack-test-runner]", out)
        self.assertIn("add-iam-policy-binding ka-slack-test-runner@bnaylor-kagents-dev.iam.gserviceaccount.com", out)

    def test_render_touches_nothing(self):
        cluster = FakeCluster()
        code, out = run_launch(cluster, "--render")
        self.assertEqual(code, launch.EXIT_OK)
        self.assertEqual(cluster.calls, [])
        self.assertIn("kind: Job", out)

    def test_owned_flags_after_the_separator_are_refused(self):
        for flag in ("--checks", "--run-id=x", "--project", "--keep-going"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                launch.parse_args(["--context", CONTEXT, "--", flag, "v"])

    def test_restart_needs_a_command_or_after_restart(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            launch.parse_args(["--context", CONTEXT, "--checks", "restart"])

    def test_context_is_required(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            launch.parse_args(["--checks", "dm"])

    def test_launcher_names_match_the_operator_source(self):
        cp = (REPO / "k8s-operator/internal/controller/credential_proxy_manifests.go").read_text()
        self.assertIn(f'const credentialProxyContainerName = "{launch.CREDENTIAL_PROXY_CONTAINER}"', cp)
        self.assertIn(f'return agent.Name + "{launch.CREDENTIAL_PROXY_SUFFIX}"', cp)
        a2a = (REPO / "k8s-operator/internal/controller/platformagent_a2a_manifests.go").read_text()
        self.assertIn(f'return agent.Name + "{launch.GATEWAY_SUFFIX}"', a2a)
        self.assertIn(f'Name:  "{launch.GATEWAY_CONTAINER}",', a2a)
        sandbox = (REPO / "k8s-operator/internal/controller/shell_sandbox_manifests.go").read_text()
        self.assertIn(f'return agent.Name + "{launch.SHELL_SANDBOX_SUFFIX}"', sandbox)
        proxy = (REPO / "agents/platform/scripts/credential_proxy.py").read_text()
        for marker in launch.LEGACY_RELAY_LOG_MARKERS:
            self.assertIn(f'"{marker}', proxy)
        self.assertIn('os.getenv("SLACK_BOT_TOKEN"', proxy)
        self.assertIn('os.getenv("SLACK_APP_TOKEN"', proxy)
        gateway = (REPO / "a2a/gateway/slack.go").read_text()
        self.assertIn(f's.log.Info("{launch.GATEWAY_SLACK_CONNECTED_MSG}"', gateway)
        ingress = (REPO / "a2a/gateway/gateway.go").read_text()
        self.assertIn(f'g.log.Info("{launch.GATEWAY_INGRESS_MSG}",', ingress)
        self.assertIn('"backendMessageId", msg.MessageID,', ingress)


if __name__ == "__main__":
    unittest.main()

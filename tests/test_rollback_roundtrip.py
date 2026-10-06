"""hack/rollback-roundtrip.sh, run whole against a simulated install.

The script flips a `spec.mode: next` install to `today` and back and asserts
the JetStream PVC and the bus creds Secret come through with their UIDs. Here
it runs end to end, unmodified, with `kubectl` on PATH replaced by a small
simulator of the operator (FAKE_KUBECTL below: a CR patch bumps generations,
and a few kubectl calls later the "reconcile" lands, tearing the A2A stack
down under today and rendering it again under next) and both doors served by
a local HTTP server, so the today turn is a real POST and the bus task is the
lane's own inject client talking to a fake gateway. Each failure case changes
one thing about what the simulated operator does and checks the run stops on
the assertion that names it.

The wiring in hack/ci-eval-pr.sh is pinned at the end: it runs only under
EVAL_MODE_NEXT=1, after the suite verdict is computed and before it is
announced, and nothing it does reaches the exit status.
"""

import base64
import http.server
import json
import os
import pathlib
import re
import signal
import stat
import subprocess
import tempfile
import textwrap
import threading
import time
import unittest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_SCRIPT = _REPO_ROOT / "hack" / "rollback-roundtrip.sh"
_CI_EVAL = _REPO_ROOT / "hack" / "ci-eval-pr.sh"
_CI_ENV = _REPO_ROOT / "hack" / "ci-env.sh"
_OPERATOR = _REPO_ROOT / "k8s-operator"
_A2A_MANIFESTS = _OPERATOR / "internal" / "controller" / "platformagent_a2a_manifests.go"
_CONTROLLER = _OPERATOR / "internal" / "controller" / "platformagent_controller.go"
_COMMON_TYPES = _OPERATOR / "api" / "v1alpha1" / "common_types.go"
_CALLOUT_KEYS = _OPERATOR / "internal" / "controller" / "platformagent_a2a_calloutkeys.go"

_NS = "kubeagents-system"
_CR = "platform-agent"
_API_KEY = "agent-api-key"
_PVC = "data-platform-agent-a2a-nats-0"
_CREDS = "platform-agent-a2a-nats-creds"
_BRIDGE = {
    "name": "hermes-bridge",
    "image": "bridge:dev",
    "env": [
        {"name": "NATS_URL", "value": f"nats://platform-agent-a2a-nats.{_NS}.svc:4222"},
        {"name": "NATS_PASSWORD", "valueFrom": {"secretKeyRef": {"name": _CREDS, "key": "bridge-password"}}},
        {"name": "API_SERVER_KEY", "value": "loopback-value-that-must-not-be-logged"},
    ],
}
_OTHER_SIDECAR = {"name": "log-shipper", "image": "shipper:1", "env": [{"name": "X", "value": "y"}]}

# Bounds for a run that is meant to pass: never reached. The timeout cases
# lower the one they test.
_FAST_ENV = {
    "ROLLBACK_POLL_SECONDS": "0",
    "ROLLBACK_PRE_READY_TIMEOUT": "30",
    "ROLLBACK_GENERATION_TIMEOUT": "30",
    "ROLLBACK_ROLLOUT_TIMEOUT": "30",
    "ROLLBACK_READY_TIMEOUT": "30",
    "ROLLBACK_TEARDOWN_TIMEOUT": "30",
    "ROLLBACK_BUS_UP_TIMEOUT": "30",
    "ROLLBACK_BRIDGE_TIMEOUT": "30",
    "ROLLBACK_SETTLE_TIMEOUT": "30",
    "ROLLBACK_TURN_TIMEOUT": "5",
    "ROLLBACK_TURN_ATTEMPTS": "2",
    "ROLLBACK_BUS_TASK_TIMEOUT": "10",
}

# Every assertion a passing run prints, in order, for a CR that declares the
# bridge sidecar.
_PASSING_ORDER = [
    "pre.mode-next",
    "pre.ready",
    "pre.nats-ready",
    "pre.callout-serving",
    "pre.gateway-serving",
    "pre.pvc-present",
    "pre.creds-present",
    "pre.bus-task",
    "leg1.bus-sidecar-unset.patched",
    "leg1.bus-sidecar-unset.reconciled",
    "leg1.bus-sidecar-unset.agent-rolled",
    "leg1.bus-sidecar-unset.ready",
    "leg1.mode-today.patched",
    "leg1.mode-today.reconciled",
    "leg1.mode-today.agent-rolled",
    "leg1.mode-today.ready",
    "leg1.a2a-torn-down",
    "leg1.pvc-kept",
    "leg1.creds-kept",
    "leg1.nothing-stuck",
    "leg1.not-degraded",
    "leg1.today-turn",
    "leg2.mode-next.patched",
    "leg2.mode-next.reconciled",
    "leg2.mode-next.agent-rolled",
    "leg2.mode-next.ready",
    "leg2.nats-ready",
    "leg2.callout-serving",
    "leg2.provisioned",
    "leg2.pvc-kept",
    "leg2.nats-on-kept-pvc",
    "leg2.creds-kept",
    "leg2.bus-sidecar-restored.patched",
    "leg2.bus-sidecar-restored.reconciled",
    "leg2.bus-sidecar-restored.agent-rolled",
    "leg2.bus-sidecar-restored.ready",
    "leg2.reprovisioned",
    "leg2.bridge-consuming",
    "leg2.verifier-serving",
    "leg2.gateway-serving",
    "leg2.bus-task",
    "leg2.nothing-stuck",
    "leg2.not-degraded",
]

# The simulated operator. State is one JSON file; every kubectl call is one
# tick, and a CR patch's reconcile lands RECONCILE_TICKS calls after it. The
# scenario dict in the state says which part of the reconcile misbehaves.
FAKE_KUBECTL = textwrap.dedent(
    r'''
    #!/usr/bin/env python3
    import base64, json, os, sys, time

    STATE = os.environ["FAKE_KUBE_STATE"]
    RECONCILE_TICKS = 3
    A2A = ["statefulset/platform-agent-a2a-nats", "deployment/platform-agent-a2a-callout",
           "deployment/platform-agent-a2a-gateway", "deployment/platform-agent-a2a-verifier",
           "secret/platform-agent-a2a-callout-keys", "secret/platform-agent-a2a-inject"]

    def load():
        with open(STATE) as f:
            return json.load(f)

    def save(s):
        tmp = STATE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(s, f)
        os.replace(tmp, STATE)

    def merge(dst, patch):
        for k, v in patch.items():
            if v is None:
                dst.pop(k, None)
            elif isinstance(v, dict) and isinstance(dst.get(k), dict):
                merge(dst[k], v)
            else:
                dst[k] = v

    def workload(kind, name, gen=1):
        return {"kind": kind, "metadata": {"name": name, "generation": gen},
                "spec": {"replicas": 1},
                "status": {"observedGeneration": gen, "updatedReplicas": 1, "readyReplicas": 1,
                           "availableReplicas": 1, "replicas": 1}}

    def bump(s, key, prefix):
        s["counters"][key] = s["counters"].get(key, 0) + 1
        return "%s-%d" % (prefix, s["counters"][key])

    def tick(s):
        s["tick"] += 1
        sc = s["scenario"]
        p = s.get("pending")
        if p and s["tick"] >= p["at"]:
            s["pending"] = None
            reconcile(s, p)
        u = s.get("unschedulable_until")
        if u is not None and s["tick"] >= u:
            s["unschedulable_until"] = None
            set_ready(s)

    def set_ready(s, phase="Ready", reason="Reconciled", status="True"):
        cr = s["cr"]
        cr["status"] = {"phase": phase, "conditions": [
            {"type": "Ready", "status": status, "reason": reason, "message": "m",
             "observedGeneration": cr["metadata"]["generation"]}]}
        # Once armed, every later status write carries it, as the operator's
        # would for a cause that has not gone away.
        if s.get("degraded_armed"):
            cr["status"]["conditions"].append({"type": "Degraded", "status": "True", "reason": "RBACIncomplete", "message": "m"})

    def reconcile(s, p):
        sc, cr, o = s["scenario"], s["cr"], s["objects"]
        mode = cr["spec"].get("mode", "today")
        dep = o["deployment/platform-agent-gateway"]
        dep["status"]["observedGeneration"] = dep["metadata"]["generation"]
        leg = "today" if mode == "today" else "next"
        if p["kind"] == "mode" and leg == "today":
            if sc.get("teardown_skipped") != "today":
                for key in A2A:
                    o.pop(key, None)
                o.pop("pod/platform-agent-a2a-nats-0", None)
                s["jobs"] = []
                s["pods"] = [x for x in s["pods"] if x["metadata"]["labels"].get("app.kubernetes.io/part-of") != "a2a-next"]
        if p["kind"] == "mode" and leg == "next":
            for key in A2A:
                kind, name = key.split("/")
                if kind == "secret":
                    o[key] = {"uid": bump(s, name, name), "data": {}}
                else:
                    o[key] = workload("StatefulSet" if kind == "statefulset" else "Deployment", name)
            token = bump(s, "token", "door-token")
            o["secret/platform-agent-a2a-inject"]["data"]["token"] = base64.b64encode(token.encode()).decode()
            claim = sc.get("nats_claim", "data-platform-agent-a2a-nats-0")
            o["pod/platform-agent-a2a-nats-0"] = {"spec": {"volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": claim}}]}}
            s["jobs"] = [job(bump(s, "job", "provision"), sc.get("provision_job", "Complete"))]
            if "pvc" in sc.get("replace_on_next", []):
                o["pvc/" + PVC]["uid"] = "pvc-new"
            if "creds" in sc.get("replace_on_next", []):
                o["secret/" + CREDS]["uid"] = "creds-new"
        if p["kind"] == "mode" and leg == "today":
            if "pvc" in sc.get("replace_on_today", []):
                o["pvc/" + PVC]["uid"] = "pvc-new"
            if "creds" in sc.get("replace_on_today", []):
                o["secret/" + CREDS]["uid"] = "creds-new"
            if "pvc" in sc.get("delete_on_today", []):
                o.pop("pvc/" + PVC, None)
        if p["kind"] == "sidecars" and leg == "next":
            s["jobs"].append(job(bump(s, "job", "provision"), "Complete"))
        if sc.get("stuck_pod_on") == leg and p["kind"] == "mode":
            s["pods"].append(sc["stuck_pod"])
        if sc.get("status_stale_on") == leg and p["kind"] == "mode":
            return
        if sc.get("never_ready_on") == leg and p["kind"] == "mode":
            set_ready(s, phase="Provisioning", reason="Provisioning", status="False")
            return
        if sc.get("refused_on") == leg and p["kind"] == "mode":
            set_ready(s, phase="Degraded", reason="A2AProvisionFailed", status="False")
            return
        if sc.get("unschedulable_on") == leg and p["kind"] == "mode":
            set_ready(s, phase="Degraded", reason="PodUnschedulable", status="False")
            s["cr"]["status"]["conditions"][0]["message"] = "0/3 nodes are available: 3 Insufficient cpu"
            s["unschedulable_until"] = s["tick"] + 6
            return
        if sc.get("degraded_on") == leg and p["kind"] == "mode":
            s["degraded_armed"] = True
        set_ready(s)

    PVC = "data-platform-agent-a2a-nats-0"
    CREDS = "platform-agent-a2a-nats-creds"

    def job(name, cond):
        conds = [] if cond == "Running" else [{"type": cond, "status": "True"}]
        return {"metadata": {"name": name, "labels": {"app.kubernetes.io/part-of": "a2a-next",
                "kubeagents.x-k8s.io/a2a-component": "provision"}}, "status": {"conditions": conds}}

    def jsonpath(obj, path):
        if path == "{.metadata.uid}|{.metadata.deletionTimestamp}":
            return "%s|%s" % (obj.get("uid", ""), obj.get("deleting", ""))
        if path.startswith("{.data."):
            return obj.get("data", {}).get(path[len("{.data."):-1], "")
        if path == "{.spec.mode}":
            return obj["spec"].get("mode", "")
        if path == "{.metadata.generation}":
            return str(obj["metadata"]["generation"])
        if path == "{.spec.volumes[*].persistentVolumeClaim.claimName}":
            return " ".join(v["persistentVolumeClaim"]["claimName"] for v in obj["spec"]["volumes"])
        sys.exit("fake kubectl: unsupported jsonpath " + path)

    def port_forward(args, dead):
        # A tunnel to the test's door server, as kubectl's would be to the
        # Service. One that is dead accepts and drops every connection, the
        # way a tunnel whose stream the API server lost does.
        import socket, threading
        local = int(args[2].split(":")[0])
        with open(os.environ["FAKE_PF_PIDS"], "a") as f:
            f.write("%d\n" % os.getpid())
        srv = socket.socket()
        # What Go's net.Listen sets, so a port whose last tunnel left
        # connections in TIME_WAIT binds again; a port someone still
        # listens on does not.
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(("127.0.0.1", local))
        except OSError as exc:
            sys.stderr.write("Unable to listen on port %d: %s\n" % (local, exc))
            return 1
        srv.listen(16)
        def pump(a, b):
            try:
                while True:
                    data = a.recv(65536)
                    if not data:
                        break
                    b.sendall(data)
            except OSError:
                pass
            finally:
                for x in (a, b):
                    try:
                        x.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
        while True:
            conn, _ = srv.accept()
            if dead:
                conn.close()
                continue
            up = socket.create_connection(("127.0.0.1", int(os.environ["FAKE_DOOR_PORT"])))
            threading.Thread(target=pump, args=(conn, up), daemon=True).start()
            threading.Thread(target=pump, args=(up, conn), daemon=True).start()

    def main(argv):
        s = load()
        tick(s)
        s["calls"].append(argv)
        args = list(argv)
        if args[:1] == ["--context"]:
            s["contexts"].append(args[1]); args = args[2:]
        if args[:1] == ["-n"]:
            args = args[2:]
        verb = args[0]
        out, rc = "", 0
        if verb == "patch":
            patch = json.loads(args[args.index("-p") + 1])
            s["patches"].append(patch)
            cr = s["cr"]
            merge(cr["spec"], patch.get("spec", {}))
            cr["metadata"]["generation"] += 1
            dep = s["objects"]["deployment/platform-agent-gateway"]
            dep["metadata"]["generation"] += 1
            kind = "mode" if "mode" in patch.get("spec", {}) else "sidecars"
            # The status is left as it was: until the reconcile lands it is
            # the previous generation's Ready, as on a real install.
            s["pending"] = {"at": s["tick"] + RECONCILE_TICKS, "kind": kind}
            out = "patched"
        elif verb == "logs":
            sidecars = s["cr"]["spec"].get("deployment", {}).get("sidecars") or []
            if s["cr"]["spec"].get("mode") == "next" and sidecars and not s["scenario"].get("bridge_silent"):
                out = '{"msg":"hermes bridge consuming","profile":"platform"}'
        elif verb == "get":
            kind, rest = args[1], args[2:]
            name = rest[0] if rest and not rest[0].startswith("-") else ""
            ignore = "--ignore-not-found" in rest
            fmt = rest[rest.index("-o") + 1] if "-o" in rest else ""
            if kind == "platformagent":
                obj = s["cr"]
                out = json.dumps(obj) if fmt == "json" else jsonpath(obj, fmt[len("jsonpath="):])
            elif kind == "pods":
                out = json.dumps({"items": s["pods"]})
            elif kind == "jobs":
                out = json.dumps({"items": s["jobs"]})
            else:
                obj = s["objects"].get("%s/%s" % (kind, name))
                if obj is None:
                    if not ignore:
                        sys.stderr.write('Error from server (NotFound): %s "%s" not found\n' % (kind, name)); rc = 1
                elif fmt == "json":
                    out = json.dumps(obj)
                elif fmt == "name":
                    out = "%s/%s" % (kind, name)
                else:
                    out = jsonpath(obj, fmt[len("jsonpath="):])
        elif verb == "port-forward":
            n = s["counters"].get("port-forward", 0)
            s["counters"]["port-forward"] = n + 1
            save(s)
            return port_forward(args, dead=bool(s["scenario"].get("dead_first_tunnel")) and n == 0)
        else:
            sys.exit("fake kubectl: unsupported " + " ".join(argv))
        save(s)
        sys.stdout.write(out)
        return rc

    sys.exit(main(sys.argv[1:]))
    '''
).lstrip()


def b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def workload(kind: str, name: str, gen: int = 1) -> dict:
    return {
        "kind": kind,
        "metadata": {"name": name, "generation": gen},
        "spec": {"replicas": 1},
        "status": {"observedGeneration": gen, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "replicas": 1},
    }


def healthy_next_state(sidecars: list | None = None, **scenario) -> dict:
    sidecars = [_BRIDGE] if sidecars is None else sidecars
    objects = {
        f"pvc/{_PVC}": {"uid": "pvc-1"},
        f"secret/{_CREDS}": {"uid": "creds-1", "data": {"bridge-password": b64("pw")}},
        "secret/platform-agent-secrets": {"uid": "s", "data": {"API_SERVER_KEY": b64(_API_KEY)}},
        "secret/platform-agent-a2a-inject": {"uid": "inject-1", "data": {"token": b64("door-token-0")}},
        "secret/platform-agent-a2a-callout-keys": {"uid": "keys-1", "data": {}},
        "statefulset/platform-agent-a2a-nats": workload("StatefulSet", "platform-agent-a2a-nats"),
        "deployment/platform-agent-a2a-callout": workload("Deployment", "platform-agent-a2a-callout"),
        "deployment/platform-agent-a2a-gateway": workload("Deployment", "platform-agent-a2a-gateway"),
        "deployment/platform-agent-a2a-verifier": workload("Deployment", "platform-agent-a2a-verifier"),
        "deployment/platform-agent-gateway": workload("Deployment", "platform-agent-gateway", 7),
        "pod/platform-agent-a2a-nats-0": {"spec": {"volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": _PVC}}]}},
    }
    spec: dict = {"mode": "next", "harness": {}}
    if sidecars:
        spec["deployment"] = {"sidecars": sidecars}
    cr = {
        "metadata": {"name": _CR, "generation": 4},
        "spec": spec,
        "status": {"phase": "Ready", "conditions": [{"type": "Ready", "status": "True", "reason": "Reconciled", "message": "m", "observedGeneration": 4}]},
    }
    pods = [
        {"metadata": {"name": "platform-agent-gateway-abc", "labels": {}}, "status": {"phase": "Running", "containerStatuses": [{"name": "platform-agent", "state": {"running": {}}}]}},
        {"metadata": {"name": "platform-agent-a2a-nats-0", "labels": {"app.kubernetes.io/part-of": "a2a-next"}}, "status": {"phase": "Running"}},
    ]
    jobs = [
        {"metadata": {"name": "provision-0", "labels": {"app.kubernetes.io/part-of": "a2a-next"}}, "status": {"conditions": [{"type": "Complete", "status": "True"}]}},
    ]
    return {"tick": 0, "cr": cr, "objects": objects, "pods": pods, "jobs": jobs, "pending": None, "scenario": scenario, "calls": [], "patches": [], "contexts": [], "counters": {}}


class _Door(http.server.BaseHTTPRequestHandler):
    """The agent API and the inject door, answering from the simulated state."""

    state_path: pathlib.Path

    def log_message(self, *args) -> None:  # noqa: D401 - silence the default access log
        pass

    def _state(self) -> dict:
        return json.loads(self.state_path.read_text())

    def _reply(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _door_token(self, state: dict) -> str:
        secret = state["objects"].get("secret/platform-agent-a2a-inject")
        return base64.b64decode(secret["data"]["token"]).decode() if secret else ""

    def _door_up(self, state: dict) -> bool:
        return state["cr"]["spec"].get("mode") == "next" and "deployment/platform-agent-a2a-gateway" in state["objects"]

    def do_POST(self) -> None:
        state = self._state()
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        auth = self.headers.get("Authorization", "")
        if self.path == "/v1/responses":
            if auth != f"Bearer {_API_KEY}" or state["scenario"].get("today_turn_fails"):
                self._reply(401, {"error": "no"})
                return
            self._reply(200, {"output": [{"role": "assistant", "content": "pong"}]})
            return
        if self.path == "/inject":
            if not self._door_up(state):
                self._reply(503, {"error": "no gateway"})
                return
            if auth != f"Bearer {self._door_token(state)}":
                self._reply(401, {"error": "stale token"})
                return
            self._reply(200, {"taskId": "task-" + body["conversation"].split("/")[1], "conversation": "inject:" + body["conversation"]})
            return
        self._reply(404, {})

    def do_GET(self) -> None:
        state = self._state()
        if not self.path.startswith("/conversations/"):
            self._reply(404, {})
            return
        conversation = self.path[len("/conversations/") :].split("?")[0]
        if "task=" not in self.path:
            self._reply(200, {"entries": [], "lastSeq": 0})
            return
        leg = "pre" if "pre.bus-task" in conversation else "leg2"
        final = "failed" if state["scenario"].get("bus_task_fails_on") == leg else "completed"
        task = re.search(r"task=([^&]+)", self.path).group(1)
        self._reply(200, {"entries": [{"kind": "terminal", "seq": 1, "taskId": task, "state": final}], "lastSeq": 1})


class Sim:
    """One simulated install, its doors, and a run of the script against it."""

    def __init__(self, state: dict) -> None:
        self.dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.dir.name)
        self.state_path = root / "state.json"
        self.state_path.write_text(json.dumps(state))
        self.bin = root / "bin"
        self.bin.mkdir()
        kubectl = self.bin / "kubectl"
        kubectl.write_text(FAKE_KUBECTL)
        kubectl.chmod(kubectl.stat().st_mode | stat.S_IEXEC)
        self.results = root / "results.txt"
        self.pf_pids = root / "pf_pids"
        handler = type("Door", (_Door,), {"state_path": self.state_path})
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.dir.cleanup()

    def env(self, **extra: str) -> dict:
        url = f"http://127.0.0.1:{self.server.server_address[1]}"
        env = dict(os.environ)
        env.update(_FAST_ENV)
        env.update(
            {
                "PATH": f"{self.bin}:{env['PATH']}",
                "FAKE_KUBE_STATE": str(self.state_path),
                "ROLLBACK_AGENT_URL": url,
                "ROLLBACK_INJECT_URL": url,
                "ROLLBACK_RESULTS_FILE": str(self.results),
                "ROLLBACK_KUBE_CONTEXT": "gke_test_ctx",
                "FAKE_PF_PIDS": str(self.pf_pids),
                "FAKE_DOOR_PORT": str(self.server.server_address[1]),
            }
        )
        env.update(extra)
        return env

    def run(self, **extra: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(_SCRIPT), _NS, _CR], capture_output=True, text=True, env=self.env(**extra), timeout=180, check=False
        )

    def state(self) -> dict:
        return json.loads(self.state_path.read_text())


def passed(result: subprocess.CompletedProcess) -> list[str]:
    return re.findall(r"^PASS (\S+):", result.stdout, re.MULTILINE)


def failed(result: subprocess.CompletedProcess) -> list[str]:
    return re.findall(r"^FAIL (\S+):", result.stdout, re.MULTILINE)


class RoundTripTest(unittest.TestCase):
    def run_sim(self, state: dict, **extra: str) -> tuple[subprocess.CompletedProcess, dict, Sim]:
        sim = Sim(state)
        self.addCleanup(sim.close)
        result = sim.run(**extra)
        return result, sim.state(), sim

    def assert_fails_at(self, result: subprocess.CompletedProcess, name: str, fragment: str = "") -> None:
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(failed(result), [name], result.stdout)
        lines = result.stdout.strip().splitlines()
        self.assertEqual(lines[-1], f"ROLLBACK ROUND TRIP FAILED at {name}", result.stdout)
        self.assertNotIn("ROLLBACK ROUND TRIP PASSED", result.stdout)
        if fragment:
            fail_line = next(line for line in lines if line.startswith(f"FAIL {name}:"))
            self.assertIn(fragment, fail_line)


class KeptRoundTripTest(RoundTripTest):
    def test_a_kept_round_trip_passes_every_assertion_in_order(self) -> None:
        result, state, sim = self.run_sim(healthy_next_state())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(passed(result), _PASSING_ORDER)
        self.assertEqual(failed(result), [])
        self.assertIn("ROLLBACK ROUND TRIP PASSED", result.stdout.strip().splitlines()[-1])
        self.assertIn("uid pvc-1", result.stdout)
        self.assertIn("uid creds-1", result.stdout)
        # Back where it started: next, with the bridge declared as it was.
        self.assertEqual(state["cr"]["spec"]["mode"], "next")
        self.assertEqual(state["cr"]["spec"]["deployment"]["sidecars"], [_BRIDGE])

    def test_the_patches_are_the_runbook_unset_flip_flip_restore(self) -> None:
        _, state, _ = self.run_sim(healthy_next_state())
        self.assertEqual(
            state["patches"],
            [
                {"spec": {"deployment": {"sidecars": None}}},
                {"spec": {"mode": "today"}},
                {"spec": {"mode": "next"}},
                {"spec": {"deployment": {"sidecars": [_BRIDGE]}}},
            ],
        )

    def test_a_sidecar_that_does_not_talk_to_the_bus_is_left_alone(self) -> None:
        result, state, _ = self.run_sim(healthy_next_state(sidecars=[_OTHER_SIDECAR, _BRIDGE]))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(state["patches"][0], {"spec": {"deployment": {"sidecars": [_OTHER_SIDECAR]}}})
        self.assertEqual(state["patches"][-1], {"spec": {"deployment": {"sidecars": [_OTHER_SIDECAR, _BRIDGE]}}})

    def test_no_bus_sidecar_skips_the_unset_and_the_restore(self) -> None:
        result, state, _ = self.run_sim(healthy_next_state(sidecars=[]))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("SKIP leg1.bus-sidecar-unset:", result.stdout)
        self.assertIn("SKIP leg2.bus-sidecar-restored:", result.stdout)
        self.assertEqual(state["patches"], [{"spec": {"mode": "today"}}, {"spec": {"mode": "next"}}])

    def test_every_kubectl_call_is_pinned_to_the_context(self) -> None:
        _, state, _ = self.run_sim(healthy_next_state())
        self.assertTrue(state["calls"])
        self.assertTrue(all(call[:2] == ["--context", "gke_test_ctx"] for call in state["calls"]))

    def test_the_sidecar_contents_never_reach_the_log(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state())
        self.assertNotIn("loopback-value-that-must-not-be-logged", result.stdout + result.stderr)

    def test_the_results_file_carries_every_line_and_the_outcome(self) -> None:
        result, _, sim = self.run_sim(healthy_next_state())
        lines = sim.results.read_text().splitlines()
        self.assertEqual([line.split(":")[0] for line in lines[:-1]], [f"PASS {name}" for name in _PASSING_ORDER])
        self.assertEqual(lines[-1], "PASSED")

    def test_the_bus_task_reads_the_door_token_fresh_after_the_flip(self) -> None:
        # The flip forward mints a new door token (the inject Secret is torn
        # down under today); a run that reused the first one would be
        # refused at the door with 401.
        result, state, _ = self.run_sim(healthy_next_state())
        self.assertEqual(result.returncode, 0, result.stdout)
        token = base64.b64decode(state["objects"]["secret/platform-agent-a2a-inject"]["data"]["token"]).decode()
        self.assertNotEqual(token, "door-token-0")

    def test_a_transient_capacity_degraded_is_waited_out(self) -> None:
        # #2414: the pod waits for an Autopilot node and the operator says
        # Degraded/PodUnschedulable for a while, then Ready.
        result, _, _ = self.run_sim(healthy_next_state(unschedulable_on="next"))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("PodUnschedulable", result.stdout)


def free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class PortForwardTest(RoundTripTest):
    """The path CI takes: no door URLs, so every turn and task goes through a
    port-forward the script starts and stops."""

    def tunnel_env(self) -> dict:
        return {"ROLLBACK_AGENT_URL": "", "ROLLBACK_INJECT_URL": "", "ROLLBACK_AGENT_LOCAL_PORT": str(free_port()), "ROLLBACK_INJECT_LOCAL_PORT": str(free_port()), "ROLLBACK_PORT_FORWARD_WAIT": "10"}

    def test_a_round_trip_through_port_forwards_passes_and_leaves_none_running(self) -> None:
        result, state, sim = self.run_sim(healthy_next_state(), **self.tunnel_env())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(passed(result), _PASSING_ORDER)
        forwards = [call for call in state["calls"] if "port-forward" in call]
        self.assertEqual(len(forwards), 3, forwards)
        self.assertTrue(all(call[:2] == ["--context", "gke_test_ctx"] for call in forwards))
        pids = [int(line) for line in sim.pf_pids.read_text().split()]
        time.sleep(0.5)
        self.assertEqual([pid for pid in pids if alive(pid)], [])

    def test_a_dead_first_tunnel_is_replaced_by_a_fresh_one(self) -> None:
        # The first port-forward (the pre bus task's) accepts and drops every
        # connection. The script's retry has to be a new tunnel on the same
        # port, which only binds if the dead one was really stopped.
        result, _, _ = self.run_sim(healthy_next_state(dead_first_tunnel=True), **self.tunnel_env(), ROLLBACK_BUS_TASK_ATTEMPTS="2")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS pre.bus-task:", result.stdout)
        self.assertIn("pre.bus-task: attempt 1/2", result.stdout)


class FailureTest(RoundTripTest):
    def test_a_pvc_replaced_by_the_flip_to_today(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(replace_on_today=["pvc"]))
        self.assert_fails_at(result, "leg1.pvc-kept", "uid pvc-1 before the flip, pvc-new now")

    def test_a_pvc_replaced_by_the_flip_back(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(replace_on_next=["pvc"]))
        self.assert_fails_at(result, "leg2.pvc-kept", "uid pvc-1 before the flip, pvc-new now")

    def test_a_creds_secret_replaced_by_the_flip_to_today(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(replace_on_today=["creds"]))
        self.assert_fails_at(result, "leg1.creds-kept", "uid creds-1 before the flip, creds-new now")

    def test_a_creds_secret_replaced_by_the_flip_back(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(replace_on_next=["creds"]))
        self.assert_fails_at(result, "leg2.creds-kept", "uid creds-1 before the flip, creds-new now")

    def test_a_pvc_missing_before_the_flip(self) -> None:
        state = healthy_next_state()
        del state["objects"][f"pvc/{_PVC}"]
        result, after, _ = self.run_sim(state)
        self.assert_fails_at(result, "pre.pvc-present", "does not exist")
        self.assertEqual(after["patches"], [])

    def test_a_pvc_deleted_by_the_flip_to_today(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(delete_on_today=["pvc"]))
        self.assert_fails_at(result, "leg1.pvc-kept", "is gone (it had uid pvc-1)")

    def test_a_pvc_being_deleted_counts_as_gone(self) -> None:
        state = healthy_next_state()
        state["objects"][f"pvc/{_PVC}"]["deleting"] = "2026-10-06T00:00:00Z"
        result, _, _ = self.run_sim(state)
        self.assert_fails_at(result, "pre.pvc-present", "is being deleted")

    def test_the_nats_pod_on_another_claim(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(nats_claim="data-something-else-0"))
        self.assert_fails_at(result, "leg2.nats-on-kept-pvc", "not data-platform-agent-a2a-nats-0")

    def test_a_crash_looping_pod_after_the_flip_to_today(self) -> None:
        stuck = {"metadata": {"name": "platform-agent-gateway-new", "labels": {}}, "status": {"phase": "Running", "containerStatuses": [{"name": "platform-agent", "state": {"waiting": {"reason": "CrashLoopBackOff"}}}]}}
        result, _, _ = self.run_sim(healthy_next_state(stuck_pod_on="today", stuck_pod=stuck), ROLLBACK_SETTLE_TIMEOUT="2")
        self.assert_fails_at(result, "leg1.nothing-stuck", "pod/platform-agent-gateway-new container platform-agent CrashLoopBackOff")

    def test_a_pending_pod_after_the_flip_back(self) -> None:
        stuck = {"metadata": {"name": "platform-agent-a2a-verifier-x", "labels": {"app.kubernetes.io/part-of": "a2a-next"}}, "status": {"phase": "Pending", "conditions": [{"type": "PodScheduled", "status": "False", "message": "0/3 nodes are available"}]}}
        result, _, _ = self.run_sim(healthy_next_state(stuck_pod_on="next", stuck_pod=stuck), ROLLBACK_SETTLE_TIMEOUT="2")
        self.assert_fails_at(result, "leg2.nothing-stuck", "pod/platform-agent-a2a-verifier-x Pending 0/3 nodes are available")

    def test_a_terminating_pod_that_never_leaves(self) -> None:
        stuck = {"metadata": {"name": "platform-agent-gateway-old", "labels": {}, "deletionTimestamp": "2026-10-06T00:00:00Z"}, "status": {"phase": "Running"}}
        result, _, _ = self.run_sim(healthy_next_state(stuck_pod_on="today", stuck_pod=stuck), ROLLBACK_SETTLE_TIMEOUT="2")
        self.assert_fails_at(result, "leg1.nothing-stuck", "terminating since")

    def test_an_a2a_job_left_under_today(self) -> None:
        # The teardown that never reaches the Jobs: they stay under today.
        state = healthy_next_state()
        state["scenario"]["stuck_pod_on"] = "today"
        state["scenario"]["stuck_pod"] = {"metadata": {"name": "platform-agent-a2a-nats-0", "labels": {"app.kubernetes.io/part-of": "a2a-next"}}, "status": {"phase": "Running"}}
        result, _, _ = self.run_sim(state, ROLLBACK_SETTLE_TIMEOUT="2")
        self.assert_fails_at(result, "leg1.nothing-stuck", "is an A2A pod still present under today")

    def test_a_failed_provision_job_on_the_flip_back(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(provision_job="Failed"))
        self.assert_fails_at(result, "leg2.provisioned", "failed")

    def test_degraded_after_the_flip_to_today(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(degraded_on="today"))
        self.assert_fails_at(result, "leg1.not-degraded", "Degraded=True/RBACIncomplete")

    def test_degraded_after_the_flip_back(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(degraded_on="next"))
        self.assert_fails_at(result, "leg2.not-degraded", "Degraded=True/RBACIncomplete")

    def test_never_ready_under_today_times_out_at_its_bound(self) -> None:
        started = time.monotonic()
        result, _, _ = self.run_sim(healthy_next_state(never_ready_on="today"), ROLLBACK_READY_TIMEOUT="2")
        self.assert_fails_at(result, "leg1.mode-today.ready", "not Ready at the current generation within 2s")
        self.assertLess(time.monotonic() - started, 60)

    def test_a_ready_left_over_from_the_previous_generation_is_not_ready(self) -> None:
        # The reconcile rolls the agent but never writes a status for the new
        # generation: the Ready on the CR is the one from before the patch.
        result, _, _ = self.run_sim(healthy_next_state(status_stale_on="today"), ROLLBACK_READY_TIMEOUT="2")
        self.assert_fails_at(result, "leg1.mode-today.ready", "(observedGeneration 5)")

    def test_a_teardown_that_never_happens_times_out(self) -> None:
        # Without this assertion a flip that tore nothing down would keep the
        # PVC and the Secret trivially.
        result, _, _ = self.run_sim(healthy_next_state(teardown_skipped="today"), ROLLBACK_TEARDOWN_TIMEOUT="2")
        self.assert_fails_at(result, "leg1.a2a-torn-down", "statefulset platform-agent-a2a-nats;")

    def test_a_refusal_fails_at_once_rather_than_at_the_bound(self) -> None:
        started = time.monotonic()
        result, _, _ = self.run_sim(healthy_next_state(refused_on="next"), ROLLBACK_READY_TIMEOUT="600")
        self.assert_fails_at(result, "leg2.mode-next.ready", "the operator refused the render")
        self.assertLess(time.monotonic() - started, 120)

    def test_no_answer_on_the_today_path(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(today_turn_fails=True))
        self.assert_fails_at(result, "leg1.today-turn", "no answer from /v1/responses in 2 attempts")

    def test_a_bus_task_that_does_not_complete_after_the_flip_back(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(bus_task_fails_on="leg2"))
        self.assert_fails_at(result, "leg2.bus-task", "terminal failed")

    def test_a_bus_task_that_does_not_complete_before_the_flip(self) -> None:
        result, after, _ = self.run_sim(healthy_next_state(bus_task_fails_on="pre"))
        self.assert_fails_at(result, "pre.bus-task", "terminal failed")
        self.assertEqual(after["patches"], [])

    def test_a_bridge_that_never_consumes(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(bridge_silent=True), ROLLBACK_BRIDGE_TIMEOUT="2")
        self.assert_fails_at(result, "leg2.bridge-consuming", "not consuming after 2s: hermes-bridge")

    def test_an_install_not_in_next(self) -> None:
        state = healthy_next_state()
        state["cr"]["spec"]["mode"] = "today"
        result, after, _ = self.run_sim(state)
        self.assert_fails_at(result, "pre.mode-next", "spec.mode is today")
        self.assertEqual(after["patches"], [])

    def test_a_failure_after_the_unset_says_where_the_sidecars_are(self) -> None:
        result, _, _ = self.run_sim(healthy_next_state(replace_on_today=["pvc"]))
        self.assertIn("this run unset the bus sidecar(s) hermes-bridge", result.stdout)
        self.assertIn("install left at spec.mode=today during leg1", result.stdout)

    def test_a_signal_is_a_named_failure(self) -> None:
        sim = Sim(healthy_next_state(never_ready_on="today"))
        self.addCleanup(sim.close)
        proc = subprocess.Popen(["bash", str(_SCRIPT), _NS, _CR], stdout=subprocess.PIPE, text=True, env=sim.env(ROLLBACK_READY_TIMEOUT="600"))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not any(p.get("spec", {}).get("mode") == "today" for p in sim.state()["patches"]):
            time.sleep(0.2)
        time.sleep(1)
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("FAIL leg1.interrupted:", out)
        self.assertEqual(out.strip().splitlines()[-1], "ROLLBACK ROUND TRIP FAILED at leg1.interrupted")


class NamesMatchTheOperatorTest(unittest.TestCase):
    """The names the script derives are the ones the operator renders."""

    def constants(self) -> dict[str, str]:
        found = {}
        for line in _SCRIPT.read_text().splitlines():
            match = re.match(r'readonly ([A-Z0-9_]+)="?([^"]*)"?$', line)
            if match:
                found[match.group(1)] = match.group(2)
        return found

    def go(self, path: pathlib.Path, pattern: str) -> str:
        match = re.search(pattern, path.read_text(), re.MULTILINE)
        self.assertIsNotNone(match, f"{pattern} not found in {path}")
        return match.group(1)

    def test_the_suffixes_are_the_api_packages(self) -> None:
        c = self.constants()
        self.assertEqual(c["NATS_NAME_SUFFIX"], self.go(_COMMON_TYPES, r'a2aNATSNameSuffix\s*=\s*"([^"]+)"'))
        self.assertEqual(c["CREDS_SECRET_SUFFIX"], self.go(_COMMON_TYPES, r'a2aCredsSecretSuffix\s*=\s*"([^"]+)"'))
        self.assertEqual(c["CALLOUT_NAME_SUFFIX"], self.go(_COMMON_TYPES, r'a2aCalloutNameSuffix\s*=\s*"([^"]+)"'))
        self.assertEqual(c["CALLOUT_KEYS_SUFFIX"], self.go(_COMMON_TYPES, r'a2aCalloutKeysSuffix\s*=\s*"([^"]+)"'))

    def test_the_claim_the_pvc_name_is_built_from_is_the_statefulsets(self) -> None:
        c = self.constants()
        self.assertEqual(c["NATS_DATA_CLAIM"], self.go(_A2A_MANIFESTS, r'^const a2aNATSDataClaim = "([^"]+)"'))
        # The operator's own spelling of the claim's name, which the script copies.
        self.assertIn('a2aNATSDataClaim + "-" + a2aNATSName(agent) + "-0"', _CONTROLLER.read_text())
        self.assertEqual(c["NATS_ORDINAL_SUFFIX"], "-0")

    def test_the_other_names_and_ports(self) -> None:
        c = self.constants()
        text = _A2A_MANIFESTS.read_text()
        self.assertIn(f'return agent.Name + "{c["A2A_GATEWAY_SUFFIX"]}"', text)
        self.assertIn(f'return agent.Name + "{c["INJECT_NAME_SUFFIX"]}"', text)
        self.assertIn(f'return agent.Name + "{c["A2A_VERIFIER_SUFFIX"]}"', text)
        self.assertEqual(c["INJECT_PORT"], self.go(_A2A_MANIFESTS, r"a2aInjectPort\s*=\s*(\d+)"))
        self.assertEqual(c["INJECT_TOKEN_FIELD"], self.go(_A2A_MANIFESTS, r'a2aInjectTokenKey\s*=\s*"([^"]+)"'))
        self.assertEqual(f'{c["A2A_PART_OF_LABEL"]}', self.go(_OPERATOR / "internal" / "controller" / "manifest_helpers.go", r'labelPartOf\s*=\s*"([^"]+)"'))
        self.assertEqual(c["A2A_PART_OF_VALUE"], self.go(_A2A_MANIFESTS, r'a2aPartOf\s*=\s*"([^"]+)"'))
        component = self.go(_A2A_MANIFESTS, r'a2aComponentLabel\s*=\s*"([^"]+)"')
        provision = self.go(_A2A_MANIFESTS, r'a2aProvisionComponent\s*=\s*"([^"]+)"')
        self.assertEqual(c["A2A_PROVISION_JOB_SELECTOR"], f"{component}={provision}")
        self.assertIn("A2ACalloutKeysSecretName(agent.Name)", _CALLOUT_KEYS.read_text())

    def test_the_kept_objects_are_on_neither_teardown_list(self) -> None:
        # What the script asserts is kept is what cleanupA2A leaves out.
        text = _A2A_MANIFESTS.read_text()
        start = text.index("func (r *PlatformAgentReconciler) a2aPreBusTeardown(")
        end = text.index("func (r *PlatformAgentReconciler) a2aNamespacedTeardown(")
        lists = text[start:end]
        self.assertNotIn("a2aCredsSecretName", lists)
        self.assertNotIn("PersistentVolumeClaim", lists)
        self.assertIn("a2aCalloutKeysName", lists)
        cleanup = text[text.index("func (r *PlatformAgentReconciler) cleanupA2A(") :]
        cleanup = cleanup[: cleanup.index("\nfunc ")]
        self.assertNotIn("a2aCredsSecretName", cleanup)
        self.assertNotIn("PersistentVolumeClaim", cleanup)
        # And the StatefulSet sets no PVC retention policy, so its default
        # (Retain) keeps the claim when the StatefulSet is deleted.
        sts = text[text.index("func buildA2ANATSStatefulSet(") :]
        sts = sts[: sts.index("\nfunc ")]
        self.assertNotIn("PersistentVolumeClaimRetentionPolicy", sts)

    def test_the_status_words_are_the_operators(self) -> None:
        c = self.constants()
        text = _CONTROLLER.read_text()
        for reason in c["CR_REFUSAL_REASONS"].split():
            self.assertIn(f'"{reason}"', text + _A2A_MANIFESTS.read_text(), reason)
        self.assertIn('newPhase = "Ready"', text)
        self.assertIn('newPhase = "Degraded"', text)


def ci_eval_function(name: str) -> str:
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}$", _CI_EVAL.read_text(), re.DOTALL | re.MULTILINE)
    if match is None:
        raise AssertionError(f"{name}() not found in {_CI_EVAL}")
    return match.group(0)


def ci_eval_constants() -> str:
    return "\n".join(line for line in _CI_EVAL.read_text().splitlines() if line.startswith("readonly EVAL_ROLLBACK_"))


class CiEvalWiringTest(unittest.TestCase):
    """run_rollback_roundtrip in hack/ci-eval-pr.sh, lifted and run."""

    def run_wiring(self, *, mode_next: str | None, script_status: int = 0, elapsed: int = 0, timeout_on_path: bool = False) -> tuple[subprocess.CompletedProcess, pathlib.Path]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = pathlib.Path(tmp.name)
        artifacts = root / "artifacts"
        artifacts.mkdir()
        (root / "rollback-roundtrip.sh").write_text(
            "echo \"stub ran: $* context=${ROLLBACK_KUBE_CONTEXT} results=${ROLLBACK_RESULTS_FILE}\"\n"
            "echo 'PASS pre.mode-next: x' >> \"${ROLLBACK_RESULTS_FILE}\"\n"
            f"exit {script_status}\n"
        )
        flag = "" if mode_next is None else f"export EVAL_MODE_NEXT={mode_next}"
        script = textwrap.dedent(
            f"""\
            set -euo pipefail
            {ci_eval_constants()}
            SCRIPT_DIR={root}
            ARTIFACT_DIR={artifacts}
            TARGET_NAMESPACE=kubeagents-system
            AGENT_SERVICE_NAME=platform-agent
            AGENT_CLUSTER_CONTEXT=gke_p_r_c
            START_TIME=$((SECONDS - {elapsed}))
            {flag}
            profile_begin() {{ echo "PROFILE $*"; }}
            collect_gateway_log() {{ echo COLLECT_GATEWAY; }}
            collect_agent_pod_diagnostics() {{ echo COLLECT_DIAG; }}
            {"" if timeout_on_path else "timeout() { shift 2; \"$@\"; }"}
            {ci_eval_function("run_rollback_roundtrip")}
            SUITE_STATUS=1
            run_rollback_roundtrip || true
            echo "SUITE_STATUS=${{SUITE_STATUS}}"
            run_rollback_roundtrip
            echo "returned=$?"
            """
        )
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60, check=False)
        return result, artifacts

    def test_skipped_outside_next_mode(self) -> None:
        for flag in (None, "", "0", "true"):
            with self.subTest(flag=flag):
                result, artifacts = self.run_wiring(mode_next=flag)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("stub ran", result.stdout)
                self.assertNotIn("PROFILE", result.stdout)
                self.assertNotIn("Rollback round trip", result.stdout)
                self.assertEqual(list(artifacts.iterdir()), [])

    def test_a_failed_round_trip_is_reported_and_changes_nothing(self) -> None:
        result, artifacts = self.run_wiring(mode_next="1", script_status=1)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("SUITE_STATUS=1", result.stdout)
        self.assertIn("returned=0", result.stdout)
        self.assertIn("Rollback round trip failed (exit 1); report-only", result.stdout)
        self.assertIn("stub ran: kubeagents-system platform-agent context=gke_p_r_c", result.stdout)
        self.assertIn("stub ran", (artifacts / "rollback-roundtrip.log").read_text())
        results = (artifacts / "rollback-roundtrip.txt").read_text().splitlines()
        self.assertEqual(results, ["PASS pre.mode-next: x", "OUTCOME: failed (exit 1)"])

    def test_a_passing_round_trip_is_reported(self) -> None:
        result, artifacts = self.run_wiring(mode_next="1", script_status=0)
        self.assertIn("Rollback round trip passed; report-only", result.stdout)
        self.assertIn("SUITE_STATUS=1", result.stdout)
        self.assertEqual((artifacts / "rollback-roundtrip.txt").read_text().splitlines()[-1], "OUTCOME: passed")

    def test_the_eval_logs_are_taken_before_the_flip(self) -> None:
        result, _ = self.run_wiring(mode_next="1")
        out = result.stdout
        self.assertLess(out.index("COLLECT_GATEWAY"), out.index("stub ran"))
        self.assertLess(out.index("COLLECT_DIAG"), out.index("stub ran"))

    def test_a_run_too_late_for_its_bound_is_skipped(self) -> None:
        result, artifacts = self.run_wiring(mode_next="1", elapsed=13000)
        self.assertNotIn("stub ran", result.stdout)
        self.assertIn("SKIPPED:", result.stdout)
        self.assertEqual((artifacts / "rollback-roundtrip.txt").read_text().splitlines()[-1], "OUTCOME: skipped")

    def test_it_runs_after_the_verdict_is_computed_and_before_it_is_announced(self) -> None:
        lines = _CI_EVAL.read_text().splitlines()
        call = [i for i, line in enumerate(lines) if line.strip().startswith("run_rollback_roundtrip") and "()" not in line]
        self.assertEqual(len(call), 1, "one call site")
        suite = next(i for i, line in enumerate(lines) if "uv run bench-gate suite" in line and "--partial" not in line and "||" not in line and line.startswith("(cd"))
        status = next(i for i in range(suite, len(lines)) if "|| SUITE_STATUS=$?" in lines[i])
        announce = next(i for i, line in enumerate(lines) if line.startswith('announce_suite_verdict "${SUITE_STATUS}"'))
        cases = next(i for i, line in enumerate(lines) if line.startswith("# ─── Per-case verdicts"))
        self.assertLess(cases, call[0])
        self.assertLess(status, call[0])
        self.assertEqual(call[0] + 1, announce)
        self.assertEqual(lines[call[0]], "run_rollback_roundtrip || true")
        # Not on the exit path: a run cut short never flips the install.
        self.assertNotIn("run_rollback_roundtrip", ci_eval_function("profile_and_dump_on_exit"))

    def test_a_deadline_term_during_the_round_trip_is_passed_on_and_exits_143(self) -> None:
        # Prow's deadline arrives as SIGTERM. Waited on in the foreground,
        # the round trip would hold it off for up to its hour; the wait lets
        # the trap run, the round trip hears the TERM, and the script exits
        # 143 with its EXIT trap, as the global trap would have it.
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "artifacts").mkdir()
            started = root / "started"
            (root / "rollback-roundtrip.sh").write_text(
                f"trap 'echo stub interrupted; exit 1' TERM\ntouch {started}\nwhile :; do sleep 1 & wait $!; done\n"
            )
            script = textwrap.dedent(
                f"""\
                set -euo pipefail
                {ci_eval_constants()}
                SCRIPT_DIR={root}
                ARTIFACT_DIR={root}/artifacts
                TARGET_NAMESPACE=kubeagents-system
                AGENT_SERVICE_NAME=platform-agent
                START_TIME=$SECONDS
                EVAL_MODE_NEXT=1
                profile_begin() {{ :; }}
                collect_gateway_log() {{ :; }}
                collect_agent_pod_diagnostics() {{ :; }}
                timeout() {{ shift 2; exec "$@"; }}
                trap 'echo EXIT TRAP RAN' EXIT
                trap 'exit 143' TERM INT
                {ci_eval_function("run_rollback_roundtrip")}
                run_rollback_roundtrip || true
                echo NOT REACHED
                """
            )
            proc = subprocess.Popen(["bash", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            deadline = time.monotonic() + 30
            while not started.exists() and time.monotonic() < deadline:
                time.sleep(0.1)
            self.assertTrue(started.exists())
            sent = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            out, _ = proc.communicate(timeout=30)
        self.assertLess(time.monotonic() - sent, 10)
        self.assertEqual(proc.returncode, 143, out)
        self.assertIn("stub interrupted", out)
        self.assertIn("EXIT TRAP RAN", out)
        self.assertNotIn("NOT REACHED", out)

    def test_the_function_never_assigns_the_suite_status(self) -> None:
        self.assertNotIn("SUITE_STATUS", ci_eval_function("run_rollback_roundtrip"))

    def test_the_bound_ends_inside_the_deadline(self) -> None:
        values = dict(re.findall(r"readonly (EVAL_ROLLBACK_\w+)=(\d+)", ci_eval_constants()))
        presubmit_deadline = 360 * 60
        deploy = 45 * 60
        total = int(values["EVAL_ROLLBACK_START_BY_SECONDS"]) + int(values["EVAL_ROLLBACK_TIMEOUT_SECONDS"]) + int(values["EVAL_ROLLBACK_KILL_AFTER_SECONDS"]) + deploy
        self.assertLess(total, presubmit_deadline)

    def test_the_script_named_exists(self) -> None:
        name = re.search(r'readonly EVAL_ROLLBACK_SCRIPT="([^"]+)"', ci_eval_constants()).group(1)
        self.assertEqual(_CI_EVAL.parent / name, _SCRIPT)
        self.assertTrue(os.access(_SCRIPT, os.X_OK))


class GatewayLogOnceTest(unittest.TestCase):
    def test_the_second_capture_does_not_overwrite_the_first(self) -> None:
        match = re.search(r"^GATEWAY_LOG_COLLECTED=\"\"\ncollect_gateway_log\(\) \{\n.*?^\}$", _CI_ENV.read_text(), re.DOTALL | re.MULTILINE)
        self.assertIsNotNone(match)
        with tempfile.TemporaryDirectory() as tmp:
            script = textwrap.dedent(
                f"""\
                set -euo pipefail
                GATEWAY_LOG_TAIL_LINES=10
                GATEWAY_LOG_MAX_BYTES=1000
                ARTIFACTS={tmp}
                kubectl() {{ echo x >> {tmp}/calls; echo "capture $(wc -l < {tmp}/calls | tr -d ' ')"; }}
                {match.group(0)}
                collect_gateway_log
                collect_gateway_log
                cat {tmp}/platform-agent-gateway.log
                """
            )
            result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "capture 1")


if __name__ == "__main__":
    unittest.main()

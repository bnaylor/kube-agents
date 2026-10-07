# Slack live check

A by-hand live check of the A2A gateway's Slack backend on a `spec.mode: next` install. Two Slack user accounts talk to the gateway's bot the way a person would, and each check prints a named PASS or FAIL with its evidence. It runs as a Kubernetes Job on the target cluster, so no workstation ever holds the user tokens. It is never run in CI and is not part of the `tests/e2e/` release gate.

| File                         | Runs where             | What it does                                                                                    |
| ---------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------- |
| `harness.py`                 | in the Job's pod       | Reads the user tokens, posts as the users, polls for the bot's replies. Standard library only.  |
| `launch.py`                  | your workstation       | Renders and applies the Job, prints its result lines, deletes it; runs the kubectl-side checks. |
| `manifests/*.yaml.template`  | applied by `launch.py` | The run namespace, its ServiceAccount and NetworkPolicy, and the per-run Job.                   |
| `../slack-live-check-run.sh` | your workstation       | The entry point: `exec`s `launch.py` with your arguments.                                       |

Offline tests: `tests/test_slack_live_check_harness.py` and `tests/test_slack_live_check_launch.py`, run by `make test-python`.

## What the checks prove

The harness always starts with a **preflight** for each user token it will use. It posts one message that does not mention the bot (in the test channel, or in the user's own DM if the user is not a member), reads it back with `conversations.history`, and fails if the message carries a `bot_id`, has a subtype outside `""`, `thread_broadcast` and `file_share`, or has no `user` (or a `user` other than the token's own). Those are the conditions under which `inbound()` in `a2a/gateway/slack.go` throws a message away as not a turn, so a failure here means every later check would time out for a reason unrelated to the gateway. It reports `app_id` when Slack sets one; the gateway ignores that field. A failed preflight stops the run even under `--keep-going`.

| Check           | Where it runs | Passes when                                                                                                                                                          |
| --------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dm`            | Job           | The listed user DMs the bot and a bot reply that is not the refusal notice arrives in the DM.                                                                        |
| `mention`       | Job           | The listed user mentions the bot in `--channel` and the reply is threaded under the mention.                                                                         |
| `thread`        | Job           | The listed user replies in that thread with no mention and the bot replies in the same thread. Needs `mention` in the same run, or `--thread-ts`.                    |
| `unlisted`      | Job           | The unlisted user DMs the bot (or mentions it, `--unlisted-via mention`) and gets the `⛔ I can't verify who you are on slack (id …)` notice naming their member id. |
| `restart`       | launcher, Job | After `--restart-cmd` (or `--after-restart`), the gateway rolls out and logs `slack connected`, then a second Job runs `dm` under this name.                         |
| `legacy-socket` | launcher      | The broker's legacy Slack relay is not armed and the gateway holds the Slack pair (below).                                                                           |
| `home`          | Job           | A bot post appears in `--home-channel` after `--home-since` (default: the run's start) within `--home-timeout`, matching `--home-match` if given. For G5.            |

By default a reply is the bot's first message, which is the `⏳ submitted…` status line. `--wait-answer` waits for the answer itself and fails on a `❌`, `🚫` or `🛑` line. Each wait is a bounded poll (`--reply-timeout`, default 180 s, every `--poll-interval`, default 5 s).

The refusal notice is sent once per sender per gateway process (`verifySender` in `a2a/gateway/gateway.go`). A second `unlisted` run against the same gateway therefore sees silence, which fails with that explanation; restart the gateway between runs, or pass `--refusal-silence-ok`. `--unlisted-repeat` sends a second message after the notice and passes only if nothing comes back within `--quiet-window`.

`legacy-socket` reads, from the PlatformAgent's namespace:

1. the `envoy-credential-proxy` container of the `<agent>-credential-proxy` Deployment, which fails the check if it carries `SLACK_BOT_TOKEN` or `SLACK_APP_TOKEN`, or any `envFrom`. The operator renders that pair on the broker only for the legacy consumer (`legacySlackConsumer` in `k8s-operator/internal/controller/platformagent_a2a_manifests.go`, used in `buildCredentialProxyEnv`), and `serve()` in `agents/platform/scripts/credential_proxy.py` starts `SlackRelay` only when both are non-empty;
2. that container's log since it started, which fails the check if it contains `Slack relay enabled` or `Slack relay initialization failed`, the two lines the relay's start-up thread logs once armed;
3. the `gateway` container of `<agent>-a2a-gateway` as the positive control: it must carry the pair and have logged `slack connected` (`SlackAdapter.Run` in `a2a/gateway/slack.go`).

`--expect-principal` (for #2547's member-id attribution) looks up the gateway's `ingress` log line for each passing listed turn by `backendMessageId` (the message's `ts`) and compares its `principal`. `{listed}` expands to the listed user's member id, so `--expect-principal 'slack:{listed}'` checks that an unmapped listed member is attributed `slack:<member id>`.

## Setup

You need:

- A Slack workspace you admin, with the gateway's app installed and its bot invited to `#ka-test`. The issue that owns this run (gke-labs/kube-agents#2098) carries the app manifest.
- Two Slack user tokens (`xoxp-`) in Secret Manager in `bnaylor-kagents-dev`: `slack-test-user-listed`, whose member id is on `spec.integration.slack.allowedUsers`, and `slack-test-user-unlisted`, whose id is not. Each needs the user scopes `chat:write`, `im:write`, `im:history`, `channels:history`, `groups:history`, `channels:read`, `groups:read` and `users:read`. The listed user must be a member of `#ka-test`.
- One bot, one socket. Slack spreads an app's events across every Socket Mode connection it has open, so exactly one gateway may hold the bot's app token at a time. The cluster manager moves the bot between installs with `scripts/slack-bot.sh` in its own workspace; check with it before you run, and never point a second install at the same app.

The Google side belongs to the cluster manager. The GSA `ka-slack-test-runner@bnaylor-kagents-dev.iam.gserviceaccount.com` holds `roles/secretmanager.secretAccessor` on exactly those two secrets. The one binding the run needs on top of that lets the run's Kubernetes ServiceAccount act as the GSA through Workload Identity:

```bash
gcloud iam service-accounts add-iam-policy-binding ka-slack-test-runner@bnaylor-kagents-dev.iam.gserviceaccount.com --project=bnaylor-kagents-dev --role=roles/iam.workloadIdentityUser --member='serviceAccount:bnaylor-kagents-dev.svc.id.goog[slack-test/slack-test-runner]'
```

`--render` prints that line for whatever `--project`, `--gsa`, `--namespace` and `--service-account` you pass. The launcher never runs gcloud.

The cluster side is the launcher's. Before its first Job it applies `manifests/setup.yaml.template`, idempotently: the `slack-test` namespace, the `slack-test-runner` ServiceAccount annotated `iam.gke.io/gcp-service-account: ka-slack-test-runner@bnaylor-kagents-dev.iam.gserviceaccount.com`, and a NetworkPolicy. It deletes the namespace when the run ends, and `--cleanup` deletes it on its own after an interrupted run. `--namespace`, `--service-account` and `--gsa` override the three names.

The pod reads the two secrets over the Secret Manager REST API with the token the GKE metadata server issues under Workload Identity. That needs only Workload Identity on the cluster; the Secret Manager CSI add-on would add a cluster add-on and a `SecretProviderClass` for the same result. `--token-source file` is there if a CSI mount is ever preferred.

The Job reaches the gateway only through Slack, never in-cluster. The NetworkPolicy admits nothing and lets the pod reach only DNS, the metadata server, and TCP 443 outside the private ranges, which is where `slack.com` and the Google APIs are. NetworkPolicy cannot name a host, so that last rule is as narrow as it gets without an FQDN policy.

## Running it

Take the shared cluster's lease first if you have one: the launcher creates and deletes the `slack-test` namespace and, with `--restart-cmd`, runs your restart. Then, in this order:

```bash
CTX=<kube-context>
# 1. No Slack traffic: the broker holds no socket, the gateway does.
hack/slack-live-check-run.sh --context "$CTX" --checks legacy-socket
# 2. The turns. dm, mention and thread as the listed user, then the refusal.
hack/slack-live-check-run.sh --context "$CTX" --checks dm,mention,thread,unlisted -- --wait-answer --unlisted-repeat
# 3. Restart the gateway and DM again. The command runs without a shell; pin its context yourself.
hack/slack-live-check-run.sh --context "$CTX" --checks restart \
  --restart-cmd "kubectl --context $CTX -n kubeagents-system rollout restart deployment/<agent>-a2a-gateway"
```

The run stops at the first FAIL unless `--keep-going` is given, and ends with an `OVERALL` line; the exit status is 0 only when every check passed. Everything after `--` goes to `harness.py` (`python3 hack/slack-live-check/harness.py --help` lists its flags, among them `--channel`, `--bot-user-id`, `--bot-name`, `--prompt`, `--reply-timeout`, `--listed-secret` and `--unlisted-secret`). `--checks`, `--project` and `--keep-going` are the launcher's, and the launcher refuses them after `--`. `--render` prints the manifests and touches nothing.

The Job runs in the install's own `agent-sandbox` image, read from the `<agent>-shell` StatefulSet (`--image` overrides it): it already has a `python3` and holds no credentials, so no new image enters the inventory. The harness is shipped in a ConfigMap. The pod runs as a non-root user with a read-only root filesystem, no capabilities and no mounted Kubernetes token, and has `backoffLimit: 0` so a failure never reposts. The launcher deletes the Job, the ConfigMap and then the namespace when the run ends. If it dies first, `ttlSecondsAfterFinished` collects the Job, and `hack/slack-live-check-run.sh --context "$CTX" --cleanup` removes the namespace and everything in it.

The harness keeps the tokens in memory only. They are never in the Job spec, the environment or a file, and every line it or the launcher prints goes through a redactor that cuts both the values it has read and anything shaped like a Slack or Google access token.

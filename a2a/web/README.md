# a2a/web — the bus, watched from a browser

The demo's web UI (`a2a-stream-demo/web`), lifted and adapted to
`a2a-jetstream/0.4`. The rail and the visual design came across intact; the
protocol underneath changed completely: addressee-scoped task subjects, the
0.4 envelope, the four reserved artifact names, four provisioned streams
instead of one, and — the point — a **read-only** connection. This page holds
the `web` NATS user's credential, whose grants stop at the JetStream read
API. It cannot publish, and the probe bar in the footer demonstrates that
live instead of asserting it.

PLAYGROUND POSTURE, stated out loud: static `web` credential from the creds
Secret, plain ws, no TLS, no ingress — the listener is ClusterIP and
`kubectl port-forward` is the only transport. Production terminates TLS in
front of the bus or keeps the listener off (spec-nats-deployment.md, web
read surface).

## Against the install

```sh
# 1. the password (key added by W6.1's repair path)
kubectl --context gke_bnaylor-kagents-dev_northamerica-northeast1_a2a-next-dev \
  -n kubeagents-system get secret platform-agent-a2a-nats-creds \
  -o jsonpath='{.data.web-password}' | base64 -d

# 2. the transport
kubectl --context gke_bnaylor-kagents-dev_northamerica-northeast1_a2a-next-dev \
  -n kubeagents-system port-forward svc/platform-agent-a2a-nats 9222:9222

# 3. the page
npm install && npm run dev
# open http://localhost:5173 and paste the password into the connect form,
# or pass it in the URL: /?ws=ws://localhost:9222&user=web&pass=...
```

Ask kage something in Discord; the rail lights, events tick, and clicking a
tap replays that session's events from the stream — no live executor asked.

## Local dev (no cluster)

`dev/nats.conf` mirrors the operator's rendered config on the points that
matter: the ws listener and the `web` user's exact grant list.

```sh
nats-server -c dev/nats.conf     # terminal 1
node dev/seed.mjs --live         # terminal 2: history + a task every ~20s
npm run dev                      # terminal 3, password dev-web
```

## Tests

```sh
npm test          # unit: protocol, reducer, rail geometry, components
npx tsc --noEmit  # strict, browser-shaped
# live, against a real server over real ws (W6-review style):
A2A_WS_URL=ws://localhost:9222 A2A_WEB_PASS=dev-web npm test -- livebus
```

The live suite drives the real bus layer — all four taps attach, seeded
history replays as non-live, a fresh publish arrives exactly once through
the dedup, and the read-only probe comes back `refused`. Against the
install, set `A2A_SKIP_SEED=1` (no seed user in hand) and the same suite
checks everything but the live-publish leg.

## Shape notes, for whoever touches this next

- **The grants dictate the client.** `_INBOX.web` must be the inbox prefix
  or every JS API reply is unsubscribable; consumers are ephemeral ordered
  *pull* consumers because `CONSUMER.CREATE.>` + `MSG.NEXT.*.*` is exactly
  that surface and `DURABLE.CREATE` is denied. Four attach loops, one per
  stream (`TASKS`/`DIRECTORY`/`TOPICS-STATE`/`TOPICS-JOURNAL`), each
  retrying independently so a fresh install lights up as provisioning runs.
- **No heartbeats.** Nothing on the install publishes `agents.hb.>` yet and
  the `web` user couldn't subscribe it anyway (`a2a.>` only). Rail liveness
  derives from stream traffic; `active` decays to `idle` after 60s quiet.
- **Retirement is data-driven.** A session that answers as the addressee of
  its own task subject (W4's session pods) retires to `done` on terminal; a
  standing service answering for a profile under its own name (the bridge:
  addressee `platform`, session `platform-bridge`) does not.
- **The transcript is the reserved artifact names.** `result` chunks merge
  into one answer entry, `progress` lines the transcript and the tap's
  status, `thinking`/`activity` stay off the transcript but count in replay.

/**
 * The browser's connection to the bus: one websocket as the read-only `web`
 * user, one JetStream tap per provisioned stream, a wall-clock tick, and the
 * connection's own status. There is no publish path here beyond the probe —
 * the `web` user's grants stop at the JetStream read API, and the probe
 * exists to demonstrate exactly that, live, instead of asserting it.
 *
 * Mechanics the grants dictate (see spec-nats-deployment.md, web read
 * surface): the inbox prefix must be `_INBOX.web` or every JS API reply is
 * unsubscribable; consumers are ephemeral ordered *pull* consumers, because
 * `CONSUMER.CREATE.>` + `MSG.NEXT.*.*` is precisely that surface and
 * `DURABLE.CREATE` is denied; and there are four streams, not one, so the
 * demo's single `a2a.>` tap becomes four attach loops that each retry
 * independently — a fresh install grows its streams one provisioning Job at
 * a time and the UI must light up as they appear, not reject.
 */
import { connect, type NatsConnection } from "nats.ws";
import { parseEnvelope, parseSubject, type Envelope } from "./protocol.ts";
import type { BusConfig } from "./config.ts";
import type { BusEvent, ProbeResult } from "./model.ts";

/** The four streams W6's provisioning Job creates; names are the contract. */
export const STREAMS = ["TASKS", "DIRECTORY", "TOPICS-STATE", "TOPICS-JOURNAL"] as const;

const TICK_MS = 5_000;
/** How long to wait before trying to attach to a missing stream again. */
const STREAM_RETRY_MS = 3_000;
/** How long the probe waits for the server's refusal before giving up. */
const PROBE_WAIT_MS = 2_000;
/** A provisioned, real subject: the refusal must be authorization, not a typo. */
const PROBE_SUBJECT = "a2a.topics.shared.blueprint";
/** Redelivery and tap restarts both repeat envelopes; this caps the dedup set. */
const DEDUP_MAX = 8_192;

export interface BusHandle {
  /**
   * Attempts a publish the `web` user must not be allowed to make and
   * reports what the server did. `refused` is the correct answer; `sent`
   * means the grant is broken and the UI says so just as loudly.
   */
  probeReadOnly(): Promise<ProbeResult>;
  close(): Promise<void>;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function startBus(
  config: BusConfig,
  dispatch: (e: BusEvent) => void,
): Promise<BusHandle> {
  // No waitOnFirstConnect: a wrong password or absent port-forward should
  // reject out to the connect form immediately, not hang the page. Once up,
  // an unlimited reconnect budget means a NATS restart mid-demo heals itself.
  const nc: NatsConnection = await connect({
    servers: config.url,
    user: config.user,
    pass: config.pass,
    name: "a2a-web",
    inboxPrefix: `_INBOX.${config.user}`,
    maxReconnectAttempts: -1,
  });
  let closed = false;

  // Permission violations arrive async on the status stream. The probe waits
  // on them; anything else that trips one (it shouldn't) surfaces the same way.
  // A property, not a let: the status loop below closes over it before the
  // probe ever assigns it, and TS narrows a captured let to its initial null.
  const violation: { notify: ((message: string) => void) | null } = { notify: null };

  dispatch({ type: "connection", state: "up" });
  void (async () => {
    for await (const s of nc.status()) {
      if (s.type === "disconnect") dispatch({ type: "connection", state: "down" });
      if (s.type === "reconnect") dispatch({ type: "connection", state: "up" });
      if (s.type === "error") {
        // A publish violation surfaces as data "PERMISSIONS_VIOLATION" with a
        // permissionContext naming the operation and subject.
        const perm = (s as { permissionContext?: { operation: string; subject: string } })
          .permissionContext;
        const message = String(s.data ?? "");
        if (perm?.operation === "publish") {
          violation.notify?.(`Permissions Violation for publish to "${perm.subject}"`);
        } else if (/permissions[ _]violation/i.test(message)) {
          violation.notify?.(message);
        }
      }
    }
  })().catch(() => {
    /* status iterator ends with the connection */
  });
  void nc.closed().then(() => {
    if (!closed) dispatch({ type: "connection", state: "down" });
  });

  const timer = setInterval(() => dispatch({ type: "tick", now: Date.now() }), TICK_MS);

  // One dedup set across all taps: a tap restart replays its stream from the
  // start, and the reducer must see each envelope once (assertion 5's analog).
  const seen = new Set<string>();
  const seenOrder: string[] = [];
  const dedup = (id: string): boolean => {
    if (seen.has(id)) return true;
    seen.add(id);
    seenOrder.push(id);
    if (seenOrder.length > DEDUP_MAX) {
      const evict = seenOrder.splice(0, seenOrder.length - DEDUP_MAX);
      for (const e of evict) seen.delete(e);
    }
    return false;
  };

  const js = nc.jetstream();
  const up = new Set<string>();
  const reportStreams = () =>
    dispatch({ type: "streams", up: up.size, total: STREAMS.length });
  reportStreams();

  const stoppers: (() => void)[] = [];

  for (const stream of STREAMS) {
    void (async () => {
      let complained = false;
      while (!closed) {
        try {
          // Snapshot where the stream ends *before* consuming: everything at
          // or below this sequence is history replaying into the UI,
          // everything above it is happening now and earns a pulse.
          const jsm = await nc.jetstreamManager();
          const info = await jsm.streams.info(stream);
          const lastSeqAtConnect = info.state.last_seq;

          // Ephemeral ordered pull consumer — the exact surface the web
          // user's grants describe. It self-heals across server restarts.
          const consumer = await js.consumers.get(stream);
          const messages = await consumer.consume();
          if (closed) {
            void messages.close();
            return;
          }
          stoppers.push(() => void messages.close());
          up.add(stream);
          reportStreams();
          complained = false;
          for await (const m of messages) {
            let env: Envelope;
            try {
              env = parseEnvelope(m.data);
            } catch {
              continue; // not ours, or malformed — surface nothing, skip
            }
            if (dedup(env.envelopeId)) continue;
            dispatch({
              type: "envelope",
              env,
              subject: parseSubject(m.subject),
              live: m.seq > lastSeqAtConnect,
            });
          }
          // Iterator ended: closed() path or a consume the server tore down.
          up.delete(stream);
          reportStreams();
          if (closed) return;
        } catch (error) {
          if (closed) return;
          up.delete(stream);
          reportStreams();
          if (!complained) {
            console.warn(`Waiting for the ${stream} stream (retrying):`, error);
            complained = true;
          }
        }
        await sleep(STREAM_RETRY_MS);
      }
    })();
  }

  return {
    async probeReadOnly(): Promise<ProbeResult> {
      const refusal = new Promise<string>((resolve) => {
        violation.notify = resolve;
      });
      const at = Date.now();
      try {
        nc.publish(
          PROBE_SUBJECT,
          new TextEncoder().encode(JSON.stringify({ probe: "a2a-web read-only check" })),
        );
        await nc.flush();
      } catch (error) {
        violation.notify = null;
        return { outcome: "error", detail: String(error), at };
      }
      const verdict = await Promise.race([
        refusal.then((detail): ProbeResult => ({ outcome: "refused", detail, at })),
        sleep(PROBE_WAIT_MS).then(
          (): ProbeResult => ({
            outcome: "sent",
            detail: `no refusal within ${PROBE_WAIT_MS / 1000}s — the publish went through; the web grant is broken`,
            at,
          }),
        ),
      ]);
      violation.notify = null;
      dispatch({ type: "probe", result: verdict });
      return verdict;
    },
    async close() {
      closed = true;
      clearInterval(timer);
      for (const stop of stoppers) stop();
      await nc.close();
    },
  };
}

import { connect, AckPolicy, DeliverPolicy } from "nats.ws";
// Port 9223 = the tightened list I already verified: CONSUMER.CREATE.TASKS.> (full-token)
const nc = await connect({ servers: "ws://localhost:9223", user: "web", pass: "dev-web", inboxPrefix: "_INBOX.web" });
const jsm = await nc.jetstreamManager();
const js = nc.jetstream();
const name = "web-" + Math.random().toString(36).slice(2, 10);
try {
  const ci = await jsm.consumers.add("TASKS", {
    name, ack_policy: AckPolicy.None, deliver_policy: DeliverPolicy.All,
    inactive_threshold: 30_000_000_000,
  });
  console.log("CREATED under full-token grant:", ci.name, "| durable:", ci.config.durable_name ?? "(none, ephemeral)");
  const c = await js.consumers.get("TASKS", name);
  const msgs = await c.consume({ max_messages: 10 });
  let n = 0;
  for await (const m of msgs) { n++; if (n >= 3) break; }
  console.log("CONSUMED via named pull consumer:", n);
  await msgs.close();
} catch (e) { console.log("FAILED:", String(e)); }
await nc.close();

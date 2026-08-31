import { describe, expect, it } from "vitest";
import { ProtocolError, parseEnvelope, parseSubject, partsText } from "./protocol.ts";

const valid = {
  protocol: "a2a-jetstream/0.4",
  envelopeId: "env-1",
  correlationId: "corr-1",
  ts: "2026-08-31T12:00:00Z",
  from: { session: "gateway", agentType: "a2a-gateway" },
  kind: "topic-update",
  payload: { name: "upgrade-readiness", parts: [] },
};

const encode = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));

describe("parseEnvelope", () => {
  it("accepts a valid 0.4 envelope", () => {
    const env = parseEnvelope(encode(valid));
    expect(env.kind).toBe("topic-update");
    expect(env.from.session).toBe("gateway");
  });

  it("accepts any minor within the major and keeps unknown fields", () => {
    const env = parseEnvelope(
      encode({ ...valid, protocol: "a2a-jetstream/0.9", futureField: 42 }),
    );
    expect((env as unknown as { futureField: number }).futureField).toBe(42);
  });

  it("rejects an unknown protocol major", () => {
    expect(() => parseEnvelope(encode({ ...valid, protocol: "a2a-jetstream/1.0" }))).toThrow(
      ProtocolError,
    );
    expect(() => parseEnvelope(encode({ ...valid, protocol: "bogus" }))).toThrow(ProtocolError);
  });

  it.each(["envelopeId", "correlationId", "ts"])("rejects a missing %s", (field) => {
    const bad: Record<string, unknown> = { ...valid };
    delete bad[field];
    expect(() => parseEnvelope(encode(bad))).toThrow(ProtocolError);
  });

  it("rejects a missing or sessionless from", () => {
    expect(() => parseEnvelope(encode({ ...valid, from: undefined }))).toThrow(ProtocolError);
    expect(() => parseEnvelope(encode({ ...valid, from: { agentType: "x" } }))).toThrow(
      ProtocolError,
    );
  });

  it("rejects an unknown kind", () => {
    expect(() => parseEnvelope(encode({ ...valid, kind: "task" }))).toThrow(ProtocolError);
  });

  it.each(["message", "status-update", "artifact-update", "cancel"] as const)(
    "requires taskId and contextId for kind %s",
    (kind) => {
      expect(() => parseEnvelope(encode({ ...valid, kind }))).toThrow(ProtocolError);
      const env = parseEnvelope(
        encode({ ...valid, kind, taskId: "task-1", contextId: "ctx-1", payload: {} }),
      );
      expect(env.taskId).toBe("task-1");
    },
  );

  it("does not require taskId for topic-update or the directory kinds", () => {
    for (const kind of ["topic-update", "agent-card", "agent-closed"] as const) {
      expect(() => parseEnvelope(encode({ ...valid, kind }))).not.toThrow();
    }
  });

  it("rejects non-JSON and non-objects", () => {
    expect(() => parseEnvelope("not json {{{")).toThrow(ProtocolError);
    expect(() => parseEnvelope(encode([1, 2]))).toThrow(ProtocolError);
  });
});

describe("parseSubject", () => {
  it("parses addressee-scoped task subjects", () => {
    expect(parseSubject("a2a.tasks.platform.task-77c0.in")).toEqual({
      plane: "tasks",
      addressee: "platform",
      taskId: "task-77c0",
      dir: "in",
    });
    expect(parseSubject("a2a.tasks.chat-otter.task-1.events")).toEqual({
      plane: "tasks",
      addressee: "chat-otter",
      taskId: "task-1",
      dir: "events",
    });
  });

  it("parses the directory and both topic scopes", () => {
    expect(parseSubject("a2a.agents.platform")).toEqual({ plane: "agents", profile: "platform" });
    expect(parseSubject("a2a.topics.agent.platform.upgrade-readiness")).toEqual({
      plane: "topics",
      scope: "agent",
      owner: "platform",
      topic: "upgrade-readiness",
    });
    expect(parseSubject("a2a.topics.shared.blueprint")).toEqual({
      plane: "topics",
      scope: "shared",
      topic: "blueprint",
    });
  });

  it("maps anything else to other", () => {
    expect(parseSubject("agents.hb.claude-code.acme.worker-otter").plane).toBe("other");
    expect(parseSubject("a2a.tasks.short").plane).toBe("other");
  });
});

describe("partsText", () => {
  it("concatenates text parts and skips non-text", () => {
    expect(
      partsText([{ kind: "text", text: "a" }, { kind: "data", data: {} }, { text: "b" }]),
    ).toBe("ab");
    expect(partsText(undefined)).toBe("");
  });
});

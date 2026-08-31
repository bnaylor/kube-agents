/**
 * The UI's brain: one immutable state tree and one pure reducer over bus
 * events. Everything the panes render is derived from here, and nothing in
 * here reads the clock or the network — timestamps always arrive on the event
 * so replay and live traffic reduce identically. Lifted from the demo UI and
 * reworked for a2a-jetstream/0.4: kinds changed (`task` → `message`,
 * `message-chunk` is gone), the transcript is driven by the four reserved
 * artifact names, and there are no heartbeats — the `web` user's read surface
 * is `a2a.>` only, and nothing on the install publishes heartbeats yet, so
 * liveness is derived from stream traffic.
 */
import {
  ARTIFACT_PROGRESS,
  ARTIFACT_RESULT,
  TERMINAL_STATES,
  partsText,
  type Artifact,
  type ArtifactUpdate,
  type Envelope,
  type Kind,
  type Message,
  type StatusUpdate,
  type SubjectInfo,
  type TaskState,
} from "./protocol.ts";

/** The rail tap for this browser. Not an agent; it reports the websocket. */
export const WEB_SESSION = "you";
/** The chatops gateway's session name (a2a/gateway/gateway.go). */
export const GATEWAY_SESSION = "gateway";
/** No traffic for longer than this and a standing agent reads as idle. */
export const IDLE_MS = 60_000;
/** The rail only ever animates a recent window of traffic. */
export const MAX_PULSES = 200;

export type AgentStatus = "active" | "idle" | "done" | "closed";

/** The browser's own link to the bus, which is not an agent lifecycle. */
export type ConnectionState = "connecting" | "up" | "down";

export interface AgentView {
  session: string;
  agentType: string;
  profile?: string;
  status: AgentStatus;
  /** ms since epoch, parsed from the last envelope's own `ts`. */
  lastActivity?: number;
  /** Latest `progress` artifact note, shown under the agent's tap. */
  statusLine?: string;
  /**
   * True when this session has answered as the addressee of its own task
   * subject — a per-session worker pod, which retires to `done` on terminal.
   * A standing service (the bridge answers for `platform` under the session
   * `platform-bridge`) goes back to idle instead.
   */
  perTask?: boolean;
}

export interface ArtifactView {
  name: string;
  text: string;
  chunks: number;
}

export interface TaskView {
  taskId: string;
  contextId: string;
  correlationId: string;
  /** The subject's addressee token: who this task is for. */
  addressee: string;
  /** The session that submitted it. */
  owner: string;
  /** The session that answered (first events publisher), once one has. */
  executor?: string;
  state: TaskState;
  final: boolean;
  artifacts: Map<string, ArtifactView>;
  /** ms since epoch of the last event, from envelope ts. */
  lastEventAt: number;
}

export type ChatKind = "user" | "steer" | "answer" | "progress" | "status" | "topic" | "cancel";

export interface ChatEntry {
  id: string;
  kind: ChatKind;
  session?: string;
  text: string;
  correlationId: string;
  taskId?: string;
}

export interface Pulse {
  /** Monotonically increasing; the rail's animation loop uses it as a watermark. */
  id: number;
  fromSession: string;
  correlationId: string;
  kind: Kind;
  at: number;
}

export type ProbeOutcome = "refused" | "sent" | "error";

export interface ProbeResult {
  outcome: ProbeOutcome;
  detail: string;
  at: number;
}

export interface UiState {
  agents: Map<string, AgentView>;
  tasks: Map<string, TaskView>;
  chat: ChatEntry[];
  pulses: Pulse[];
  streamMsgCount: number;
  connection: ConnectionState;
  /** JetStream taps attached, out of `streamsTotal`. */
  streamsUp: number;
  streamsTotal: number;
  /** Latest read-only probe result, if one was run. */
  probe?: ProbeResult;
}

export type BusEvent =
  | { type: "envelope"; env: Envelope; subject: SubjectInfo; live: boolean }
  | { type: "tick"; now: number }
  | { type: "connection"; state: ConnectionState }
  | { type: "streams"; up: number; total: number }
  | { type: "probe"; result: ProbeResult };

export const initialState: UiState = {
  agents: new Map(),
  tasks: new Map(),
  chat: [],
  pulses: [],
  streamMsgCount: 0,
  connection: "connecting",
  streamsUp: 0,
  streamsTotal: 0,
};

/**
 * Correlation ids get a stable hue so one conversational thread reads as one
 * colour everywhere — chat chips, rail pulses, replay strips. FNV-1a keeps
 * neighbouring uuids far apart in hue space.
 */
export function corrColor(corrId: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < corrId.length; i++) {
    h ^= corrId.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return `hsl(${(h >>> 0) % 360} 70% 60%)`;
}

function isTerminal(state: TaskState): boolean {
  return TERMINAL_STATES.includes(state);
}

function tsMs(env: Envelope): number {
  return Date.parse(env.ts) || 0;
}

function withAgent(
  agents: Map<string, AgentView>,
  session: string,
  patch: Partial<AgentView>,
): Map<string, AgentView> {
  const prev = agents.get(session);
  if (!prev) return agents;
  const next = new Map(agents);
  next.set(session, { ...prev, ...patch });
  return next;
}

/** Every session heard from is a tap on the rail; traffic alone earns one. */
function touchAgent(state: UiState, env: Envelope): Map<string, AgentView> {
  const session = env.from.session;
  if (session === "") return state.agents;
  const prev = state.agents.get(session);
  const agents = new Map(state.agents);
  agents.set(session, {
    session,
    agentType: env.from.agentType ?? prev?.agentType ?? "unknown",
    profile: env.from.profile ?? prev?.profile,
    statusLine: prev?.statusLine,
    perTask: prev?.perTask,
    status: prev?.status === "closed" ? "closed" : "active",
    lastActivity: Math.max(prev?.lastActivity ?? 0, tsMs(env)),
  });
  return agents;
}

/** Ensures the task exists, then folds in whatever this envelope knows. */
function upsertTask(
  tasks: Map<string, TaskView>,
  env: Envelope,
  subject: SubjectInfo,
  patch: Partial<TaskView> = {},
): Map<string, TaskView> {
  if (!env.taskId) return tasks;
  const prev = tasks.get(env.taskId);
  const addressee = subject.plane === "tasks" ? subject.addressee : "";
  const base: TaskView = prev ?? {
    taskId: env.taskId,
    contextId: env.contextId ?? "",
    correlationId: env.correlationId,
    addressee,
    owner: env.from.session,
    state: "submitted",
    final: false,
    artifacts: new Map(),
    lastEventAt: 0,
  };
  const next = new Map(tasks);
  next.set(env.taskId, {
    ...base,
    ...patch,
    lastEventAt: Math.max(base.lastEventAt, tsMs(env)),
  });
  return next;
}

function pushChat(state: UiState, entry: Omit<ChatEntry, "id">): ChatEntry[] {
  return [...state.chat, { id: `chat-${state.streamMsgCount + 1}`, ...entry }];
}

/** Streaming chunks of one artifact merge into a single transcript entry. */
function appendChunk(state: UiState, entry: Omit<ChatEntry, "id">): ChatEntry[] {
  const last = state.chat[state.chat.length - 1];
  const mergeable =
    last !== undefined &&
    last.kind === entry.kind &&
    last.session === entry.session &&
    last.taskId !== undefined &&
    last.taskId === entry.taskId;
  if (!mergeable) return pushChat(state, entry);
  const merged = [...state.chat];
  merged[merged.length - 1] = { ...last, text: last.text + entry.text };
  return merged;
}

function reduceMessage(next: UiState, state: UiState, env: Envelope, subject: SubjectInfo): void {
  const payload = env.payload as Message;
  const text = partsText(payload.parts);
  const known = state.tasks.get(env.taskId ?? "");
  // The first message on a task subject is the submission — the user's ask,
  // echoed from the stream so the transcript never trusts local state. A
  // later message on the same task is steering or follow-up input.
  const isSubmission = known === undefined;
  next.tasks = upsertTask(state.tasks, env, subject, isSubmission ? {} : undefined);
  next.chat = pushChat(state, {
    kind: isSubmission ? "user" : "steer",
    session: env.from.session,
    text,
    correlationId: env.correlationId,
    taskId: env.taskId,
  });
}

function reduceStatusUpdate(
  next: UiState,
  state: UiState,
  env: Envelope,
  subject: SubjectInfo,
): void {
  const payload = env.payload as StatusUpdate;
  const taskState = payload.status?.state ?? "working";
  const final = payload.final === true;
  next.tasks = upsertTask(state.tasks, env, subject, {
    state: taskState,
    final,
    executor: state.tasks.get(env.taskId ?? "")?.executor ?? env.from.session,
  });

  // A status that carries a message (input-required's question, a supervisor's
  // reason) belongs in the transcript.
  const note = partsText(payload.status?.message?.parts);
  if (note !== "") {
    next.chat = pushChat(state, {
      kind: "status",
      session: env.from.session,
      text: note,
      correlationId: env.correlationId,
      taskId: env.taskId,
    });
  }

  const session = env.from.session;
  if (subject.plane === "tasks" && session === subject.addressee) {
    // Answering as the addressee of its own subject: a per-session worker.
    next.agents = withAgent(next.agents, session, { perTask: true });
  }
  if ((final || isTerminal(taskState)) && session !== GATEWAY_SESSION) {
    const agent = next.agents.get(session);
    if (agent && agent.status !== "closed" && agent.perTask) {
      next.agents = withAgent(next.agents, session, { status: "done" });
    }
  }
}

function reduceArtifactUpdate(
  next: UiState,
  state: UiState,
  env: Envelope,
  subject: SubjectInfo,
): void {
  const payload = env.payload as ArtifactUpdate;
  const artifact = payload.artifact ?? {};
  const name = artifact.name ?? artifact.artifactId ?? "unnamed";
  const text = partsText(artifact.parts);

  const prev = env.taskId ? state.tasks.get(env.taskId) : undefined;
  const artifacts = new Map(prev?.artifacts ?? []);
  const existing = artifacts.get(name);
  artifacts.set(name, {
    name,
    text: payload.append && existing ? existing.text + text : text,
    chunks: (existing?.chunks ?? 0) + 1,
  });
  next.tasks = upsertTask(state.tasks, env, subject, {
    artifacts,
    executor: prev?.executor ?? env.from.session,
  });

  if (name === ARTIFACT_RESULT) {
    next.chat = appendChunk(state, {
      kind: "answer",
      session: env.from.session,
      text,
      correlationId: env.correlationId,
      taskId: env.taskId,
    });
  } else if (name === ARTIFACT_PROGRESS) {
    next.chat = pushChat(state, {
      kind: "progress",
      session: env.from.session,
      text,
      correlationId: env.correlationId,
      taskId: env.taskId,
    });
    next.agents = withAgent(next.agents, env.from.session, { statusLine: text });
  }
  // thinking/activity stay out of the transcript; they still pulse the rail
  // and count on the task for replay.
}

function reduceEnvelope(
  state: UiState,
  env: Envelope,
  subject: SubjectInfo,
  live: boolean,
): UiState {
  const next: UiState = { ...state, streamMsgCount: state.streamMsgCount + 1 };

  if (live) {
    const pulse: Pulse = {
      id: next.streamMsgCount,
      fromSession: env.from.session,
      correlationId: env.correlationId,
      kind: env.kind,
      at: tsMs(env),
    };
    const pulses = [...state.pulses, pulse];
    next.pulses = pulses.length > MAX_PULSES ? pulses.slice(pulses.length - MAX_PULSES) : pulses;
  }

  next.agents = touchAgent(state, env);
  const touched: UiState = { ...next };

  switch (env.kind) {
    case "message":
      reduceMessage(next, touched, env, subject);
      break;

    case "status-update":
      reduceStatusUpdate(next, touched, env, subject);
      break;

    case "artifact-update":
      reduceArtifactUpdate(next, touched, env, subject);
      break;

    case "cancel": {
      next.tasks = upsertTask(touched.tasks, env, subject);
      next.chat = pushChat(touched, {
        kind: "cancel",
        session: env.from.session,
        text: "cancel requested",
        correlationId: env.correlationId,
        taskId: env.taskId,
      });
      break;
    }

    case "agent-card": {
      // Published by the profile's owner for the profile, not by workers: the
      // card names a profile others can address, so the tap is the profile.
      if (subject.plane !== "agents") break;
      const key = subject.profile;
      const prev = touched.agents.get(key);
      const agents = new Map(touched.agents);
      agents.set(key, {
        session: key,
        agentType: prev?.agentType ?? "profile",
        profile: key,
        status: prev?.status === "closed" ? "active" : (prev?.status ?? "idle"),
        lastActivity: Math.max(prev?.lastActivity ?? 0, tsMs(env)),
        statusLine: prev?.statusLine,
        perTask: prev?.perTask,
      });
      next.agents = agents;
      break;
    }

    case "agent-closed": {
      if (subject.plane !== "agents") break;
      const key = subject.profile;
      const prev = touched.agents.get(key);
      if (!prev) break;
      next.agents = withAgent(touched.agents, key, { status: "closed" });
      break;
    }

    case "topic-update": {
      const artifact = env.payload as Artifact;
      const topic = subject.plane === "topics" ? subject.topic : (artifact.name ?? "topic");
      const summary = partsText(artifact.parts);
      next.chat = pushChat(touched, {
        kind: "topic",
        session: env.from.session,
        text: summary !== "" ? `${topic}: ${summary}` : `updated ${topic}`,
        correlationId: env.correlationId,
        taskId: env.taskId,
      });
      break;
    }
  }

  return next;
}

export function reduce(state: UiState, event: BusEvent): UiState {
  switch (event.type) {
    case "envelope":
      return reduceEnvelope(state, event.env, event.subject, event.live);

    case "tick": {
      let agents: Map<string, AgentView> | undefined;
      for (const [session, agent] of state.agents) {
        if (agent.status !== "active" && agent.status !== "idle") continue;
        const since = agent.lastActivity;
        const want: AgentStatus =
          since !== undefined && event.now - since > IDLE_MS ? "idle" : agent.status;
        if (want === agent.status) continue;
        agents ??= new Map(state.agents);
        agents.set(session, { ...agent, status: want });
      }
      return agents ? { ...state, agents } : state;
    }

    case "connection":
      return state.connection === event.state ? state : { ...state, connection: event.state };

    case "streams":
      return state.streamsUp === event.up && state.streamsTotal === event.total
        ? state
        : { ...state, streamsUp: event.up, streamsTotal: event.total };

    case "probe":
      return { ...state, probe: event.result };
  }
}

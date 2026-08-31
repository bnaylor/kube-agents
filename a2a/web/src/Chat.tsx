/**
 * Transcript pane, read-only. The demo's input box is gone on purpose: this
 * page holds the `web` credential, which cannot publish, and the pane's
 * footer is the place where that stops being an assertion — the verify
 * button publishes one probe and prints the server's refusal verbatim.
 *
 * Entries render by kind: `user` is the ask the gateway echoed onto the bus,
 * `steer` a follow-up into a running task, `answer` the result artifact
 * streaming in, `progress`/`status`/`topic`/`cancel` the quieter lines.
 * Each exchange group gets one correlation chip colored by corrColor.
 */
import { useEffect, useRef, useState } from "react";
import type { ChatEntry, ProbeResult } from "./model.ts";
import { corrColor } from "./model.ts";

interface ChatProps {
  entries: ChatEntry[];
  probe?: ProbeResult;
  onProbe: () => void;
}

const GLYPH: Record<ChatEntry["kind"], string> = {
  user: "ask>",
  steer: "steer>",
  answer: "",
  progress: "⏳",
  status: "⋯",
  topic: "⊙",
  cancel: "✕",
};

function probeText(probe: ProbeResult): string {
  switch (probe.outcome) {
    case "refused":
      return `server refused the publish: ${probe.detail}`;
    case "sent":
      return `PUBLISH WENT THROUGH — ${probe.detail}`;
    case "error":
      return `probe failed before the server saw it: ${probe.detail}`;
  }
}

export default function Chat({ entries, probe, onProbe }: ChatProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [shouldAutoScroll, setShouldAutoScroll] = useState(true);

  const handleScroll = () => {
    if (containerRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
      setShouldAutoScroll(scrollHeight - scrollTop - clientHeight < 10);
    }
  };

  useEffect(() => {
    if (shouldAutoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [entries, shouldAutoScroll]);

  const renderedEntries = entries.map((entry, idx) => {
    const isFirstInGroup =
      idx === 0 || entries[idx - 1].correlationId !== entry.correlationId;
    const corrChip = isFirstInGroup ? (
      <div
        className="corr-chip"
        style={{ backgroundColor: corrColor(entry.correlationId) }}
        title={`Correlation: ${entry.correlationId.slice(0, 8)}...`}
        data-corr={entry.correlationId}
      />
    ) : null;

    const glyph = GLYPH[entry.kind];
    return (
      <div key={entry.id} className="chat-entry-group">
        {corrChip}
        <div className={`chat-entry chat-${entry.kind}`}>
          {glyph !== "" && <span className="chat-glyph">{glyph}</span>}
          {(entry.kind === "progress" || entry.kind === "topic") && entry.session && (
            <span className="chat-session">[{entry.session}]</span>
          )}
          <span className="chat-text">{entry.text}</span>
        </div>
      </div>
    );
  });

  return (
    <div className="chat-pane">
      <div
        className="chat-transcript"
        ref={containerRef}
        onScroll={handleScroll}
      >
        {renderedEntries.length > 0 ? (
          renderedEntries
        ) : (
          <div className="chat-empty">watching the bus — ask kage something in Discord</div>
        )}
      </div>
      <div className="probe-bar">
        <span className="probe-label">
          connected as <code>web</code> · read-only
        </span>
        <button type="button" className="probe-button" onClick={onProbe}>
          verify
        </button>
        {probe && (
          <span className={`probe-result probe-${probe.outcome}`}>{probeText(probe)}</span>
        )}
      </div>
    </div>
  );
}

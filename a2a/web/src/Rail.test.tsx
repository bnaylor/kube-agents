// @vitest-environment jsdom
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import Rail from "./Rail.tsx";
import { initialState, type UiState } from "./model.ts";

afterEach(cleanup);

// jsdom has no ResizeObserver; the rail only needs it for live re-measure.
class FakeResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}
(globalThis as { ResizeObserver?: unknown }).ResizeObserver = FakeResizeObserver;

function withAgents(state: UiState, ...sessions: string[]): UiState {
  const agents = new Map(state.agents);
  for (const session of sessions) {
    agents.set(session, { session, agentType: "hermes-bridge", status: "active" });
  }
  return { ...state, agents };
}

describe("Rail", () => {
  it("always shows the fixed you/gateway head, even before any traffic", () => {
    render(<Rail state={initialState} />);
    expect(screen.getByText("you")).toBeTruthy();
    expect(screen.getByText("gateway")).toBeTruthy();
    expect(screen.getByText("connecting")).toBeTruthy();
    expect(screen.getByText("no traffic yet")).toBeTruthy();
  });

  it("reports the browser's link on the you tap", () => {
    render(<Rail state={{ ...initialState, connection: "up" }} />);
    expect(screen.getByText("websocket")).toBeTruthy();
  });

  it("puts sessions heard on the bus onto the rail as replayable taps", () => {
    const state = withAgents({ ...initialState, connection: "up" }, "platform-bridge");
    render(<Rail state={state} />);
    expect(screen.getByText("platform-bridge")).toBeTruthy();
    expect(screen.getByRole("button", { name: /replay platform-bridge/i })).toBeTruthy();
  });

  it("shows the stream attach count in the footer", () => {
    render(<Rail state={{ ...initialState, streamsUp: 4, streamsTotal: 4, streamMsgCount: 12 }} />);
    expect(screen.getByText(/4\/4 streams · 12 msgs/)).toBeTruthy();
  });
});

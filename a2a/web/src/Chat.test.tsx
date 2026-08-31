// @vitest-environment jsdom
import { describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach } from "vitest";
import Chat from "./Chat.tsx";
import type { ChatEntry } from "./model.ts";

afterEach(cleanup);

const entries: ChatEntry[] = [
  { id: "c1", kind: "user", session: "gateway", text: "are we ready?", correlationId: "corr-1" },
  {
    id: "c2",
    kind: "progress",
    session: "platform-bridge",
    text: "reading the topic",
    correlationId: "corr-1",
  },
  {
    id: "c3",
    kind: "answer",
    session: "platform-bridge",
    text: "acme-prod is ready",
    correlationId: "corr-1",
  },
];

describe("Chat", () => {
  it("renders the transcript read-only: no input, no send", () => {
    render(<Chat entries={entries} onProbe={() => {}} />);
    expect(screen.getByText("are we ready?")).toBeTruthy();
    expect(screen.getByText("acme-prod is ready")).toBeTruthy();
    expect(document.querySelector("input")).toBeNull();
    expect(screen.queryByText(/send/i)).toBeNull();
  });

  it("fires the read-only probe from the verify button", async () => {
    const onProbe = vi.fn();
    render(<Chat entries={[]} onProbe={onProbe} />);
    await userEvent.click(screen.getByRole("button", { name: /verify/i }));
    expect(onProbe).toHaveBeenCalledOnce();
  });

  it("shows the server's refusal verbatim and flags a probe that got through", () => {
    const { rerender } = render(
      <Chat
        entries={[]}
        probe={{
          outcome: "refused",
          detail: 'Permissions Violation for Publish to "a2a.topics.shared.blueprint"',
          at: 1,
        }}
        onProbe={() => {}}
      />,
    );
    expect(screen.getByText(/Permissions Violation for Publish/)).toBeTruthy();

    rerender(
      <Chat
        entries={[]}
        probe={{ outcome: "sent", detail: "no refusal within 2s — the publish went through; the web grant is broken", at: 2 }}
        onProbe={() => {}}
      />,
    );
    expect(screen.getByText(/PUBLISH WENT THROUGH/)).toBeTruthy();
  });

  it("groups by correlation with one chip per exchange", () => {
    const { container } = render(<Chat entries={entries} onProbe={() => {}} />);
    expect(container.querySelectorAll(".corr-chip")).toHaveLength(1);
  });
});

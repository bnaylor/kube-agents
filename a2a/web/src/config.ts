/**
 * Where the bus is and how to authenticate to it. Playground posture: the
 * `web` user is read-only by the server's own grants, the listener is plain
 * ws behind kubectl port-forward, and the password travels as a query param
 * or a pasted field — never baked into the bundle.
 */

export interface BusConfig {
  url: string;
  user: string;
  pass: string;
}

const STORAGE_KEY = "a2a-web-config";

export const DEFAULT_WS_URL = "ws://localhost:9222";
export const DEFAULT_USER = "web";

/** Query params override storage; storage remembers the last connect. */
export function loadConfig(): BusConfig | null {
  const params = new URLSearchParams(window.location.search);
  const url = params.get("ws");
  const pass = params.get("pass");
  if (pass !== null) {
    return {
      url: url ?? DEFAULT_WS_URL,
      user: params.get("user") ?? DEFAULT_USER,
      pass,
    };
  }
  try {
    const stored = sessionStorage.getItem(STORAGE_KEY);
    if (stored) return JSON.parse(stored) as BusConfig;
  } catch {
    // fall through to the connect form
  }
  return null;
}

export function saveConfig(config: BusConfig): void {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(config));
  } catch {
    // storage denied — the form will ask again next load
  }
}

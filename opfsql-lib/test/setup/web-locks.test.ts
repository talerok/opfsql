import { describe, expect, it } from "vitest";

// The polyfill must match what Chromium, Firefox and WebKit actually do,
// since the engine's tab coordination relies on these rules.

const tick = () => new Promise((resolve) => setTimeout(resolve, 5));

describe("Web Locks polyfill", () => {
  it("holds shared locks concurrently", async () => {
    const order: string[] = [];
    let releaseA!: () => void;
    const a = navigator.locks.request("shared-1", { mode: "shared" }, () => {
      order.push("A");
      return new Promise<void>((r) => (releaseA = r));
    });
    const b = navigator.locks.request("shared-1", { mode: "shared" }, () => {
      order.push("B");
    });
    await b;
    expect(order).toEqual(["A", "B"]);
    releaseA();
    await a;
  });

  it("queues fairly: a shared request waits behind an earlier exclusive one", async () => {
    const order: string[] = [];
    let releaseA!: () => void;
    const a = navigator.locks.request("fair-1", { mode: "shared" }, () => {
      order.push("A");
      return new Promise<void>((r) => (releaseA = r));
    });
    await tick();
    const x = navigator.locks.request("fair-1", { mode: "exclusive" }, () => {
      order.push("X");
    });
    await tick();
    const b = navigator.locks.request("fair-1", { mode: "shared" }, () => {
      order.push("B");
    });
    await tick();
    order.push("release A");
    releaseA();
    await Promise.all([a, x, b]);
    expect(order).toEqual(["A", "release A", "X", "B"]);
  });

  it("ifAvailable gets null instead of waiting", async () => {
    let release!: () => void;
    const held = navigator.locks.request("busy-1", () => {
      return new Promise<void>((r) => (release = r));
    });
    await tick();
    const lock = await navigator.locks.request(
      "busy-1",
      { ifAvailable: true },
      (l) => l,
    );
    expect(lock).toBeNull();
    release();
    await held;
    const free = await navigator.locks.request(
      "busy-1",
      { ifAvailable: true },
      (l) => l?.mode,
    );
    expect(free).toBe("exclusive");
  });

  it("withdraws a waiting request when its signal aborts", async () => {
    let release!: () => void;
    const held = navigator.locks.request("signal-1", async () => {
      await new Promise<void>((r) => (release = r));
    });
    const controller = new AbortController();
    const waiting = navigator.locks.request(
      "signal-1",
      { signal: controller.signal },
      () => "granted",
    );
    controller.abort(new Error("stop"));
    await expect(waiting).rejects.toThrow("stop");

    let hasRun = false;
    const after = navigator.locks.request("signal-1", () => {
      hasRun = true;
    });
    release();
    await held;
    await after;
    expect(hasRun).toBe(true);
  });

  it("rejects at once when the signal has already aborted", async () => {
    const signal = AbortSignal.abort(new Error("too late"));
    await expect(
      navigator.locks.request("signal-2", { signal }, () => "granted"),
    ).rejects.toThrow("too late");
  });

  it("ignores an abort after the grant", async () => {
    const controller = new AbortController();
    const granted = navigator.locks.request(
      "signal-3",
      { signal: controller.signal },
      async () => {
        controller.abort();
        await tick();
        return "kept";
      },
    );
    await expect(granted).resolves.toBe("kept");
  });
});

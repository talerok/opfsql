import { describe, expect, it } from "vitest";
import { Storage } from "../storage.js";
import type { SyncIPageStorage } from "../types.js";

/**
 * A versioned backend that records the oldest snapshot it is told about and
 * can fail to publish it, as a write to the read-mark file can.
 */
class PublishingBackend implements SyncIPageStorage {
  readonly pageCapacity = 4096;
  published: (number | null)[] = [];
  failPublishing = false;

  async open(): Promise<void> {
    // nothing to open
  }

  close(): void {
    // nothing to close
  }

  readPage(): null {
    return null;
  }

  getNextPageId(): number {
    return 3;
  }

  latestCommit(): number {
    return 7;
  }

  setOldestSnapshot(oldest: number | null): void {
    if (this.failPublishing) throw new Error("read marks unwritable");
    this.published.push(oldest);
  }

  commit(): void {
    // nothing to store
  }
}

describe("snapshot pins", () => {
  it("are not counted when publishing them fails", async () => {
    const backend = new PublishingBackend();
    const storage = new Storage(backend);
    await storage.open();
    const session = storage.createSession();

    backend.failPublishing = true;
    expect(() => session.beginSnapshot()).toThrow("read marks unwritable");
    backend.failPublishing = false;

    // No pin was left behind: the next one is the only one, and releasing
    // it leaves none.
    expect(session.beginSnapshot()).toBe(true);
    session.endSnapshot();
    expect(backend.published).toEqual([7, null]);
  });

  it("are forgotten even when publishing the release fails", async () => {
    const backend = new PublishingBackend();
    const storage = new Storage(backend);
    await storage.open();
    const session = storage.createSession();

    expect(session.beginSnapshot()).toBe(true);
    backend.failPublishing = true;
    expect(() => {
      session.endSnapshot();
    }).toThrow("read marks unwritable");
    backend.failPublishing = false;

    // The session holds no snapshot, and storage counts none.
    expect(session.isSnapshotStale()).toBe(false);
    expect(session.beginSnapshot()).toBe(true);
    session.endSnapshot();
    expect(backend.published).toEqual([7, 7, null]);
  });
});

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StorageFiles } from "../../storage/index.js";
import { Pager } from "../../storage/pager/pager.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { mulberry32 } from "../../../test/support/random.js";
import { openTab, sharedFiles } from "./tabs.js";

// ---------------------------------------------------------------------------
// Several engines ("tabs") on one database, coordinated the way real tabs
// are: Web Locks for writers, read marks for snapshots. Each tab has its own
// WAL index and schema cache; only the files are shared.
// ---------------------------------------------------------------------------

interface Tab {
  engine: Engine;
  session: Session;
}

let opened = 0;

async function openTabs(
  name: string,
  count: number,
  files: StorageFiles = sharedFiles(),
): Promise<Tab[]> {
  const unique = `${name}-${String(opened++)}`;
  const tabs: Tab[] = [];
  for (let i = 0; i < count; i++) {
    const engine = await openTab(unique, files);
    tabs.push({ engine, session: engine.connect() });
  }
  return tabs;
}

/** Where the read-mark file keeps slot `slot`'s mark: a float64 after the header. */
function slotOffset(slot: number): number {
  return 24 + slot * 8;
}

/** The slot of a tab that died: the last, which no test tab claims. */
const ORPHAN_SLOT = 63;

/** Writes the mark a tab that died holding snapshot 1 would have left. */
function writeOrphanMark(marks: StorageFiles["marks"]): void {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, 1);
  marks!.write(bytes, { at: slotOffset(ORPHAN_SLOT) });
}

function markIn(marks: StorageFiles["marks"], slot: number): number {
  const bytes = new Uint8Array(8);
  marks!.read(bytes, { at: slotOffset(slot) });
  return new DataView(bytes.buffer).getFloat64(0);
}

async function rows(session: Session): Promise<unknown[]> {
  const [result] = await session.execute("SELECT id, v FROM t ORDER BY id");
  return result.objects();
}

beforeEach(() => {
  vi.restoreAllMocks();
});

// Every step re-reads and integrity-checks the database from each tab, so the
// property tests take seconds rather than the default 5 s budget.
describe("multiple tabs", { timeout: 30_000 }, () => {
  for (const seed of [1, 2, 3]) {
    it(`interleaved writes and checkpoints keep every tab consistent (seed ${seed})`, async () => {
      const random = mulberry32(seed);
      const tabs = await openTabs(`tabs-${seed}`, 2);
      const model = new Map<number, number>();
      let nextId = 1;
      const pick = <T>(items: T[]): T =>
        items[Math.floor(random() * items.length)];

      await tabs[0].session.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
      );

      for (let step = 0; step < 400; step++) {
        const { engine, session } = pick(tabs);
        const ids = [...model.keys()];
        const r = random();
        const v = Math.floor(random() * 1000);

        if (r < 0.4 || ids.length === 0) {
          const id = nextId++;
          await session.query("INSERT INTO t (id, v) VALUES ($1, $2)", [id, v]);
          model.set(id, v);
        } else if (r < 0.6) {
          const id = pick(ids);
          await session.query("UPDATE t SET v = $1 WHERE id = $2", [v, id]);
          model.set(id, v);
        } else if (r < 0.75) {
          const id = pick(ids);
          await session.query("DELETE FROM t WHERE id = $1", [id]);
          model.delete(id);
        } else if (r < 0.9) {
          await session.execute("BEGIN");
          for (let i = 0; i < 3; i++) {
            const id = nextId++;
            await session.query("INSERT INTO t (id, v) VALUES ($1, $2)", [
              id,
              v + i,
            ]);
            model.set(id, v + i);
          }
          await session.execute("COMMIT");
        } else {
          await engine.checkpoint();
        }

        const expected = [...model.entries()]
          .sort(([a], [b]) => a - b)
          .map(([id, v]) => ({ id, v }));
        for (const tab of tabs) {
          expect(await rows(tab.session)).toEqual(expected);
          expect(await tab.engine.checkIntegrity()).toEqual([]);
        }
      }
    });
  }

  // Tabs open and close read transactions at random while others write and
  // checkpoint; every open transaction must keep seeing the model as of its
  // BEGIN, and every other read must see the latest model.
  for (const seed of [1, 2, 3]) {
    it(`transactions in every tab keep their snapshots (seed ${seed})`, async () => {
      const random = mulberry32(seed);
      const tabs = await openTabs(`tx-tabs-${seed}`, 3);
      const model = new Map<number, number>();
      const snapshots: Array<unknown[] | null> = tabs.map(() => null);
      const expected = () =>
        [...model.entries()]
          .sort(([a], [b]) => a - b)
          .map(([id, v]) => ({ id, v }));
      let nextId = 1;

      await tabs[0].session.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
      );

      for (let step = 0; step < 300; step++) {
        const i = Math.floor(random() * tabs.length);
        const { engine, session } = tabs[i];
        const r = random();
        const ids = [...model.keys()];

        if (snapshots[i] === null && r < 0.45) {
          if (ids.length === 0 || r < 0.25) {
            const id = nextId++;
            await session.query("INSERT INTO t (id, v) VALUES ($1, $2)", [
              id,
              step,
            ]);
            model.set(id, step);
          } else {
            const id = ids[Math.floor(random() * ids.length)];
            await session.query("DELETE FROM t WHERE id = $1", [id]);
            model.delete(id);
          }
        } else if (snapshots[i] === null && r < 0.65) {
          await session.execute("BEGIN");
          snapshots[i] = expected();
        } else if (snapshots[i] !== null && r < 0.8) {
          await session.execute("COMMIT");
          snapshots[i] = null;
        } else {
          await engine.checkpoint();
        }

        for (const [j, tab] of tabs.entries()) {
          expect(await rows(tab.session)).toEqual(snapshots[j] ?? expected());
          expect(await tab.engine.checkIntegrity()).toEqual([]);
        }
      }
    });
  }

  it("a tab that died with a snapshot open does not hold checkpoints back for good", async () => {
    const files = sharedFiles();
    const [writer] = await openTabs("dead-tab", 1, files);
    await writer.session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    writeOrphanMark(files.marks);
    // Only a restart truncates the WAL here: no append fails in this test.
    const restarts = vi.spyOn(files.wal, "truncate");

    for (let id = 1; id <= 40; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    // The orphan's mark was cleared, and a checkpoint then went all the way
    // and restarted the WAL.
    expect(markIn(files.marks, ORPHAN_SLOT)).toBe(-1);
    expect(restarts).toHaveBeenCalled();
  });

  it("close() waits for the background release of dead tabs' read marks", async () => {
    const files = sharedFiles();
    const [writer] = await openTabs("closing", 1, files);
    await writer.session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    writeOrphanMark(files.marks);
    const events: string[] = [];
    const gate = Promise.withResolvers<undefined>();
    const release = Reflect.get<Pager, "releaseAbandonedSnapshots">(
      Pager.prototype,
      "releaseAbandonedSnapshots",
    );
    vi.spyOn(Pager.prototype, "releaseAbandonedSnapshots").mockImplementation(
      async function released(this: Pager) {
        await gate.promise;
        await release.call(this);
        events.push("released");
      },
    );
    const close = Reflect.get<Pager, "close">(Pager.prototype, "close");
    vi.spyOn(Pager.prototype, "close").mockImplementation(function closed(
      this: Pager,
    ) {
      events.push("closed");
      close.call(this);
    });

    // Checkpoints the orphan holds back start releases, which wait on `gate`.
    for (let id = 1; id <= 20; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    const closing = writer.engine.close();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(events).toEqual([]);

    gate.resolve(undefined);
    await closing;
    expect(events.at(-1)).toBe("closed");
    expect(events.filter((event) => event === "closed")).toHaveLength(1);
    expect(events).toContain("released");
  });

  it("AUTOINCREMENT ids stay unique when tabs take turns inserting", async () => {
    const tabs = await openTabs("autoincrement", 2);
    await tabs[0].session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)",
    );
    for (let i = 0; i < 50; i++) {
      const { engine, session } = tabs[i % 2];
      await session.query("INSERT INTO t (v) VALUES ($1)", [i]);
      if (i % 7 === 0) await engine.checkpoint();
    }

    const [result] = await tabs[1].session.execute(
      "SELECT id, v FROM t ORDER BY id",
    );
    expect(result.objects()).toEqual(
      Array.from({ length: 50 }, (_, i) => ({ id: i + 1, v: i })),
    );
  });

  it("a transaction keeps its snapshot while another tab writes and checkpoints", async () => {
    const [writer, reader] = await openTabs("snapshot", 2);
    await writer.session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    for (let id = 1; id <= 100; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    await writer.engine.checkpoint();

    await reader.session.execute("BEGIN");
    const snapshot = await rows(reader.session);
    for (let id = 101; id <= 300; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    await writer.engine.checkpoint();

    expect(await rows(reader.session)).toEqual(snapshot);
    await reader.session.execute("COMMIT");
  });
});

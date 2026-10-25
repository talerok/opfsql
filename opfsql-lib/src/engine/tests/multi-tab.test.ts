import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import { MemoryFileHandle } from "../../store/wal/file-handle.js";
import { WalStorage } from "../../store/wal/wal-storage.js";
import { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { mulberry32 } from "./random.js";

// ---------------------------------------------------------------------------
// Several engines ("tabs") on one database, coordinated through Web Locks the
// way real tabs are. Each tab has its own WAL index and catalog; only the
// files are shared.
// ---------------------------------------------------------------------------

interface Tab {
  engine: Engine;
  session: Session;
}

/**
 * Real tabs each open their own handles in readwrite-unsafe mode. opfs-mock
 * allows one handle per file, so the tabs share the handle objects instead —
 * the bytes every tab reads and writes are the same either way.
 */
async function openTabs(name: string, count: number): Promise<Tab[]> {
  const main = new OPFSSyncStorage(name, { pageSize: 4096 });
  const walFile = new MemoryFileHandle();
  const tabs: Tab[] = [];
  for (let i = 0; i < count; i++) {
    const engine = await Engine.create(new WalStorage(main, walFile, 8), {
      lockName: `opfsql:${name}`,
    });
    tabs.push({ engine, session: engine.createSession() });
  }
  return tabs;
}

async function rows(session: Session): Promise<unknown[]> {
  const [result] = await session.execute("SELECT id, v FROM t ORDER BY id");
  return result.type === "rows" ? result.rows : [];
}

beforeEach(() => {
  resetMockOPFS();
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
          await session.execute("INSERT INTO t (id, v) VALUES ($1, $2)", [
            id,
            v,
          ]);
          model.set(id, v);
        } else if (r < 0.6) {
          const id = pick(ids);
          await session.execute("UPDATE t SET v = $1 WHERE id = $2", [v, id]);
          model.set(id, v);
        } else if (r < 0.75) {
          const id = pick(ids);
          await session.execute("DELETE FROM t WHERE id = $1", [id]);
          model.delete(id);
        } else if (r < 0.9) {
          await session.execute("BEGIN");
          for (let i = 0; i < 3; i++) {
            const id = nextId++;
            await session.execute("INSERT INTO t (id, v) VALUES ($1, $2)", [
              id,
              v + i,
            ]);
            model.set(id, v + i);
          }
          await session.execute("COMMIT");
        } else {
          engine.checkpoint();
        }

        const expected = [...model.entries()]
          .sort(([a], [b]) => a - b)
          .map(([id, v]) => ({ id, v }));
        for (const tab of tabs) {
          expect(await rows(tab.session)).toEqual(expected);
          expect(tab.engine.checkIntegrity()).toEqual([]);
        }
      }
    });
  }

  it("AUTOINCREMENT ids stay unique when tabs take turns inserting", async () => {
    const tabs = await openTabs("autoincrement", 2);
    await tabs[0].session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)",
    );
    for (let i = 0; i < 50; i++) {
      const { engine, session } = tabs[i % 2];
      await session.execute("INSERT INTO t (v) VALUES ($1)", [i]);
      if (i % 7 === 0) engine.checkpoint();
    }

    const [result] = await tabs[1].session.execute(
      "SELECT id, v FROM t ORDER BY id",
    );
    expect(result.type === "rows" ? result.rows : []).toEqual(
      Array.from({ length: 50 }, (_, i) => ({ id: i + 1, v: i })),
    );
  });

  // Known bug, fixed by coordinating readers with checkpoints: a checkpoint
  // overwrites main-file pages in place while another tab's transaction may
  // still be reading their old versions.
  it.fails(
    "a transaction keeps its snapshot while another tab writes and checkpoints",
    async () => {
      const [writer, reader] = await openTabs("snapshot", 2);
      await writer.session.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
      );
      for (let id = 1; id <= 100; id++) {
        await writer.session.execute("INSERT INTO t (id, v) VALUES ($1, 0)", [
          id,
        ]);
      }
      writer.engine.checkpoint();

      await reader.session.execute("BEGIN");
      const snapshot = await rows(reader.session);
      for (let id = 101; id <= 300; id++) {
        await writer.session.execute("INSERT INTO t (id, v) VALUES ($1, 0)", [
          id,
        ]);
      }
      writer.engine.checkpoint();

      expect(await rows(reader.session)).toEqual(snapshot);
      await reader.session.execute("COMMIT");
    },
  );
});

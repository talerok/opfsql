import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import { MemoryFileHandle } from "../../store/wal/file-handle.js";
import { WalStorage } from "../../store/wal/wal-storage.js";
import { Engine } from "../engine.js";
import type { Session } from "../session.js";

// ---------------------------------------------------------------------------
// Storage-efficiency budgets: today's measured costs, enforced as ceilings so
// they can only improve. A change that makes one cheaper lowers it here.
// Measured on the production stack (WAL over the page file, default 32 KB
// pages); the data is deterministic, so the numbers are exact.
// ---------------------------------------------------------------------------

const BUDGET = {
  /** Pages one single-row INSERT writes, by table shape. */
  pagesPerInsert: { autoincrement: 2, integerKey: 1, noKey: 1 },
  /** File bytes per row for rows of ~60 bytes. */
  fileBytesPerSmallRow: 92,
  /** File size after "insert N, delete all, insert N" over after "insert N". */
  growthAfterDeleteAndRefill: 1.65,
};

const ROWS = 5000;

let seq = 0;

beforeEach(() => {
  resetMockOPFS();
});

async function openDatabase() {
  const name = `efficiency-${seq++}`;
  const wal = new WalStorage(new OPFSSyncStorage(name), new MemoryFileHandle());
  const engine = await Engine.create(wal);
  return { name, wal, engine, session: engine.createSession() };
}

async function fileSize(name: string): Promise<number> {
  const root = await navigator.storage.getDirectory();
  const file = await (await root.getFileHandle(`${name}.opfsql`)).getFile();
  return file.size;
}

async function insertSmallRows(session: Session, from: number, count: number) {
  const insert = session.prepare("INSERT INTO t (id, body) VALUES ($1, $2)");
  await session.execute("BEGIN");
  for (let id = from; id < from + count; id++) {
    await insert.run([id, "x".repeat(50)]);
  }
  await session.execute("COMMIT");
}

describe("storage efficiency budgets", () => {
  it.each([
    {
      shape: "autoincrement",
      columns: "id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT",
      inserts: ["(body) VALUES ('a')", "(body) VALUES ('b')"],
    },
    {
      shape: "integerKey",
      columns: "id INTEGER PRIMARY KEY, body TEXT",
      inserts: ["(id, body) VALUES (1, 'a')", "(id, body) VALUES (2, 'b')"],
    },
    {
      shape: "noKey",
      columns: "body TEXT",
      inserts: ["(body) VALUES ('a')", "(body) VALUES ('b')"],
    },
  ] as const)(
    "a single-row INSERT into a table with $shape stays within its page budget",
    async ({ shape, columns, inserts: [first, measured] }) => {
      const { wal, session } = await openDatabase();
      await session.execute(`CREATE TABLE t (${columns})`);
      await session.execute(`INSERT INTO t ${first}`);

      const commit = vi.spyOn(wal, "commit");
      await session.execute(`INSERT INTO t ${measured}`);
      const pages = commit.mock.calls[0][0].size;
      expect(pages).toBeLessThanOrEqual(BUDGET.pagesPerInsert[shape]);
    },
  );

  it("small rows stay within the file-size budget", async () => {
    const { name, engine, session } = await openDatabase();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    await insertSmallRows(session, 0, ROWS);
    await engine.checkpoint();
    session.close();
    engine.close();

    const bytesPerRow = Math.ceil((await fileSize(name)) / ROWS);
    expect(bytesPerRow).toBeLessThanOrEqual(BUDGET.fileBytesPerSmallRow);
  });

  it("refilling a table after DELETE stays within the growth budget", async () => {
    const { name, engine, session } = await openDatabase();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    await insertSmallRows(session, 0, ROWS);
    await engine.checkpoint();
    const filled = await fileSize(name);

    await session.execute("DELETE FROM t");
    await insertSmallRows(session, ROWS, ROWS);
    await engine.checkpoint();
    const growth = (await fileSize(name)) / filled;
    expect(growth).toBeLessThanOrEqual(BUDGET.growthAfterDeleteAndRefill);
  });
});

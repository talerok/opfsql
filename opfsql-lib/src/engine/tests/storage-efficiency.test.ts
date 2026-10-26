import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryFile } from "../../storage/file/sync-file.js";
import { Pager } from "../../storage/pager/pager.js";
import { createEngine } from "../engine.js";
import type { Session } from "../session.js";

// ---------------------------------------------------------------------------
// Storage-efficiency budgets: today's measured costs, enforced as ceilings so
// they can only improve. A change that makes one cheaper lowers it here.
// Measured with the default page size; the data is deterministic, so the
// numbers are exact.
// ---------------------------------------------------------------------------

const BUDGET = {
  /** Pages one single-row INSERT writes, by table shape. */
  pagesPerInsert: { autoincrement: 2, integerKey: 1, noKey: 1 },
  /** File bytes per row for rows of ~60 bytes. */
  fileBytesPerSmallRow: 68,
  /** File size after "insert N, delete all, insert N" over after "insert N". */
  growthAfterDeleteAndRefill: 1,
};

const ROWS = 5000;

afterEach(() => {
  vi.restoreAllMocks();
});

async function openDatabase() {
  const database = new MemoryFile();
  const files = { database, wal: new MemoryFile(), marks: null };
  const engine = await createEngine(files);
  return { database, engine, session: engine.connect() };
}

/** Pages the commit `work` makes writes to the WAL. */
async function pagesWrittenBy(work: () => Promise<unknown>): Promise<number> {
  const append = Reflect.get<Pager, "append">(Pager.prototype, "append");
  let written = 0;
  vi.spyOn(Pager.prototype, "append").mockImplementation(function counted(
    this: Pager,
    pages,
    pageCount,
  ) {
    written += pages.size;
    return append.call(this, pages, pageCount);
  });
  await work();
  return written;
}

/** ROWS rows of about 60 bytes, ids from `from` on, in one transaction. */
async function insertSmallRows(session: Session, from: number) {
  const insert = await session.prepare(
    "INSERT INTO t (id, body) VALUES ($1, $2)",
  );
  await session.execute("BEGIN");
  for (let id = from; id < from + ROWS; id++) {
    await insert.query([id, "x".repeat(50)]);
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
    async ({ shape, columns, inserts }) => {
      const [first, measured] = inserts;
      const { session } = await openDatabase();
      await session.execute(`CREATE TABLE t (${columns})`);
      await session.execute(`INSERT INTO t ${first}`);

      const pages = await pagesWrittenBy(async () =>
        session.execute(`INSERT INTO t ${measured}`),
      );
      expect(pages).toBeLessThanOrEqual(BUDGET.pagesPerInsert[shape]);
    },
  );

  it("small rows stay within the file-size budget", async () => {
    const { database, engine, session } = await openDatabase();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    await insertSmallRows(session, 0);
    await engine.checkpoint();
    await session.close();
    await engine.close();

    const bytesPerRow = Math.ceil(database.getSize() / ROWS);
    expect(bytesPerRow).toBeLessThanOrEqual(BUDGET.fileBytesPerSmallRow);
  });

  it("refilling a table after DELETE stays within the growth budget", async () => {
    const { database, engine, session } = await openDatabase();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    await insertSmallRows(session, 0);
    await engine.checkpoint();
    const filled = database.getSize();

    await session.execute("DELETE FROM t");
    await insertSmallRows(session, ROWS);
    await engine.checkpoint();
    const growth = database.getSize() / filled;
    expect(growth).toBeLessThanOrEqual(BUDGET.growthAfterDeleteAndRefill);
  });
});

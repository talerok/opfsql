import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { AbortError, BusyError } from "../../errors.js";
import { openTabs } from "./tabs.js";
import {
  objectsOf,
  rowsOf,
  tableNamesOf,
} from "../../../test/support/results.js";

// What reviewers reproduced with several tabs on one database: a writer that
// pinned its schema before waiting for the lock, a lock that waited forever,
// a failed script that kept the lock. Each tab here is an engine of its own,
// as a tab's worker is.

let seq = 0;

beforeEach(() => {
  resetMockOPFS();
});

const tick = async () => new Promise((resolve) => setTimeout(resolve, 20));

describe("schema changes across tabs", () => {
  it("a CREATE TABLE that waited for the lock keeps the other tab's table", async () => {
    const [a, b] = await openTabs(`tables-${String(seq++)}`, 2);
    const tabA = a.engine.connect();
    const tabB = b.engine.connect();

    await tabB.execute("BEGIN");
    await tabB.execute("CREATE TABLE b_tbl (id INTEGER PRIMARY KEY)");
    const waiting = tabA.execute("CREATE TABLE a_tbl (id INTEGER PRIMARY KEY)");
    await tick();
    await tabB.execute("COMMIT");
    await waiting;

    for (const tab of [a, b]) {
      const names = await tableNamesOf(tab.engine.connect().schema());
      expect(names.toSorted((x, y) => x.localeCompare(y))).toEqual([
        "a_tbl",
        "b_tbl",
      ]);
      expect(await tab.engine.checkIntegrity()).toEqual([]);
    }
  });

  it("a CREATE INDEX that waited for the lock indexes the rows committed meanwhile", async () => {
    const [a, b] = await openTabs(`index-${String(seq++)}`, 2);
    const tabA = a.engine.connect();
    const tabB = b.engine.connect();
    await tabA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    await tabA.execute("INSERT INTO t VALUES (1, 'one')");

    await tabB.execute("BEGIN");
    await tabB.execute("INSERT INTO t VALUES (2, 'two')");
    const indexing = tabA.execute("CREATE INDEX t_v ON t (v)");
    await tick();
    await tabB.execute("COMMIT");
    await indexing;

    expect(await a.engine.checkIntegrity()).toEqual([]);
    const result = await tabB.query("SELECT id FROM t WHERE v = 'two'");
    expect(result.rows).toEqual([[2]]);
  });

  it("a transaction keeps reading its snapshot's schema while another tab changes it", async () => {
    const [a, b] = await openTabs(`snapshot-schema-${String(seq++)}`, 2);
    const reader = a.engine.connect();
    const writer = b.engine.connect();
    await writer.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await writer.execute("INSERT INTO t VALUES (1)");

    await reader.execute("BEGIN");
    await writer.execute(
      "CREATE TABLE u (id INTEGER PRIMARY KEY); INSERT INTO u VALUES (1); DROP TABLE t",
    );
    expect(await tableNamesOf(reader.schema())).toEqual(["t"]);
    const result = await reader.query("SELECT id FROM t");
    expect(result.rows).toEqual([[1]]);
    await expect(reader.execute("SELECT id FROM u")).rejects.toMatchObject({
      code: "SEMANTIC_ERROR",
    });
    await reader.execute("ROLLBACK");

    expect(await tableNamesOf(reader.schema())).toEqual(["u"]);
  });

  it("a deferred transaction that read the old schema cannot write on it", async () => {
    const [a, b] = await openTabs(`stale-schema-${String(seq++)}`, 2);
    const late = a.engine.connect();
    const other = b.engine.connect();
    await other.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await late.execute("BEGIN");
    await late.execute("SELECT * FROM t");
    await other.execute("CREATE TABLE u (id INTEGER PRIMARY KEY)");
    await expect(
      late.execute("CREATE TABLE v (id INTEGER PRIMARY KEY)"),
    ).rejects.toMatchObject({ code: "SERIALIZATION_FAILURE" });
    await late.execute("ROLLBACK");

    await late.execute("CREATE TABLE v (id INTEGER PRIMARY KEY)");
    const names = await tableNamesOf(other.schema());
    expect(names).toEqual(["t", "u", "v"]);
  });

  it("prepared statements survive DDL in another tab", async () => {
    const [a, b] = await openTabs(`prepared-${String(seq++)}`, 2);
    const tabA = a.engine.connect();
    const tabB = b.engine.connect();
    await tabA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await tabA.execute("INSERT INTO t VALUES (1)");
    const select = await tabA.prepare("SELECT * FROM t");

    await tabB.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)");
    expect(await rowsOf(select.query())).toEqual([[1]]);
    await tabB.execute("ALTER TABLE t ADD COLUMN v TEXT DEFAULT 'x'");
    expect(await objectsOf(select.query())).toEqual([{ id: 1, v: "x" }]);
  });
});

describe("waiting for the lock across tabs", () => {
  it("waits as long as the busy timeout, then fails with BUSY", async () => {
    const [a, b] = await openTabs(`busy-${String(seq++)}`, 2, {
      busyTimeout: 30,
    });
    const holder = a.engine.connect();
    const waiter = b.engine.connect();
    await holder.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await holder.execute("BEGIN IMMEDIATE");

    const started = performance.now();
    await expect(
      waiter.execute("INSERT INTO t VALUES (1)"),
    ).rejects.toBeInstanceOf(BusyError);
    expect(performance.now() - started).toBeGreaterThanOrEqual(25);

    await holder.execute("COMMIT");
    await waiter.execute("INSERT INTO t VALUES (1)");
  });

  it("stops waiting when the call's signal aborts", async () => {
    const [a, b] = await openTabs(`abort-${String(seq++)}`, 2, {
      busyTimeout: Infinity,
    });
    const holder = a.engine.connect();
    const waiter = b.engine.connect();
    await holder.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await holder.execute("BEGIN IMMEDIATE");

    const controller = new AbortController();
    const waiting = expect(
      waiter.query("INSERT INTO t VALUES (1)", [], {
        signal: controller.signal,
      }),
    ).rejects.toBeInstanceOf(AbortError);
    await tick();
    controller.abort();
    await waiting;
    await holder.execute("ROLLBACK");
  });

  it("a failed script leaves no transaction and no lock behind", async () => {
    const [a, b] = await openTabs(`failed-script-${String(seq++)}`, 2, {
      busyTimeout: 0,
    });
    const tabA = a.engine.connect();
    const tabB = b.engine.connect();
    await tabA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await tabA.execute("INSERT INTO t VALUES (1)");

    await expect(
      tabA.execute(
        "BEGIN; INSERT INTO t VALUES (2); INSERT INTO t VALUES (1); COMMIT",
      ),
    ).rejects.toMatchObject({
      code: "CONSTRAINT_VIOLATION",
      statementIndex: 2,
    });
    expect(tabA.inTransaction).toBe(false);

    // The other tab gets the lock at once.
    await tabB.execute("INSERT INTO t VALUES (3)");
    const result = await tabA.query("SELECT id FROM t ORDER BY id");
    expect(result.rows).toEqual([[1], [3]]);
  });

  it("a failed statement keeps its transaction's lock until ROLLBACK", async () => {
    const [a, b] = await openTabs(`failed-statement-${String(seq++)}`, 2, {
      busyTimeout: 0,
    });
    const tabA = a.engine.connect();
    const tabB = b.engine.connect();
    await tabA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await tabA.execute("BEGIN");
    await tabA.execute("INSERT INTO t VALUES (1)");
    await expect(tabA.execute("INSERT INTO t VALUES (1)")).rejects.toThrow();
    expect(tabA.inTransaction).toBe(true);

    await expect(
      tabB.execute("INSERT INTO t VALUES (2)"),
    ).rejects.toMatchObject({ code: "BUSY" });
    await tabA.execute("ROLLBACK");
    await tabB.execute("INSERT INTO t VALUES (2)");
    const result = await tabA.query("SELECT id FROM t");
    expect(result.rows).toEqual([[2]]);
  });

  it("a closed engine releases its lock and rolls back its transaction", async () => {
    const [a, b] = await openTabs(`engine-close-${String(seq++)}`, 2, {
      busyTimeout: 0,
    });
    const tabA = a.engine.connect();
    const tabB = b.engine.connect();
    await tabA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await tabA.execute("BEGIN");
    await tabA.execute("INSERT INTO t VALUES (1)");

    await a.engine.close();
    await tabB.execute("INSERT INTO t VALUES (2)");
    const result = await tabB.query("SELECT id FROM t");
    expect(result.rows).toEqual([[2]]);
    await expect(tabA.execute("SELECT 1")).rejects.toMatchObject({
      code: "MISUSE",
    });
  });
});

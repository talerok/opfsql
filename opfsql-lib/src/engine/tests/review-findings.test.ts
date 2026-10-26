import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  AbortError,
  DatatypeMismatchError,
  MisuseError,
  SemanticError,
} from "../../errors.js";
import { Pager } from "../../storage/pager/pager.js";
import { Engine } from "../engine.js";
import { openTab, sharedFiles } from "./tabs.js";
import { failureOf } from "../../../test/support/failure.js";
import {
  objectsOf,
  rowsOf,
  tableNamesOf,
} from "../../../test/support/results.js";

// Regression tests for what the review of the engine rebuild found (F1–F10);
// each failed before its fix.

let seq = 0;

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  resetMockOPFS();
});

const tick = async () => new Promise((resolve) => setTimeout(resolve, 20));

describe("lock and snapshot release", () => {
  it("F1: a failure releasing the snapshot does not keep the write lock", async () => {
    const name = `leak-${String(seq++)}`;
    const files = sharedFiles();
    const marks = files.marks!;
    const a = await openTab(name, files, { busyTimeout: 300 });
    const b = await openTab(name, files, { busyTimeout: 300 });
    const tabA = a.connect();
    const tabB = b.connect();
    await tabA.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const write = marks.write.bind(marks);
    let isFailing = false;
    // The commit is written; publishing that the snapshot is released fails.
    vi.spyOn(marks, "write").mockImplementation((buffer, options) => {
      if (isFailing) {
        isFailing = false;
        throw new DOMException("shm write failed", "InvalidStateError");
      }
      return write(buffer, options);
    });
    await tabA.begin({ mode: "immediate" });
    await tabA.query("INSERT INTO t VALUES (1)");
    isFailing = true;
    expect(await failureOf(tabA.commit())).toMatchObject({ code: "IO_ERROR" });
    expect(tabA.inTransaction).toBe(false);

    // The lease went with the failure: tab B writes without waiting.
    await tabB.query("INSERT INTO t VALUES (2)");
    expect(await rowsOf(tabB.query("SELECT id FROM t ORDER BY id"))).toEqual([
      [1],
      [2],
    ]);
  });

  it("F2: pinning does not spin on a read-mark file that outlived the database", async () => {
    const name = `spin-${String(seq++)}`;
    let engine = await Engine.open(name);
    const session = engine.connect();
    await session.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    for (let index = 0; index < 5; index++) {
      await session.query("INSERT INTO t VALUES ($1)", [index]);
    }
    await engine.checkpoint(); // leaves a checkpoint intent in the -shm file
    await engine.close();
    const root = await navigator.storage.getDirectory();
    await root.removeEntry(`${name}.opfsql`);
    await root.removeEntry(`${name}.opfsql-wal`);

    const beginRead = Reflect.get<Pager, "beginRead">(
      Pager.prototype,
      "beginRead",
    );
    let calls = 0;
    vi.spyOn(Pager.prototype, "beginRead").mockImplementation(function guarded(
      this: Pager,
    ) {
      // A regression would hang the worker in a synchronous loop.
      if (++calls > 10_000) throw new Error("beginRead spun 10000 times");
      return beginRead.call(this);
    });
    engine = await Engine.open(name);
    expect(await rowsOf(engine.connect().query("SELECT 1"))).toEqual([[1]]);
    expect(calls).toBeLessThan(10);
    await engine.close();
  });
});

describe("errors", () => {
  it("F3: one call's statementIndex does not appear on another call's error", async () => {
    const engine = await Engine.open(`abort-${String(seq++)}`, {
      busyTimeout: 10_000,
    });
    await engine.connect().query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = engine.connect();
    await holder.begin({ mode: "immediate" });
    const session = engine.connect();
    const script = failureOf(
      session.execute("SELECT 1; BEGIN; INSERT INTO t VALUES (1); COMMIT"),
    );
    const query = failureOf(session.query("SELECT 2"));
    await tick();
    await session.close();
    const [scriptError, queryError] = await Promise.all([script, query]);
    expect(scriptError).toMatchObject({ code: "ABORTED", statementIndex: 2 });
    expect(queryError).toBeInstanceOf(AbortError);
    expect(queryError).not.toHaveProperty("statementIndex");
    await engine.close();
  });

  it("F3: a caller's own abort reason is never changed", async () => {
    const engine = await Engine.open(`reason-${String(seq++)}`, {
      busyTimeout: 10_000,
    });
    await engine.connect().query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = engine.connect();
    await holder.begin({ mode: "immediate" });
    const reason = new MisuseError("the page went away");
    const controller = new AbortController();
    const script = failureOf(
      engine.connect().execute("SELECT 1; INSERT INTO t VALUES (1)", {
        signal: controller.signal,
      }),
    );
    await tick();
    controller.abort(reason);
    const error = await script;
    expect(error).toBeInstanceOf(AbortError);
    expect(error).toHaveProperty("cause", reason);
    expect(reason).not.toHaveProperty("statementIndex");
    await engine.close();
  });

  it("F4: codes do not depend on table names", async () => {
    const engine = await Engine.open(`codes-${String(seq++)}`);
    const error = await failureOf(
      engine.connect().query("DROP TABLE parameters"),
    );
    expect(error).toMatchObject({ code: "SEMANTIC_ERROR" });
    await engine.close();
  });

  it("F5: a non-integer INTEGER PRIMARY KEY is DATATYPE_MISMATCH, not INTERNAL", async () => {
    const engine = await Engine.open(`rowid-${String(seq++)}`);
    const session = engine.connect();
    await session.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    for (const sql of [
      "INSERT INTO t VALUES (1.5)",
      "INSERT INTO t VALUES ('z')",
    ]) {
      expect(await failureOf(session.query(sql))).toBeInstanceOf(
        DatatypeMismatchError,
      );
    }
    await engine.close();
  });

  it("F6: DATATYPE_MISMATCH is a DatatypeMismatchError, as api.md's table says", async () => {
    const engine = await Engine.open(`cast-${String(seq++)}`);
    const error = await failureOf(
      engine.connect().query("SELECT CAST('abc' AS INTEGER)"),
    );
    expect(error).toMatchObject({ code: "DATATYPE_MISMATCH" });
    expect(error).toBeInstanceOf(DatatypeMismatchError);
    await engine.close();
  });

  it("F7: a statement that cannot bind fails as such, without waiting for the lock", async () => {
    const engine = await Engine.open(`bind-${String(seq++)}`, {
      busyTimeout: 300,
    });
    const holder = engine.connect();
    const session = engine.connect();
    await session.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await holder.begin({ mode: "immediate" });
    const started = performance.now();

    const unknown = await failureOf(
      session.query("INSERT INTO nosuch VALUES (1)"),
    );
    expect(unknown).toBeInstanceOf(SemanticError);
    const parameters = await failureOf(
      session.query("INSERT INTO t VALUES ($1)"),
    );
    expect(parameters).toBeInstanceOf(MisuseError);
    const script = await failureOf(
      session.execute(
        "INSERT INTO t VALUES (1); INSERT INTO nosuch VALUES (2)",
      ),
    );
    expect(script).toMatchObject({ code: "SEMANTIC_ERROR", statementIndex: 1 });
    expect(performance.now() - started).toBeLessThan(200);
    await holder.rollback();
    await engine.close();
  });

  it("F7: in a deferred transaction, a bad statement is a binding error, not a serialization failure", async () => {
    const engine = await Engine.open(`deferred-${String(seq++)}`);
    const session = engine.connect();
    const other = engine.connect();
    await session.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.begin();
    await session.query("SELECT * FROM t");
    await other.query("INSERT INTO t VALUES (1)");
    const error = await failureOf(
      session.query("INSERT INTO nosuch VALUES (1)"),
    );
    expect(error).toBeInstanceOf(SemanticError);
    await session.rollback();
    await engine.close();
  });
});

describe("checkIntegrity", () => {
  it("F8: an open write transaction's pages are not counted, before or after it commits", async () => {
    const engine = await Engine.open(`ci-${String(seq++)}`, {
      pageSize: 4096,
    });
    const session = engine.connect();
    await session.query("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    await session.begin({ mode: "immediate" });
    for (let index = 0; index < 200; index++) {
      await session.query("INSERT INTO t (v) VALUES ($1)", ["x".repeat(100)]);
    }
    expect(await engine.checkIntegrity()).toEqual([]);
    await session.commit();
    expect(await engine.checkIntegrity()).toEqual([]);
    await engine.close();
  });
});

describe("results and schema()", () => {
  it("F9: objects() keeps a column named __proto__", async () => {
    const engine = await Engine.open(`proto-${String(seq++)}`);
    const [row] = await objectsOf(
      engine.connect().query('SELECT 1 AS "__proto__", 2 AS b'),
    );
    expect(Object.keys(row)).toEqual(["__proto__", "b"]);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    await engine.close();
  });

  it("F10: ColumnInfo.type is the column's normalized type", async () => {
    const engine = await Engine.open(`types-${String(seq++)}`);
    const session = engine.connect();
    await session.query(
      "CREATE TABLE x (a VARCHAR(20), b INT, c BIGINT, d DOUBLE, e BOOL)",
    );
    expect(await tableNamesOf(session.schema())).toEqual(["x"]);
    const schema = await session.schema();
    expect(schema.tables[0]?.columns.map((column) => column.type)).toEqual([
      "TEXT",
      "INTEGER",
      "INTEGER",
      "REAL",
      "BOOLEAN",
    ]);
    await engine.close();
  });
});

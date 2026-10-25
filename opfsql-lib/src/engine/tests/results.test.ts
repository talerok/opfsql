import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  MisuseError,
  OpfsqlError,
  SemanticError,
  SqlSyntaxError,
} from "../../errors.js";
import { Result } from "../api/index.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { failureOf } from "../../../test/support/failure.js";
import { openTestEngine } from "./test-engine.js";
import { lastRowidOf } from "../../../test/support/results.js";

// What statements return, and how a script of several runs.

let seq = 0;
let engine: Engine;
let session: Session;

beforeEach(async () => {
  resetMockOPFS();
  engine = await openTestEngine(`results-${String(seq++)}`);
  session = engine.connect();
  await session.execute(`
    CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
    CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER);
    CREATE TABLE seq (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
    CREATE TABLE plain (v TEXT);
  `);
});

afterEach(async () => {
  await session.close();
  await engine.close();
});

async function ids(table: string): Promise<unknown[]> {
  const result = await session.query(`SELECT id FROM ${table} ORDER BY id`);
  return result.rows.map(([id]) => id);
}

describe("result rows", () => {
  it("keep every column, also when names repeat", async () => {
    await session.execute(
      "INSERT INTO t VALUES (1, 'a'); INSERT INTO u VALUES (10, 1)",
    );
    const joined = await session.query(
      "SELECT t.id, u.id FROM t JOIN u ON u.t_id = t.id",
    );
    expect(joined.columns).toEqual(["id", "id"]);
    expect(joined.rows).toEqual([[1, 10]]);

    const constants = await session.query("SELECT 1 a, 2 a");
    expect(constants.columns).toEqual(["a", "a"]);
    expect(constants.rows).toEqual([[1, 2]]);
  });

  it("offer objects when the names are distinct, and refuse when they are not", async () => {
    await session.query("INSERT INTO t VALUES (1, 'a')");
    const result = await session.query("SELECT id, v AS value FROM t");
    expect(result.objects()).toEqual([{ id: 1, value: "a" }]);
    expect(() =>
      new Result({
        columns: ["id", "id"],
        rows: [],
        changes: 0,
        lastInsertRowid: null,
      }).objects(),
    ).toThrow(MisuseError);
  });

  it("name their columns when there are none", async () => {
    const result = await session.query("SELECT id, v FROM t WHERE id < 0");
    expect(result.columns).toEqual(["id", "v"]);
    expect(result.rows).toEqual([]);
    const star = await session.query("SELECT * FROM u");
    expect(star.columns).toEqual(["id", "t_id"]);
  });
});

describe("changes and lastInsertRowid", () => {
  it("count the rows a write touched", async () => {
    const insert = await session.query(
      "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    expect(insert).toMatchObject({ changes: 3, lastInsertRowid: 3 });
    const update = await session.query("UPDATE t SET v = 'x' WHERE id > 1");
    expect(update).toMatchObject({ changes: 2, lastInsertRowid: null });
    const remove = await session.query("DELETE FROM t WHERE id = 1");
    expect(remove).toMatchObject({ changes: 1, lastInsertRowid: null });
    const select = await session.query("SELECT * FROM t");
    expect(select).toMatchObject({ changes: 0, lastInsertRowid: null });
    const ddl = await session.query("CREATE TABLE w (id INTEGER PRIMARY KEY)");
    expect(ddl).toMatchObject({ columns: [], changes: 0 });
  });

  it("give the rowid of the last inserted row, assigned or explicit", async () => {
    expect(
      await lastRowidOf(
        session.query("INSERT INTO seq (v) VALUES ('a'), ('b')"),
      ),
    ).toBe(2);
    expect(
      await lastRowidOf(
        session.query("INSERT INTO t (id, v) VALUES (42, 'x')"),
      ),
    ).toBe(42);
    expect(
      await lastRowidOf(session.query("INSERT INTO plain (v) VALUES ('p')")),
    ).toEqual(expect.any(Number));
    expect(
      await lastRowidOf(
        session.query("INSERT INTO t (id, v) SELECT id + 100, v FROM seq"),
      ),
    ).toBe(102);
  });

  it("are per statement in a script", async () => {
    const results = await session.execute(
      "INSERT INTO t VALUES (1, 'a'); UPDATE t SET v = 'b'; SELECT v FROM t",
    );
    expect(results.map(({ changes }) => changes)).toEqual([1, 1, 0]);
    expect(results.map(({ lastInsertRowid }) => lastInsertRowid)).toEqual([
      1,
      null,
      null,
    ]);
    expect(results[2].rows).toEqual([["b"]]);
  });
});

describe("scripts", () => {
  it("run as one transaction: a failure undoes the statements before it", async () => {
    const failure = session.execute(
      "INSERT INTO t VALUES (1, 'a'); INSERT INTO u VALUES (1, 1); INSERT INTO nope VALUES (1)",
    );
    await expect(failure).rejects.toBeInstanceOf(SemanticError);
    await expect(failure).rejects.toMatchObject({ statementIndex: 2 });
    expect(await ids("t")).toEqual([]);
    expect(await ids("u")).toEqual([]);
  });

  it("run nothing when any statement does not parse", async () => {
    const failure = session.execute(
      "INSERT INTO t VALUES (1, 'a'); INSERT INTO t VALUES (2,",
    );
    await expect(failure).rejects.toBeInstanceOf(SqlSyntaxError);
    expect(await ids("t")).toEqual([]);
  });

  it("that control transactions commit outside them statement by statement", async () => {
    const failure = session.execute(`
      INSERT INTO t VALUES (1, 'kept');
      BEGIN;
      INSERT INTO t VALUES (2, 'undone');
      INSERT INTO t VALUES (1, 'duplicate');
      COMMIT
    `);
    await expect(failure).rejects.toMatchObject({
      code: "CONSTRAINT_VIOLATION",
      statementIndex: 3,
    });
    expect(session.inTransaction).toBe(false);
    expect(await ids("t")).toEqual([1]);
  });

  it("inside an earlier call's transaction, abort it on failure", async () => {
    await session.query("BEGIN");
    await session.query("INSERT INTO t VALUES (1, 'a')");
    await expect(
      session.execute(
        "INSERT INTO t VALUES (2, 'b'); INSERT INTO t VALUES (1, 'c')",
      ),
    ).rejects.toMatchObject({ statementIndex: 1 });
    expect(session.inTransaction).toBe(true);
    await expect(session.query("SELECT 1")).rejects.toMatchObject({
      code: "TRANSACTION_ABORTED",
    });
    await session.query("ROLLBACK");
    expect(await ids("t")).toEqual([]);
  });

  it("take no parameters; query takes one statement", async () => {
    await expect(session.execute("SELECT $1")).rejects.toBeInstanceOf(
      OpfsqlError,
    );
    await expect(session.query("SELECT 1; SELECT 2")).rejects.toBeInstanceOf(
      MisuseError,
    );
    await expect(session.query("")).rejects.toBeInstanceOf(MisuseError);
    expect(await session.execute("")).toEqual([]);
  });
});

describe("errors", () => {
  it("point at the SQL that does not parse", async () => {
    const sql = "SELECT id\nFROM t WHERE id = = 1";
    const error = await failureOf(session.query(sql));
    expect(error).toBeInstanceOf(SqlSyntaxError);
    const { offset, length } = error as SqlSyntaxError;
    expect(sql.slice(offset, offset + length)).toBe("=");
    expect(offset).toBe(sql.lastIndexOf("= 1"));
  });

  it("are OpfsqlErrors with codes, whatever layer threw them", async () => {
    await session.query("INSERT INTO t VALUES (1, 'a')");
    const failures = await Promise.all(
      [
        "SELECT * FROM nope",
        "INSERT INTO t VALUES (1, 'b')",
        "SELECT CAST('x' AS INTEGER)",
        "SELECT * FROM t WHERE id = $1",
        "SELEC",
      ].map(async (sql) => failureOf(session.query(sql))),
    );
    for (const failure of failures) expect(failure).toBeInstanceOf(OpfsqlError);
    expect(failures.map((failure) => (failure as OpfsqlError).code)).toEqual([
      "SEMANTIC_ERROR",
      "CONSTRAINT_VIOLATION",
      "DATATYPE_MISMATCH",
      "MISUSE",
      "SYNTAX_ERROR",
    ]);
  });
});

describe("EXPLAIN", () => {
  it("returns the plan in one column", async () => {
    const result = await session.query("EXPLAIN SELECT * FROM t WHERE id = 1");
    expect(result.columns).toEqual(["plan"]);
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0][0]).toEqual(expect.any(String));
  });
});

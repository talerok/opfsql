import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Engine, type Session } from "../../src/worker-entry.js";
import { parseRecords } from "./records.js";
import { formatValue, hashOf, mismatchOf, resultValues } from "./results.js";
import { runRecords } from "./run.js";

// The sqllogictest runner itself, on small files written here: what it
// parses, how it prints values, and what it counts as a failure.

describe("parsing", () => {
  it("reads statements, queries and directives, each at its line", () => {
    const records = parseRecords(
      [
        "hash-threshold 8",
        "",
        "# a comment",
        "statement ok",
        "CREATE TABLE t1(a INTEGER)",
        "",
        "statement error",
        "SELECT nope",
        "",
        "query IT rowsort label-1",
        "SELECT a, 'x'",
        "FROM t1",
        "----",
        "1",
        "x",
        "",
        "query I nosort",
        "SELECT a FROM t1",
        "----",
        "30 values hashing to 0123456789abcdef0123456789abcdef",
      ].join("\n"),
    );
    expect(records).toEqual([
      { kind: "hash-threshold", line: 1, threshold: 8 },
      {
        kind: "statement",
        line: 4,
        sql: "CREATE TABLE t1(a INTEGER)",
        isErrorExpected: false,
      },
      { kind: "statement", line: 7, sql: "SELECT nope", isErrorExpected: true },
      {
        kind: "query",
        line: 10,
        sql: "SELECT a, 'x'\nFROM t1",
        types: "IT",
        sort: "rowsort",
        expected: { kind: "values", values: ["1", "x"] },
      },
      {
        kind: "query",
        line: 17,
        sql: "SELECT a FROM t1",
        types: "I",
        sort: "nosort",
        expected: {
          kind: "hash",
          count: 30,
          hash: "0123456789abcdef0123456789abcdef",
        },
      },
    ]);
  });

  it("keeps what SQLite runs: skipif another engine, onlyif sqlite", () => {
    const records = parseRecords(
      [
        "skipif sqlite",
        "statement ok",
        "SELECT 1",
        "",
        "onlyif mysql",
        "statement ok",
        "SELECT 2",
        "",
        "skipif mysql",
        "skipif postgresql",
        "statement ok",
        "SELECT 3",
        "",
        "onlyif sqlite",
        "statement ok",
        "SELECT 4",
      ].join("\n"),
    );
    expect(records.map((record) => "sql" in record && record.sql)).toEqual([
      "SELECT 3",
      "SELECT 4",
    ]);
    expect(records.map((record) => record.line)).toEqual([11, 15]);
  });

  it("stops at a halt SQLite runs, not at another engine's", () => {
    const records = parseRecords(
      [
        "onlyif mssql",
        "halt",
        "",
        "statement ok",
        "SELECT 1",
        "",
        "halt",
        "",
        "statement ok",
        "SELECT 2",
      ].join("\n"),
    );
    expect(records.map((record) => record.kind)).toEqual(["statement", "halt"]);
  });
});

describe("values", () => {
  it("print by the column's letter, as sqllogictest's SQLite client does", () => {
    expect(formatValue(7.9, "I")).toBe("7");
    expect(formatValue(-7.9, "I")).toBe("-7");
    expect(formatValue("12abc", "I")).toBe("12");
    expect(formatValue("abc", "I")).toBe("0");
    expect(formatValue(2, "R")).toBe("2.000");
    expect(formatValue(1 / 3, "R")).toBe("0.333");
    expect(formatValue("x\ty", "T")).toBe("x@y");
    expect(formatValue("é", "T")).toBe("@@");
    expect(formatValue("", "T")).toBe("(empty)");
    expect(formatValue(null, "I")).toBe("NULL");
    expect(formatValue(true, "I")).toBe("1");
    expect(formatValue(false, "T")).toBe("0");
  });

  it("sort as the record asks: rows, or every value alone", () => {
    const rows = [
      [2, "b"],
      [10, "a"],
      [2, "a"],
    ];
    expect(resultValues(rows, "IT", "nosort")).toEqual([
      "2",
      "b",
      "10",
      "a",
      "2",
      "a",
    ]);
    // By text, as strcmp orders them: "10" before "2".
    expect(resultValues(rows, "IT", "rowsort")).toEqual([
      "10",
      "a",
      "2",
      "a",
      "2",
      "b",
    ]);
    expect(resultValues(rows, "IT", "valuesort")).toEqual([
      "10",
      "2",
      "2",
      "a",
      "a",
      "b",
    ]);
  });

  it("hash as sqllogictest does: each value and a newline, MD5", () => {
    // printf '1\n2\n' | md5sum
    expect(hashOf(["1", "2"])).toBe("6ddb4095eb719e2a9f0a3f95677d24e0");
    const hashed = {
      kind: "hash",
      count: 2,
      hash: hashOf(["1", "2"]),
    } as const;
    expect(mismatchOf(["1", "2"], hashed)).toBeNull();
    expect(mismatchOf(["1", "3"], hashed)).toMatch(/^expected 2 values/u);
  });
});

describe("running", () => {
  let engine: Engine;
  let session: Session;

  beforeEach(async () => {
    resetMockOPFS();
    engine = await Engine.open("runner");
    session = engine.connect();
  });

  afterEach(async () => {
    await session.close();
    await engine.close();
  });

  it("fails a record that does not do what the file says, and goes on", async () => {
    const records = parseRecords(
      [
        "statement ok",
        "CREATE TABLE t1(a INTEGER)",
        "",
        "statement ok",
        "INSERT INTO t1 VALUES(1),(2)",
        "",
        "statement error",
        "INSERT INTO nowhere VALUES(1)",
        "",
        "statement error",
        "SELECT 1",
        "",
        "query I rowsort",
        "SELECT a FROM t1",
        "----",
        "1",
        "3",
        "",
        "query I valuesort",
        "SELECT a FROM t1",
        "----",
        "1",
        "2",
      ].join("\n"),
    );
    const outcomes = await runRecords(session, records);
    expect(outcomes.map(({ line, failure }) => [line, failure])).toEqual([
      [1, null],
      [4, null],
      [7, null],
      [10, "expected an error"],
      [13, "expected [1, 3], got [1, 2]"],
      [19, null],
    ]);
  });
});

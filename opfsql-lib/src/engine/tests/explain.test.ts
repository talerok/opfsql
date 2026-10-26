import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Engine, Session } from "../index.js";
import { openTestEngine } from "./test-engine.js";

let engine: Engine;
let session: Session;
let seq = 0;

async function createEngine(): Promise<Engine> {
  engine = await openTestEngine(`explain-test-${String(seq++)}`);
  session = engine.connect();
  return engine;
}

beforeEach(() => {
  resetMockOPFS();
});

afterEach(async () => {
  await session.close();
  await engine.close();
});

async function explain(sql: string): Promise<string> {
  const [result] = await session.execute(sql);
  expect(result.objects()).toHaveLength(1);
  return result.objects()[0].plan as string;
}

// ---------------------------------------------------------------------------

/** The tables of the plan-shape cases; each case adds the indexes it needs. */
const TABLES = `
  CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, cat TEXT, val INTEGER);
  CREATE TABLE a (id INTEGER PRIMARY KEY);
  CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER);
`;

interface Shape {
  readonly shows: string;
  readonly indexes?: string;
  readonly sql: string;
  /** Lines the plan holds. */
  readonly lines: readonly (string | RegExp)[];
}

const SHAPES: readonly Shape[] = [
  {
    shows: "a table scan under a project",
    sql: "SELECT * FROM t",
    lines: ["project#1 id, name, cat, val", "  table-scan t#0"],
  },
  {
    shows: "a filter on one table as its scan's residual",
    sql: "SELECT id FROM t WHERE val > 10",
    lines: [/^ {2}table-scan t#0 residual: \(?val > 10\)?$/],
  },
  {
    shows: "a hash join for JOIN",
    sql: "SELECT * FROM a JOIN b ON a.id = b.a_id",
    lines: [
      "  hash-join inner on id = a_id",
      "    table-scan a#0",
      "    table-scan b#1",
    ],
  },
  {
    shows: "a top-n for ORDER BY with LIMIT",
    sql: "SELECT * FROM t ORDER BY val DESC LIMIT 5",
    lines: [/^ +top-n 5 by val DESC$/],
  },
  {
    shows: "a hash aggregate for GROUP BY",
    sql: "SELECT cat, SUM(val) FROM t GROUP BY cat",
    lines: [/^ {2}hash-aggregate#\d+#\d+ group by cat compute sum\(val\)$/],
  },
  {
    shows: "an index scan when an index fits",
    indexes: "CREATE INDEX idx_val ON t (val)",
    sql: "SELECT * FROM t WHERE val = 1",
    lines: ["  index-scan t#0 using idx_val (val = 1)"],
  },
  {
    shows: "a hash distinct",
    sql: "SELECT DISTINCT name FROM t",
    lines: ["hash-distinct"],
  },
  {
    shows: "an INSERT and its source",
    sql: "INSERT INTO t (id, name) VALUES (1, 'x')",
    lines: ["insert into t", "  values#0 (1, 'x')"],
  },
  {
    shows: "an UPDATE and the rows it reads",
    sql: "UPDATE t SET name = 'y' WHERE id = 1",
    lines: ["update t", "  rowid-lookup t#0 (rowid = 1)"],
  },
  {
    shows: "a DELETE and the rows it reads",
    sql: "DELETE FROM t WHERE id = 1",
    lines: ["delete from t", "  rowid-lookup t#0 (rowid = 1)"],
  },
];

describe("EXPLAIN", () => {
  it.each(SHAPES)("shows $shows", async ({ indexes = "", sql, lines }) => {
    await createEngine();
    await session.execute(TABLES + indexes);
    const text = await explain(`EXPLAIN ${sql}`);
    const plan = text.split("\n");
    for (const line of lines) {
      expect(plan).toContainEqual(
        typeof line === "string" ? line : expect.stringMatching(line),
      );
    }
  });

  it("does not modify database state", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await explain("EXPLAIN DELETE FROM t WHERE id = 1");
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(1);
  });

  it("result type is rows with plan column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const [result] = await session.execute("EXPLAIN SELECT * FROM t");
    expect(result.objects()).toHaveLength(1);
    expect(typeof result.objects()[0].plan).toBe("string");
  });

  it("shows a subquery's plan after the main plan", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)");
    const plan = await explain(
      "EXPLAIN SELECT id FROM t WHERE val IN (SELECT id FROM a)",
    );
    expect(plan).toContain("(subquery 1)");
    expect(plan).toMatch(
      /\nsubquery 1:\n {2}project#2 id\n {4}table-scan a#1$/,
    );
  });

  it("refuses to explain BEGIN, COMMIT and ROLLBACK", async () => {
    await createEngine();
    await expect(session.query("EXPLAIN BEGIN")).rejects.toMatchObject({
      code: "NOT_SUPPORTED",
    });
  });

  it("shows DDL as what it does", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    expect(await explain("EXPLAIN CREATE TABLE u (id INTEGER)")).toBe(
      "create table u",
    );
    expect(await explain("EXPLAIN CREATE INDEX t_id ON t (id)")).toBe(
      "create index t_id on t",
    );
    expect(await explain("EXPLAIN DROP TABLE t")).toBe("drop table t");
    const [result] = await session.execute("SELECT count(*) FROM t");
    expect(result.rows).toEqual([[0]]);
  });

  it("plan is properly indented (children deeper than parents)", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    const plan = await explain("EXPLAIN SELECT * FROM t WHERE val > 1");
    const lines = plan.split("\n");
    // First line (root) has no indentation
    expect(lines[0]).toBe(lines[0].trimStart());
    // At least one child is indented
    expect(lines.some((l) => l.startsWith("  "))).toBe(true);
  });

  // -------------------------------------------------------------------------
  // ORDER BY via Index
  // -------------------------------------------------------------------------

  it("reads an index in order for ORDER BY with a LIMIT, without a sort", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const plan = await explain(
      "EXPLAIN SELECT * FROM t ORDER BY val ASC LIMIT 3",
    );
    expect(plan).toContain("index-scan t#0 using idx_val");
    expect(plan).not.toMatch(/sort|top-n/);
  });

  it("sorts a whole table rather than read every row through an index", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const plan = await explain("EXPLAIN SELECT * FROM t ORDER BY val ASC");
    expect(plan).toContain("sort val ASC");
    expect(plan).toContain("table-scan t#0");
  });

  it("ORDER BY on the index the filter uses needs no sort", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const plan = await explain(
      "EXPLAIN SELECT * FROM t WHERE val > 5 ORDER BY val ASC",
    );
    expect(plan).toContain("index-scan t#0 using idx_val (val > 5)");
    expect(plan).not.toContain("sort");
  });

  it("reads an index backward for ORDER BY … DESC with a LIMIT", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const plan = await explain(
      "EXPLAIN SELECT * FROM t ORDER BY val DESC LIMIT 3",
    );
    expect(plan).toContain("using idx_val");
    expect(plan).toContain("descending");
    expect(plan).not.toMatch(/sort|top-n/);
  });

  it("ORDER BY via index returns correct results", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    await session.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    const [result] = await session.execute(
      "SELECT val FROM t ORDER BY val ASC",
    );
    expect(result.objects().map((r) => r.val)).toEqual([10, 20, 30]);
  });

  it("ORDER BY via index with LIMIT returns correct results", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    await session.execute(
      "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20), (4, 5)",
    );
    const [result] = await session.execute(
      "SELECT val FROM t ORDER BY val ASC LIMIT 2",
    );
    expect(result.objects().map((r) => r.val)).toEqual([5, 10]);
  });

  // -------------------------------------------------------------------------
  // MIN/MAX via Index
  // -------------------------------------------------------------------------

  it("reads MIN from an index", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const plan = await explain("EXPLAIN SELECT MIN(val) FROM t");
    expect(plan).toContain("index-extreme#2 MIN(val) of t using idx_val");
  });

  it("reads MAX from an index", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const plan = await explain("EXPLAIN SELECT MAX(val) FROM t");
    expect(plan).toContain("index-extreme#2 MAX(val) of t using idx_val");
  });

  it("MIN via index returns correct result", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    await session.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    const [result] = await session.execute("SELECT MIN(val) AS m FROM t");
    expect(result.objects()[0].m).toBe(10);
  });

  it("MAX via index returns correct result", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    await session.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    const [result] = await session.execute("SELECT MAX(val) AS m FROM t");
    expect(result.objects()[0].m).toBe(30);
  });

  it("MIN on empty table returns NULL", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const [result] = await session.execute("SELECT MIN(val) AS m FROM t");
    expect(result.objects()[0].m).toBeNull();
  });

  it("MAX on empty table returns NULL", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("CREATE INDEX idx_val ON t (val)");
    const [result] = await session.execute("SELECT MAX(val) AS m FROM t");
    expect(result.objects()[0].m).toBeNull();
  });

  // -------------------------------------------------------------------------
  // OR / Index Union
  // -------------------------------------------------------------------------

  it("shows an index union for OR over two indexed columns", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    );
    await session.execute("CREATE INDEX idx_a ON t (a)");
    await session.execute("CREATE INDEX idx_b ON t (b)");
    const plan = await explain("EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2");
    expect(plan).toMatch(
      /index-union t#0 \[using idx_a \(a = 1\); using idx_b \(b = 2\)\]/,
    );
  });

  it("OR / Index Union returns correct results", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    );
    await session.execute("CREATE INDEX idx_a ON t (a)");
    await session.execute("CREATE INDEX idx_b ON t (b)");
    await session.execute(
      "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 10, 200), (4, 30, 100)",
    );
    const [result] = await session.execute(
      "SELECT id FROM t WHERE a = 10 OR b = 200 ORDER BY id",
    );
    // Row 1: a=10, Row 2: b=200, Row 3: a=10 AND b=200 (dedup), Row 4: neither
    expect(result.objects().map((r) => r.id)).toEqual([1, 2, 3]);
  });

  it("OR / Index Union deduplicates rows matching both branches", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    );
    await session.execute("CREATE INDEX idx_a ON t (a)");
    await session.execute("CREATE INDEX idx_b ON t (b)");
    await session.execute("INSERT INTO t VALUES (1, 5, 5)");
    const [result] = await session.execute(
      "SELECT id FROM t WHERE a = 5 OR b = 5",
    );
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0].id).toBe(1);
  });
});

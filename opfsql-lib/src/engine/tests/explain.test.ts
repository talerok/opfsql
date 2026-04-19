import { afterEach, describe, expect, it } from "vitest";
import { MemoryPageStorage } from "../../store/backend/memory-storage.js";
import { Engine } from "../index.js";

let engine: Engine;

async function createEngine(): Promise<Engine> {
  engine = await Engine.create(new MemoryPageStorage());
  return engine;
}

afterEach(() => {
  engine?.close();
});

function explain(sql: string): string {
  const [result] = engine.execute(sql);
  expect(result.type).toBe("rows");
  expect(result.rows).toHaveLength(1);
  return result.rows![0].plan as string;
}

// ---------------------------------------------------------------------------

describe("EXPLAIN", () => {
  it("returns a plan for a simple SELECT", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    const plan = explain("EXPLAIN SELECT * FROM t");
    expect(plan).toContain("Scan t");
    expect(plan).toContain("Projection");
  });

  it("shows pushed-down filter on scan", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    const plan = explain("EXPLAIN SELECT * FROM t WHERE val > 10");
    // Simple filters get pushed into the scan node
    expect(plan).toContain("Scan t [t.val > 10]");
  });

  it("shows HashJoin for JOIN", async () => {
    await createEngine();
    engine.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)");
    engine.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)");
    const plan = explain(
      "EXPLAIN SELECT * FROM a JOIN b ON a.id = b.a_id",
    );
    expect(plan).toContain("HashJoin [INNER]");
    expect(plan).toContain("Scan a");
    expect(plan).toContain("Scan b");
  });

  it("shows Sort and Limit", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    const plan = explain(
      "EXPLAIN SELECT * FROM t ORDER BY val DESC LIMIT 5",
    );
    expect(plan).toContain("Sort");
    expect(plan).toContain("DESC");
    expect(plan).toContain("Limit 5");
  });

  it("shows Aggregate for GROUP BY", async () => {
    await createEngine();
    engine.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)",
    );
    const plan = explain(
      "EXPLAIN SELECT cat, SUM(val) FROM t GROUP BY cat",
    );
    expect(plan).toContain("Aggregate");
    expect(plan).toContain("SUM");
    expect(plan).toContain("group by");
  });

  it("shows IndexScan when index exists", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const plan = explain("EXPLAIN SELECT * FROM t WHERE val = 1");
    expect(plan).toContain("IndexScan t (idx_val)");
  });

  it("shows Insert plan", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    const plan = explain("EXPLAIN INSERT INTO t (id, val) VALUES (1, 'x')");
    expect(plan).toContain("Insert t");
  });

  it("shows Update plan", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    const plan = explain("EXPLAIN UPDATE t SET val = 'y' WHERE id = 1");
    expect(plan).toContain("Update t");
    expect(plan).toContain("Scan t");
  });

  it("shows Delete plan", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const plan = explain("EXPLAIN DELETE FROM t WHERE id = 1");
    expect(plan).toContain("Delete t");
  });

  it("does not modify database state", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");
    explain("EXPLAIN DELETE FROM t WHERE id = 1");
    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
  });

  it("shows Distinct", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    const plan = explain("EXPLAIN SELECT DISTINCT val FROM t");
    expect(plan).toContain("Distinct");
  });

  it("result type is rows with plan column", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const [result] = engine.execute("EXPLAIN SELECT * FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(1);
    expect(typeof result.rows![0].plan).toBe("string");
  });

  it("plan is properly indented (children deeper than parents)", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    const plan = explain("EXPLAIN SELECT * FROM t WHERE val > 1");
    const lines = plan.split("\n");
    // First line (root) has no indentation
    expect(lines[0]).toBe(lines[0].trimStart());
    // At least one child is indented
    expect(lines.some((l) => l.startsWith("  "))).toBe(true);
  });

  // -------------------------------------------------------------------------
  // ORDER BY via Index
  // -------------------------------------------------------------------------

  it("shows IndexOrderScan and eliminates Sort", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const plan = explain("EXPLAIN SELECT * FROM t ORDER BY val ASC");
    expect(plan).toContain("IndexOrderScan t (idx_val)");
    expect(plan).not.toContain("Sort");
  });

  it("ORDER BY with same index as filter eliminates Sort", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const plan = explain("EXPLAIN SELECT * FROM t WHERE val > 5 ORDER BY val ASC");
    expect(plan).toContain("IndexScan t (idx_val)");
    expect(plan).not.toContain("Sort");
  });

  it("ORDER BY DESC keeps Sort (no reverse scan)", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const plan = explain("EXPLAIN SELECT * FROM t ORDER BY val DESC");
    expect(plan).toContain("Sort");
  });

  it("ORDER BY via index returns correct results", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    engine.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    const [result] = engine.execute("SELECT val FROM t ORDER BY val ASC");
    expect(result.rows!.map((r) => r.val)).toEqual([10, 20, 30]);
  });

  it("ORDER BY via index with LIMIT returns correct results", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    engine.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20), (4, 5)");
    const [result] = engine.execute("SELECT val FROM t ORDER BY val ASC LIMIT 2");
    expect(result.rows!.map((r) => r.val)).toEqual([5, 10]);
  });

  // -------------------------------------------------------------------------
  // MIN/MAX via Index
  // -------------------------------------------------------------------------

  it("shows IndexMinMax for MIN", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const plan = explain("EXPLAIN SELECT MIN(val) FROM t");
    expect(plan).toContain("IndexMinMax [MIN] (idx_val)");
  });

  it("shows IndexMinMax for MAX", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const plan = explain("EXPLAIN SELECT MAX(val) FROM t");
    expect(plan).toContain("IndexMinMax [MAX] (idx_val)");
  });

  it("MIN via index returns correct result", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    engine.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    const [result] = engine.execute("SELECT MIN(val) AS m FROM t");
    expect(result.rows![0].m).toBe(10);
  });

  it("MAX via index returns correct result", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    engine.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    const [result] = engine.execute("SELECT MAX(val) AS m FROM t");
    expect(result.rows![0].m).toBe(30);
  });

  it("MIN on empty table returns NULL", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const [result] = engine.execute("SELECT MIN(val) AS m FROM t");
    expect(result.rows![0].m).toBeNull();
  });

  it("MAX on empty table returns NULL", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("CREATE INDEX idx_val ON t (val)");
    const [result] = engine.execute("SELECT MAX(val) AS m FROM t");
    expect(result.rows![0].m).toBeNull();
  });

  // -------------------------------------------------------------------------
  // OR / Index Union
  // -------------------------------------------------------------------------

  it("shows IndexUnionScan for OR with two indexed columns", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
    engine.execute("CREATE INDEX idx_a ON t (a)");
    engine.execute("CREATE INDEX idx_b ON t (b)");
    const plan = explain("EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2");
    expect(plan).toContain("IndexUnionScan t (idx_a, idx_b)");
  });

  it("OR / Index Union returns correct results", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
    engine.execute("CREATE INDEX idx_a ON t (a)");
    engine.execute("CREATE INDEX idx_b ON t (b)");
    engine.execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 10, 200), (4, 30, 100)");
    const [result] = engine.execute("SELECT id FROM t WHERE a = 10 OR b = 200 ORDER BY id");
    // Row 1: a=10, Row 2: b=200, Row 3: a=10 AND b=200 (dedup), Row 4: neither
    expect(result.rows!.map((r) => r.id)).toEqual([1, 2, 3]);
  });

  it("OR / Index Union deduplicates rows matching both branches", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
    engine.execute("CREATE INDEX idx_a ON t (a)");
    engine.execute("CREATE INDEX idx_b ON t (b)");
    engine.execute("INSERT INTO t VALUES (1, 5, 5)");
    const [result] = engine.execute("SELECT id FROM t WHERE a = 5 OR b = 5");
    expect(result.rows!).toHaveLength(1);
    expect(result.rows![0].id).toBe(1);
  });
});

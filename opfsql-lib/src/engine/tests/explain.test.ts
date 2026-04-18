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
});

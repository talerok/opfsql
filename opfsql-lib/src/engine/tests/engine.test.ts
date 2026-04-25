import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import { Engine, EngineError, PreparedStatement, Session } from "../index.js";

let engine: Engine;
let session: Session;
let seq = 0;

function newDbName(): string {
  return `engine-test-${seq++}`;
}

async function createEngine(name?: string): Promise<Engine> {
  const dbName = name ?? newDbName();
  engine = await Engine.create(new OPFSSyncStorage(dbName));
  session = engine.createSession();
  return engine;
}

beforeEach(() => {
  resetMockOPFS();
});

afterEach(() => {
  session?.close();
  engine?.close();
});

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

describe("initialization", () => {
  it("Engine.create opens storage and returns an engine", async () => {
    const e = await createEngine();
    expect(e).toBeInstanceOf(Engine);
  });

  it("Engine.create loads catalog from storage", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    const s = e.createSession();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("INSERT INTO t (id) VALUES (42)");
    s.close();
    e.close();

    engine = await Engine.create(new OPFSSyncStorage(name));
    session = engine.createSession();
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toEqual([{ id: 42 }]);
  });
});

// ---------------------------------------------------------------------------
// Autocommit
// ---------------------------------------------------------------------------

describe("autocommit", () => {
  it("SELECT does not persist writes", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(1);
  });

  it("INSERT persists immediately in autocommit", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "a" }]);
  });

  it("error in INSERT rolls back catalog", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await expect(
      session.execute("INSERT INTO nonexistent (id) VALUES (1)"),
    ).rejects.toThrow();

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(0);
  });

  it("DDL persists in autocommit", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    const s = e.createSession();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    s.close();
    e.close();

    engine = await Engine.create(new OPFSSyncStorage(name));
    session = engine.createSession();
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Explicit transactions
// ---------------------------------------------------------------------------

describe("explicit transactions", () => {
  it("BEGIN + INSERT + COMMIT persists data", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await session.execute("COMMIT");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "a" }]);
  });

  it("BEGIN + INSERT + ROLLBACK discards data", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await session.execute("ROLLBACK");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("BEGIN + CREATE TABLE + ROLLBACK removes table from catalog", async () => {
    await createEngine();

    await session.execute("BEGIN");
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("ROLLBACK");

    await expect(session.execute("SELECT * FROM t")).rejects.toThrow();
  });

  it("BEGIN + CREATE TABLE + INSERT + COMMIT persists both", async () => {
    await createEngine();

    await session.execute("BEGIN");
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await session.execute("COMMIT");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "a" }]);
  });

  it("COMMIT without BEGIN is a no-op", async () => {
    await createEngine();
    const [result] = await session.execute("COMMIT");
    expect(result.type).toBe("ok");
  });

  it("ROLLBACK without BEGIN is a no-op", async () => {
    await createEngine();
    const [result] = await session.execute("ROLLBACK");
    expect(result.type).toBe("ok");
  });

  it("nested BEGIN throws EngineError", async () => {
    await createEngine();
    await session.execute("BEGIN");
    await expect(session.execute("BEGIN")).rejects.toThrow(EngineError);
  });
});

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

describe("DROP TABLE", () => {
  it("DROP TABLE removes table and persists", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    const s = e.createSession();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("INSERT INTO t (id) VALUES (1)");
    await s.execute("DROP TABLE t");
    s.close();
    e.close();

    engine = await Engine.create(new OPFSSyncStorage(name));
    session = engine.createSession();
    await expect(session.execute("SELECT * FROM t")).rejects.toThrow();
  });

  it("DROP TABLE in transaction + ROLLBACK restores table", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t (id) VALUES (1)");

    await session.execute("BEGIN");
    await session.execute("DROP TABLE t");
    await session.execute("ROLLBACK");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Multiple statements
// ---------------------------------------------------------------------------

describe("multiple statements", () => {
  it("multiple INSERTs in one execute call all persist", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await session.execute(
      "INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (2); INSERT INTO t (id) VALUES (3)",
    );

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(3);
  });

  it("statement execution order is preserved", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");

    await session.execute(
      "INSERT INTO t (id, val) VALUES (1, 'first'); INSERT INTO t (id, val) VALUES (2, 'second'); INSERT INTO t (id, val) VALUES (3, 'third')",
    );

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(3);
    const ids = result.rows!.map((r) => r.id).sort();
    expect(ids).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// Column aliases (AS)
// ---------------------------------------------------------------------------

describe("column aliases", () => {
  it("AS alias appears as output row key", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'alice')");

    const [result] = await session.execute("SELECT name AS username FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toHaveProperty("username");
    expect(result.rows![0].username).toBe("alice");
  });

  it("multiple aliases all appear in output", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)",
    );

    const [result] = await session.execute("SELECT name AS n, age AS a FROM t");
    expect(result.rows![0]).toEqual({ n: "alice", a: 30 });
  });

  it("expression alias on computed column", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, price REAL, qty INTEGER)",
    );
    await session.execute("INSERT INTO t (id, price, qty) VALUES (1, 10.5, 3)");

    const [result] = await session.execute(
      "SELECT price * qty AS total FROM t",
    );
    expect(result.rows![0]).toHaveProperty("total");
    expect(result.rows![0].total).toBeCloseTo(31.5);
  });
});

// ---------------------------------------------------------------------------
// WHERE + column pruning
// ---------------------------------------------------------------------------

describe("WHERE + column pruning", () => {
  it("SELECT subset with WHERE on non-selected column", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (3, 'Charlie', 35)",
    );

    const [result] = await session.execute("SELECT name FROM t WHERE age > 28");
    expect(result.rows).toHaveLength(2);
    const names = result.rows!.map((r) => r.name).sort();
    expect(names).toEqual(["Alice", "Charlie"]);
  });
});

// ---------------------------------------------------------------------------
// DML without WHERE
// ---------------------------------------------------------------------------

describe("DML without WHERE", () => {
  it("UPDATE all rows without WHERE", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)",
    );

    await session.execute("UPDATE t SET age = 99");
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows!.every((r: any) => r.age === 99)).toBe(true);
  });

  it("DELETE all rows without WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await session.execute("INSERT INTO t (id, name) VALUES (2, 'b')");

    await session.execute("DELETE FROM t");
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("UPDATE all rows — check actual values", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)",
    );
    await session.execute("INSERT INTO t (id, age) VALUES (1, 10)");
    await session.execute("INSERT INTO t (id, age) VALUES (2, 20)");

    const [updateResult] = await session.execute("UPDATE t SET age = 99");
    const [after] = await session.execute("SELECT * FROM t");

    expect(updateResult.rowsAffected).toBe(2);
    expect(after.rows).toHaveLength(2);
    expect(after.rows!.map((r: any) => r.age).sort()).toEqual([99, 99]);
  });

  it("UPDATE with WHERE on non-updated column", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)",
    );
    await session.execute(
      "INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)",
    );

    await session.execute("UPDATE t SET name = 'X' WHERE age > 28");
    const [result] = await session.execute("SELECT * FROM t");
    const alice = result.rows!.find((r: any) => r.id === 1);
    expect(alice!.name).toBe("X");
    const bob = result.rows!.find((r: any) => r.id === 2);
    expect(bob!.name).toBe("Bob");
  });
});

// ---------------------------------------------------------------------------
// Recursive CTE
// ---------------------------------------------------------------------------

describe("recursive CTE", () => {
  it("generates a number sequence", async () => {
    await createEngine();
    const [result] = await session.execute(
      "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT n FROM nums",
    );
    expect(result.rows!.map((r: any) => r.n)).toEqual([1, 2, 3, 4, 5]);
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe("error handling", () => {
  it("error in middle of transaction aborts entire transaction", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (id) VALUES (1)");

    await expect(
      session.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).rejects.toThrow();

    await expect(
      session.execute("INSERT INTO t (id) VALUES (3)"),
    ).rejects.toThrow(/transaction is aborted/);

    await expect(session.execute("COMMIT")).rejects.toThrow(/ROLLBACK/);

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("ROLLBACK after aborted transaction allows new work", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (id) VALUES (1)");
    await expect(
      session.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).rejects.toThrow();

    await session.execute("ROLLBACK");

    await session.execute("INSERT INTO t (id) VALUES (10)");
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBe(10);
  });

  it("error in autocommit rolls back only that statement", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t (id) VALUES (1)");

    await expect(
      session.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).rejects.toThrow();

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Auto PK index
// ---------------------------------------------------------------------------

describe("auto PK index", () => {
  it("PRIMARY KEY creates a unique index automatically", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'a')");
    await session.execute("INSERT INTO t VALUES (2, 'b')");
    await session.execute("INSERT INTO t VALUES (3, 'c')");

    const [result] = await session.execute("SELECT name FROM t WHERE id = 2");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].name).toBe("b");
  });

  it("PK index enforces uniqueness", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'a')");

    await expect(
      session.execute("INSERT INTO t VALUES (1, 'dup')"),
    ).rejects.toThrow();
  });

  it("PK index works with UPDATE and DELETE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'a')");
    await session.execute("INSERT INTO t VALUES (2, 'b')");

    await session.execute("DELETE FROM t WHERE id = 1");
    await session.execute("INSERT INTO t VALUES (1, 'reused')");
    const [result] = await session.execute("SELECT name FROM t WHERE id = 1");
    expect(result.rows![0].name).toBe("reused");
  });

  it("PK index survives transaction rollback", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t VALUES (2)");
    await session.execute("ROLLBACK");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);

    await session.execute("INSERT INTO t VALUES (2)");
    const [result2] = await session.execute("SELECT * FROM t WHERE id = 2");
    expect(result2.rows).toHaveLength(1);
  });

  it("DROP TABLE cleans up PK index", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("DROP TABLE t");

    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// ALTER TABLE ADD COLUMN — default values for existing rows
// ---------------------------------------------------------------------------

describe("ALTER TABLE ADD COLUMN defaults", () => {
  it("existing rows get DEFAULT value after ADD COLUMN", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("INSERT INTO t VALUES (2)");

    await session.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");

    const [result] = await session.execute("SELECT id, val FROM t ORDER BY id");
    expect(result.rows).toEqual([
      { id: 1, val: 99 },
      { id: 2, val: 99 },
    ]);
  });

  it("existing rows get NULL for ADD COLUMN without DEFAULT", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");

    await session.execute("ALTER TABLE t ADD COLUMN val TEXT");

    const [result] = await session.execute("SELECT id, val FROM t");
    expect(result.rows).toEqual([{ id: 1, val: null }]);
  });

  it("new rows after ADD COLUMN get explicit value", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");

    await session.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    await session.execute("INSERT INTO t (id, val) VALUES (2, 42)");

    const [result] = await session.execute("SELECT id, val FROM t ORDER BY id");
    expect(result.rows).toEqual([
      { id: 1, val: 99 },
      { id: 2, val: 42 },
    ]);
  });

  it("UPDATE works on rows with default-filled column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");

    await session.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    await session.execute("UPDATE t SET val = 50 WHERE id = 1");

    const [result] = await session.execute("SELECT val FROM t");
    expect(result.rows![0].val).toBe(50);
  });

  it("DELETE works on rows with default-filled column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("INSERT INTO t VALUES (2)");

    await session.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    await session.execute("DELETE FROM t WHERE val = 99");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("ADD COLUMN defaults persist across reopen", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    const s = e.createSession();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("INSERT INTO t VALUES (1)");
    await s.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    s.close();
    e.close();

    engine = await Engine.create(new OPFSSyncStorage(name));
    session = engine.createSession();
    const [result] = await session.execute("SELECT id, val FROM t");
    expect(result.rows).toEqual([{ id: 1, val: 99 }]);
  });
});

// ---------------------------------------------------------------------------
// CREATE INDEX — __pk_ prefix rejection
// ---------------------------------------------------------------------------

describe("CREATE INDEX — __pk_ prefix", () => {
  it("rejects index name with __pk_ prefix", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER, name TEXT)");

    await expect(
      session.execute("CREATE INDEX __pk_t ON t (name)"),
    ).rejects.toThrow(/__pk_/);
  });
});

// ---------------------------------------------------------------------------
// ALTER TABLE DROP COLUMN — index reference
// ---------------------------------------------------------------------------

describe("ALTER TABLE DROP COLUMN — index reference", () => {
  it("rejects DROP COLUMN when column is indexed", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    await session.execute("CREATE INDEX idx_name ON t (name)");

    await expect(
      session.execute("ALTER TABLE t DROP COLUMN name"),
    ).rejects.toThrow(/referenced by index/);
  });

  it("allows DROP COLUMN after dropping the index", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    await session.execute("CREATE INDEX idx_name ON t (name)");
    await session.execute("DROP INDEX idx_name");
    await session.execute("ALTER TABLE t DROP COLUMN name");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// SUBSTR — negative start
// ---------------------------------------------------------------------------

describe("SUBSTR — negative start", () => {
  it("SUBSTR with negative start returns from end", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'hello')");

    const [r1] = await session.execute("SELECT SUBSTR(s, -1) AS v FROM t");
    expect(r1.rows![0].v).toBe("o");

    const [r2] = await session.execute("SELECT SUBSTR(s, -2, 2) AS v FROM t");
    expect(r2.rows![0].v).toBe("lo");
  });
});

// ---------------------------------------------------------------------------
// GROUP BY + aggregates
// ---------------------------------------------------------------------------

describe("GROUP BY + aggregates", () => {
  it("GROUP BY with COUNT(*)", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
    );
    await session.execute(
      "INSERT INTO products VALUES (1, 'A', 10.0, 'Books')",
    );
    await session.execute(
      "INSERT INTO products VALUES (2, 'B', 20.0, 'Books')",
    );
    await session.execute("INSERT INTO products VALUES (3, 'C', 30.0, 'Toys')");

    const [result] = await session.execute(
      "SELECT category, COUNT(*) AS cnt FROM products GROUP BY category",
    );
    expect(result.rows).toHaveLength(2);
    const sorted = result.rows!.sort((a: any, b: any) =>
      a.category.localeCompare(b.category),
    );
    expect(sorted[0]).toEqual({ category: "Books", cnt: 2 });
    expect(sorted[1]).toEqual({ category: "Toys", cnt: 1 });
  });

  it("aggregate without GROUP BY", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("INSERT INTO t VALUES (1, 10)");
    await session.execute("INSERT INTO t VALUES (2, 20)");
    await session.execute("INSERT INTO t VALUES (3, 30)");

    const [result] = await session.execute(
      "SELECT COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val FROM t",
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toEqual({ cnt: 3, total: 60, avg_val: 20 });
  });

  it("ORDER BY + LIMIT returns correct top-K rows", async () => {
    await createEngine();
    await session.execute("CREATE TABLE scores (id INTEGER, val INTEGER)");
    await session.execute("INSERT INTO scores VALUES (1, 50)");
    await session.execute("INSERT INTO scores VALUES (2, 90)");
    await session.execute("INSERT INTO scores VALUES (3, 10)");
    await session.execute("INSERT INTO scores VALUES (4, 80)");
    await session.execute("INSERT INTO scores VALUES (5, 70)");

    const [desc] = await session.execute(
      "SELECT * FROM scores ORDER BY val DESC LIMIT 3",
    );
    expect(desc.rows!.map((r: any) => r.val)).toEqual([90, 80, 70]);
  });
});

// ---------------------------------------------------------------------------
// Parameterized queries
// ---------------------------------------------------------------------------

describe("parameterized queries", () => {
  it("execute() with $1 in INSERT", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES ($1, $2)", [
      1,
      "alice",
    ]);

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "alice" }]);
  });

  it("execute() with $1 in WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'alice')");
    await session.execute("INSERT INTO t VALUES (2, 'bob')");

    const [result] = await session.execute("SELECT * FROM t WHERE id = $1", [
      1,
    ]);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toEqual({ id: 1, name: "alice" });
  });

  it("prepare() + run() executes multiple times with different params", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    const stmt = session.prepare("INSERT INTO t (id, name) VALUES ($1, $2)");
    expect(stmt).toBeInstanceOf(PreparedStatement);

    await stmt.run([1, "alice"]);
    await stmt.run([2, "bob"]);
    await stmt.run([3, "charlie"]);

    const [result] = await session.execute("SELECT * FROM t ORDER BY id");
    expect(result.rows).toHaveLength(3);
    expect(result.rows![0]).toEqual({ id: 1, name: "alice" });
    expect(result.rows![1]).toEqual({ id: 2, name: "bob" });
    expect(result.rows![2]).toEqual({ id: 3, name: "charlie" });
  });

  it("prepare() SELECT with $1 in WHERE returns correct row", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("INSERT INTO t VALUES (1, 100)");
    await session.execute("INSERT INTO t VALUES (2, 200)");
    await session.execute("INSERT INTO t VALUES (3, 300)");

    const stmt = session.prepare("SELECT val FROM t WHERE id = $1");
    const r1 = await stmt.run([2]);
    expect(r1.type).toBe("rows");
    expect(r1.rows).toHaveLength(1);
    expect(r1.rows![0].val).toBe(200);

    const r3 = await stmt.run([3]);
    expect(r3.rows![0].val).toBe(300);
  });

  it("prepare() requires exactly one statement", async () => {
    await createEngine();
    expect(() => session.prepare("SELECT 1; SELECT 2")).toThrow(EngineError);
  });

  it("UPDATE with parameterized SET and WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'old')");

    await session.execute("UPDATE t SET name = $1 WHERE id = $2", ["new", 1]);

    const [result] = await session.execute("SELECT name FROM t WHERE id = 1");
    expect(result.rows![0].name).toBe("new");
  });

  it("DELETE with parameterized WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("INSERT INTO t VALUES (2)");

    await session.execute("DELETE FROM t WHERE id = $1", [1]);

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// JSON type
// ---------------------------------------------------------------------------

describe("JSON type", () => {
  it("INSERT and SELECT with dot path access", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(
      `INSERT INTO t (id, data) VALUES (1, '{"name": "Alice", "age": 30}')`,
    );

    const [result] = await session.execute(
      "SELECT id, data.name, data.age FROM t",
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toMatchObject({ id: 1 });
  });

  it("WHERE filter with JSON path range comparison", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    await session.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    const [result] = await session.execute(
      "SELECT id FROM t WHERE data.age > 20 AND data.age < 30",
    );
    expect(result.rows!.map((r) => r.id)).toEqual([2]);
  });

  it("WHERE with single JSON path less-than", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    await session.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    const [r1] = await session.execute("SELECT id FROM t WHERE data.age < 30");
    expect(r1.rows!.map((r) => r.id)).toEqual([2]);

    const [r2] = await session.execute("SELECT id FROM t WHERE data.age > 20");
    expect(r2.rows!.map((r) => r.id).sort()).toEqual([1, 2]);
  });

  it("DELETE with multi-expression filter on JSON path", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    await session.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);
    await session.execute(`INSERT INTO t (id, data) VALUES (3, '{"age": 15}')`);

    await session.execute(
      "DELETE FROM t WHERE data.age > 20 AND data.age < 30",
    );

    const [result] = await session.execute("SELECT id FROM t ORDER BY id");
    expect(result.rows!.map((r) => r.id)).toEqual([1, 3]);
  });

  it("UPDATE with multi-expression filter on JSON path", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    await session.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    await session.execute(
      `UPDATE t SET data = '{"age": 99}' WHERE data.age > 20 AND data.age < 30`,
    );

    const [result] = await session.execute(
      "SELECT id, data.age FROM t ORDER BY id",
    );
    expect(result.rows![0]).toMatchObject({ id: 1 });
    expect(result.rows![1]).toMatchObject({ id: 2 });
  });

  it("CONCAT with JSON column produces JSON string", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(
      `INSERT INTO t (id, data) VALUES (1, '{"name": "Alice"}')`,
    );

    const [result] = await session.execute(
      "SELECT data.name || '!' AS greeting FROM t",
    );
    expect(result.rows![0].greeting).toBe("Alice!");
  });

  it("CAST number/boolean to JSON preserves value", async () => {
    await createEngine();
    const [r1] = await session.execute("SELECT CAST(42 AS JSON) AS v");
    expect(r1.rows![0].v).toBe(42);

    const [r2] = await session.execute("SELECT CAST(TRUE AS JSON) AS v");
    expect(r2.rows![0].v).toBe(true);
  });
});

// ============================================================================
// BLOB type
// ============================================================================

describe("BLOB type", () => {
  it("INSERT and SELECT blob literal", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t (id, data) VALUES (1, x'DEADBEEF')");

    const [result] = await session.execute("SELECT id, data FROM t");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].data).toBeInstanceOf(Uint8Array);
    expect(result.rows![0].data).toEqual(
      new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
    );
  });

  it("CAST blob to TEXT returns hex", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'CAFE')");

    const [result] = await session.execute(
      "SELECT CAST(data AS TEXT) AS hex FROM t",
    );
    expect(result.rows![0].hex).toBe("CAFE");
  });

  it("CAST text to BLOB", async () => {
    await createEngine();
    const [result] = await session.execute("SELECT CAST('FF00' AS BLOB) AS v");
    expect(result.rows![0].v).toBeInstanceOf(Uint8Array);
    expect(result.rows![0].v).toEqual(new Uint8Array([0xff, 0x00]));
  });

  it("blob parameter support", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, $1)", [
      new Uint8Array([0xaa, 0xbb]),
    ]);

    const [result] = await session.execute("SELECT data FROM t");
    expect(result.rows![0].data).toEqual(new Uint8Array([0xaa, 0xbb]));
  });

  it("blob comparison in WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'AA')");
    await session.execute("INSERT INTO t VALUES (2, x'BB')");

    const [result] = await session.execute(
      "SELECT id FROM t WHERE data = x'BB'",
    );
    expect(result.rows!.map((r) => r.id)).toEqual([2]);
  });

  it("ORDER BY blob column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'FF')");
    await session.execute("INSERT INTO t VALUES (2, x'00')");
    await session.execute("INSERT INTO t VALUES (3, x'AA')");

    const [result] = await session.execute("SELECT id FROM t ORDER BY data");
    expect(result.rows!.map((r) => r.id)).toEqual([2, 3, 1]);
  });

  it("UPDATE blob column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'AA')");
    await session.execute("UPDATE t SET data = x'BB' WHERE id = 1");

    const [result] = await session.execute("SELECT data FROM t");
    expect(result.rows![0].data).toEqual(new Uint8Array([0xbb]));
  });

  it("NULL blob", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, NULL)");

    const [result] = await session.execute("SELECT data FROM t WHERE id = 1");
    expect(result.rows![0].data).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Catalog versioning
// ---------------------------------------------------------------------------

describe("catalog versioning", () => {
  it("version starts at 0 for a fresh engine", async () => {
    await createEngine();
    const schema = session.getSchema();
    expect(schema.version).toBe(0);
  });

  it("version increments on DDL commit", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const schema = session.getSchema();
    expect(schema.version).toBe(1);
  });

  it("multiple DDLs in one transaction produce single version bump", async () => {
    await createEngine();
    await session.execute("BEGIN");
    await session.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)");
    await session.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)");
    await session.execute("COMMIT");

    const schema = session.getSchema();
    expect(schema.version).toBe(1);
  });

  it("DML-only commit does not bump version", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const v1 = session.getSchema().version;

    await session.execute("INSERT INTO t VALUES (1)");
    const v2 = session.getSchema().version;
    expect(v2).toBe(v1);
  });

  it("version persists across reopen", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    const s = e.createSession();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("CREATE INDEX idx ON t (id)");
    const v1 = s.getSchema().version;
    s.close();
    e.close();

    engine = await Engine.create(new OPFSSyncStorage(name));
    session = engine.createSession();
    expect(session.getSchema().version).toBe(v1);
  });

  it("DDL rollback does not bump version", async () => {
    await createEngine();
    const v0 = session.getSchema().version;

    await session.execute("BEGIN");
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("ROLLBACK");

    expect(session.getSchema().version).toBe(v0);
  });
});

// ---------------------------------------------------------------------------
// Stale prepared statement
// ---------------------------------------------------------------------------

describe("stale prepared statement", () => {
  it("throws when schema changes after prepare()", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'alice')");

    const stmt = session.prepare("SELECT * FROM t WHERE id = $1");

    await session.execute("ALTER TABLE t ADD COLUMN age INTEGER DEFAULT 0");

    await expect(stmt.run([1])).rejects.toThrow(/stale/);
  });

  it("works when schema has not changed", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'alice')");

    const stmt = session.prepare("SELECT * FROM t WHERE id = $1");
    const result = await stmt.run([1]);
    expect(result.type).toBe("rows");
    expect(result.rows![0].name).toBe("alice");
  });

  it("stale across sessions from same engine", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    const s2 = engine.createSession();
    const stmt = s2.prepare("SELECT * FROM t");

    await session.execute("ALTER TABLE t ADD COLUMN v INTEGER DEFAULT 0");

    await expect(stmt.run()).rejects.toThrow(/stale/);
    s2.close();
  });
});

// ---------------------------------------------------------------------------
// Use after close
// ---------------------------------------------------------------------------

describe("use after close", () => {
  it("execute() throws after session is closed", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    session.close();

    await expect(session.execute("SELECT * FROM t")).rejects.toThrow();
  });
});

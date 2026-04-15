import { afterEach, describe, expect, it } from "vitest";
import { MemoryPageStorage } from "../../store/memory-storage.js";
import { Engine, EngineError, PreparedStatement } from "../index.js";

let engine: Engine;
let dbName: string;
const storageMap = new Map<string, MemoryPageStorage>();

function newDbName(): string {
  return `test-${Math.random()}`;
}

function getStorage(name: string): MemoryPageStorage {
  let s = storageMap.get(name);
  if (!s) {
    s = new MemoryPageStorage();
    storageMap.set(name, s);
  }
  return s;
}

async function createEngine(name?: string): Promise<Engine> {
  dbName = name ?? newDbName();
  engine = await Engine.create(getStorage(dbName));
  return engine;
}

afterEach(() => {
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
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    e.execute("INSERT INTO t (id) VALUES (42)");
    e.close();

    engine = await Engine.create(getStorage(name));
    const [result] = engine.execute("SELECT * FROM t");
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(1);
  });

  it("INSERT persists immediately in autocommit", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "a" }]);
  });

  it("error in INSERT rolls back catalog", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    expect(() =>
      engine.execute("INSERT INTO nonexistent (id) VALUES (1)"),
    ).toThrow();

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(0);
  });

  it("DDL persists in autocommit", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    e.close();

    engine = await Engine.create(getStorage(name));
    const [result] = engine.execute("SELECT * FROM t");
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    engine.execute("BEGIN");
    engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    engine.execute("COMMIT");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "a" }]);
  });

  it("BEGIN + INSERT + ROLLBACK discards data", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    engine.execute("BEGIN");
    engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    engine.execute("ROLLBACK");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("BEGIN + CREATE TABLE + ROLLBACK removes table from catalog", async () => {
    await createEngine();

    engine.execute("BEGIN");
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("ROLLBACK");

    expect(() => engine.execute("SELECT * FROM t")).toThrow();
  });

  it("BEGIN + CREATE TABLE + INSERT + COMMIT persists both", async () => {
    await createEngine();

    engine.execute("BEGIN");
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    engine.execute("COMMIT");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "a" }]);
  });

  it("COMMIT without BEGIN is a no-op", async () => {
    await createEngine();
    const [result] = engine.execute("COMMIT");
    expect(result.type).toBe("ok");
  });

  it("ROLLBACK without BEGIN is a no-op", async () => {
    await createEngine();
    const [result] = engine.execute("ROLLBACK");
    expect(result.type).toBe("ok");
  });

  it("nested BEGIN throws EngineError", async () => {
    await createEngine();
    engine.execute("BEGIN");
    expect(() => engine.execute("BEGIN")).toThrow(EngineError);
  });
});

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

describe("DROP TABLE", () => {
  it("DROP TABLE removes table and persists", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    e.execute("INSERT INTO t (id) VALUES (1)");
    e.execute("DROP TABLE t");
    e.close();

    engine = await Engine.create(getStorage(name));
    expect(() => engine.execute("SELECT * FROM t")).toThrow();
  });

  it("DROP TABLE in transaction + ROLLBACK restores table", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t (id) VALUES (1)");

    engine.execute("BEGIN");
    engine.execute("DROP TABLE t");
    engine.execute("ROLLBACK");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Multiple statements
// ---------------------------------------------------------------------------

describe("multiple statements", () => {
  it("multiple INSERTs in one execute call all persist", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    engine.execute(
      "INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (2); INSERT INTO t (id) VALUES (3)",
    );

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(3);
  });

  it("statement execution order is preserved", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");

    engine.execute(
      "INSERT INTO t (id, val) VALUES (1, 'first'); INSERT INTO t (id, val) VALUES (2, 'second'); INSERT INTO t (id, val) VALUES (3, 'third')",
    );

    const [result] = engine.execute("SELECT * FROM t");
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t (id, name) VALUES (1, 'alice')");

    const [result] = engine.execute("SELECT name AS username FROM t");
    expect(result.type).toBe("rows");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toHaveProperty("username");
    expect(result.rows![0].username).toBe("alice");
  });

  it("multiple aliases all appear in output", async () => {
    await createEngine();
    engine.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)");

    const [result] = engine.execute("SELECT name AS n, age AS a FROM t");
    expect(result.rows![0]).toEqual({ n: "alice", a: 30 });
  });

  it("expression alias on computed column", async () => {
    await createEngine();
    engine.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, price REAL, qty INTEGER)",
    );
    engine.execute("INSERT INTO t (id, price, qty) VALUES (1, 10.5, 3)");

    const [result] = engine.execute("SELECT price * qty AS total FROM t");
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
    engine.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)");
    engine.execute("INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)");
    engine.execute("INSERT INTO t (id, name, age) VALUES (3, 'Charlie', 35)");

    const [result] = engine.execute("SELECT name FROM t WHERE age > 28");
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
    engine.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)");
    engine.execute("INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)");

    engine.execute("UPDATE t SET age = 99");
    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows!.every((r: any) => r.age === 99)).toBe(true);
  });

  it("DELETE all rows without WHERE", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    engine.execute("INSERT INTO t (id, name) VALUES (2, 'b')");

    engine.execute("DELETE FROM t");
    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("UPDATE all rows — check actual values", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)");
    engine.execute("INSERT INTO t (id, age) VALUES (1, 10)");
    engine.execute("INSERT INTO t (id, age) VALUES (2, 20)");

    const [updateResult] = engine.execute("UPDATE t SET age = 99");
    const [after] = engine.execute("SELECT * FROM t");

    expect(updateResult.rowsAffected).toBe(2);
    expect(after.rows).toHaveLength(2);
    expect(after.rows!.map((r: any) => r.age).sort()).toEqual([99, 99]);
  });

  it("UPDATE with WHERE on non-updated column", async () => {
    await createEngine();
    engine.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)");
    engine.execute("INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)");

    engine.execute("UPDATE t SET name = 'X' WHERE age > 28");
    const [result] = engine.execute("SELECT * FROM t");
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
    const [result] = engine.execute(
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    engine.execute("BEGIN");
    engine.execute("INSERT INTO t (id) VALUES (1)");

    expect(() =>
      engine.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).toThrow();

    expect(() => engine.execute("INSERT INTO t (id) VALUES (3)")).toThrow(
      /transaction is aborted/,
    );

    expect(() => engine.execute("COMMIT")).toThrow(/ROLLBACK/);

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("ROLLBACK after aborted transaction allows new work", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    engine.execute("BEGIN");
    engine.execute("INSERT INTO t (id) VALUES (1)");
    expect(() =>
      engine.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).toThrow();

    engine.execute("ROLLBACK");

    engine.execute("INSERT INTO t (id) VALUES (10)");
    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBe(10);
  });

  it("error in autocommit rolls back only that statement", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t (id) VALUES (1)");

    expect(() =>
      engine.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).toThrow();

    const [result] = engine.execute("SELECT * FROM t");
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t VALUES (1, 'a')");
    engine.execute("INSERT INTO t VALUES (2, 'b')");
    engine.execute("INSERT INTO t VALUES (3, 'c')");

    const [result] = engine.execute("SELECT name FROM t WHERE id = 2");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].name).toBe("b");
  });

  it("PK index enforces uniqueness", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t VALUES (1, 'a')");

    expect(() => engine.execute("INSERT INTO t VALUES (1, 'dup')")).toThrow();
  });

  it("PK index works with UPDATE and DELETE", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t VALUES (1, 'a')");
    engine.execute("INSERT INTO t VALUES (2, 'b')");

    engine.execute("DELETE FROM t WHERE id = 1");
    engine.execute("INSERT INTO t VALUES (1, 'reused')");
    const [result] = engine.execute("SELECT name FROM t WHERE id = 1");
    expect(result.rows![0].name).toBe("reused");
  });

  it("PK index survives transaction rollback", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");

    engine.execute("BEGIN");
    engine.execute("INSERT INTO t VALUES (2)");
    engine.execute("ROLLBACK");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);

    engine.execute("INSERT INTO t VALUES (2)");
    const [result2] = engine.execute("SELECT * FROM t WHERE id = 2");
    expect(result2.rows).toHaveLength(1);
  });

  it("DROP TABLE cleans up PK index", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");
    engine.execute("DROP TABLE t");

    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");
    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// ALTER TABLE ADD COLUMN — default values for existing rows
// ---------------------------------------------------------------------------

describe("ALTER TABLE ADD COLUMN defaults", () => {
  it("existing rows get DEFAULT value after ADD COLUMN", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");
    engine.execute("INSERT INTO t VALUES (2)");

    engine.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");

    const [result] = engine.execute("SELECT id, val FROM t ORDER BY id");
    expect(result.rows).toEqual([
      { id: 1, val: 99 },
      { id: 2, val: 99 },
    ]);
  });

  it("existing rows get NULL for ADD COLUMN without DEFAULT", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");

    engine.execute("ALTER TABLE t ADD COLUMN val TEXT");

    const [result] = engine.execute("SELECT id, val FROM t");
    expect(result.rows).toEqual([{ id: 1, val: null }]);
  });

  it("new rows after ADD COLUMN get explicit value", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");

    engine.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    engine.execute("INSERT INTO t (id, val) VALUES (2, 42)");

    const [result] = engine.execute("SELECT id, val FROM t ORDER BY id");
    expect(result.rows).toEqual([
      { id: 1, val: 99 },
      { id: 2, val: 42 },
    ]);
  });

  it("UPDATE works on rows with default-filled column", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");

    engine.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    engine.execute("UPDATE t SET val = 50 WHERE id = 1");

    const [result] = engine.execute("SELECT val FROM t");
    expect(result.rows![0].val).toBe(50);
  });

  it("DELETE works on rows with default-filled column", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");
    engine.execute("INSERT INTO t VALUES (2)");

    engine.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    engine.execute("DELETE FROM t WHERE val = 99");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });

  it("ADD COLUMN defaults persist across reopen", async () => {
    const name = newDbName();
    const e = await createEngine(name);
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    e.execute("INSERT INTO t VALUES (1)");
    e.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    e.close();

    engine = await Engine.create(getStorage(name));
    const [result] = engine.execute("SELECT id, val FROM t");
    expect(result.rows).toEqual([{ id: 1, val: 99 }]);
  });
});

// ---------------------------------------------------------------------------
// CREATE INDEX — __pk_ prefix rejection
// ---------------------------------------------------------------------------

describe("CREATE INDEX — __pk_ prefix", () => {
  it("rejects index name with __pk_ prefix", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER, name TEXT)");

    expect(() => engine.execute("CREATE INDEX __pk_t ON t (name)")).toThrow(/__pk_/);
  });
});

// ---------------------------------------------------------------------------
// ALTER TABLE DROP COLUMN — index reference
// ---------------------------------------------------------------------------

describe("ALTER TABLE DROP COLUMN — index reference", () => {
  it("rejects DROP COLUMN when column is indexed", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
    engine.execute("CREATE INDEX idx_name ON t (name)");

    expect(() => engine.execute("ALTER TABLE t DROP COLUMN name")).toThrow(/referenced by index/);
  });

  it("allows DROP COLUMN after dropping the index", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
    engine.execute("CREATE INDEX idx_name ON t (name)");
    engine.execute("DROP INDEX idx_name");
    engine.execute("ALTER TABLE t DROP COLUMN name");

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// SUBSTR — negative start
// ---------------------------------------------------------------------------

describe("SUBSTR — negative start", () => {
  it("SUBSTR with negative start returns from end", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)");
    engine.execute("INSERT INTO t VALUES (1, 'hello')");

    const [r1] = engine.execute("SELECT SUBSTR(s, -1) AS v FROM t");
    expect(r1.rows![0].v).toBe("o");

    const [r2] = engine.execute("SELECT SUBSTR(s, -2, 2) AS v FROM t");
    expect(r2.rows![0].v).toBe("lo");
  });
});

// ---------------------------------------------------------------------------
// GROUP BY + aggregates
// ---------------------------------------------------------------------------

describe("GROUP BY + aggregates", () => {
  it("GROUP BY with COUNT(*)", async () => {
    await createEngine();
    engine.execute(
      "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
    );
    engine.execute("INSERT INTO products VALUES (1, 'A', 10.0, 'Books')");
    engine.execute("INSERT INTO products VALUES (2, 'B', 20.0, 'Books')");
    engine.execute("INSERT INTO products VALUES (3, 'C', 30.0, 'Toys')");

    const [result] = engine.execute(
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("INSERT INTO t VALUES (1, 10)");
    engine.execute("INSERT INTO t VALUES (2, 20)");
    engine.execute("INSERT INTO t VALUES (3, 30)");

    const [result] = engine.execute(
      "SELECT COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val FROM t",
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toEqual({ cnt: 3, total: 60, avg_val: 20 });
  });

  it("ORDER BY + LIMIT returns correct top-K rows", async () => {
    await createEngine();
    engine.execute("CREATE TABLE scores (id INTEGER, val INTEGER)");
    engine.execute("INSERT INTO scores VALUES (1, 50)");
    engine.execute("INSERT INTO scores VALUES (2, 90)");
    engine.execute("INSERT INTO scores VALUES (3, 10)");
    engine.execute("INSERT INTO scores VALUES (4, 80)");
    engine.execute("INSERT INTO scores VALUES (5, 70)");

    const [desc] = engine.execute(
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t (id, name) VALUES ($1, $2)", [1, "alice"]);

    const [result] = engine.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "alice" }]);
  });

  it("execute() with $1 in WHERE", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t VALUES (1, 'alice')");
    engine.execute("INSERT INTO t VALUES (2, 'bob')");

    const [result] = engine.execute("SELECT * FROM t WHERE id = $1", [1]);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toEqual({ id: 1, name: "alice" });
  });

  it("prepare() + run() executes multiple times with different params", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    const stmt = engine.prepare("INSERT INTO t (id, name) VALUES ($1, $2)");
    expect(stmt).toBeInstanceOf(PreparedStatement);

    stmt.run([1, "alice"]);
    stmt.run([2, "bob"]);
    stmt.run([3, "charlie"]);

    const [result] = engine.execute("SELECT * FROM t ORDER BY id");
    expect(result.rows).toHaveLength(3);
    expect(result.rows![0]).toEqual({ id: 1, name: "alice" });
    expect(result.rows![1]).toEqual({ id: 2, name: "bob" });
    expect(result.rows![2]).toEqual({ id: 3, name: "charlie" });
  });

  it("prepare() SELECT with $1 in WHERE returns correct row", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
    engine.execute("INSERT INTO t VALUES (1, 100)");
    engine.execute("INSERT INTO t VALUES (2, 200)");
    engine.execute("INSERT INTO t VALUES (3, 300)");

    const stmt = engine.prepare("SELECT val FROM t WHERE id = $1");
    const r1 = stmt.run([2]);
    expect(r1.type).toBe("rows");
    expect(r1.rows).toHaveLength(1);
    expect(r1.rows![0].val).toBe(200);

    const r3 = stmt.run([3]);
    expect(r3.rows![0].val).toBe(300);
  });

  it("prepare() requires exactly one statement", async () => {
    await createEngine();
    expect(() => engine.prepare("SELECT 1; SELECT 2")).toThrow(EngineError);
  });

  it("UPDATE with parameterized SET and WHERE", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    engine.execute("INSERT INTO t VALUES (1, 'old')");

    engine.execute("UPDATE t SET name = $1 WHERE id = $2", ["new", 1]);

    const [result] = engine.execute("SELECT name FROM t WHERE id = 1");
    expect(result.rows![0].name).toBe("new");
  });

  it("DELETE with parameterized WHERE", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    engine.execute("INSERT INTO t VALUES (1)");
    engine.execute("INSERT INTO t VALUES (2)");

    engine.execute("DELETE FROM t WHERE id = $1", [1]);

    const [result] = engine.execute("SELECT * FROM t");
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
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    engine.execute(`INSERT INTO t (id, data) VALUES (1, '{"name": "Alice", "age": 30}')`);

    const [result] = engine.execute("SELECT id, data.name, data.age FROM t");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toMatchObject({ id: 1 });
  });

  it("WHERE filter with JSON path range comparison", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    engine.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    engine.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    const [result] = engine.execute("SELECT id FROM t WHERE data.age > 20 AND data.age < 30");
    expect(result.rows!.map(r => r.id)).toEqual([2]);
  });

  it("WHERE with single JSON path less-than", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    engine.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    engine.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    const [r1] = engine.execute("SELECT id FROM t WHERE data.age < 30");
    expect(r1.rows!.map(r => r.id)).toEqual([2]);

    const [r2] = engine.execute("SELECT id FROM t WHERE data.age > 20");
    expect(r2.rows!.map(r => r.id).sort()).toEqual([1, 2]);
  });

  it("DELETE with multi-expression filter on JSON path", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    engine.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    engine.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);
    engine.execute(`INSERT INTO t (id, data) VALUES (3, '{"age": 15}')`);

    engine.execute("DELETE FROM t WHERE data.age > 20 AND data.age < 30");

    const [result] = engine.execute("SELECT id FROM t ORDER BY id");
    expect(result.rows!.map(r => r.id)).toEqual([1, 3]);
  });

  it("UPDATE with multi-expression filter on JSON path", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    engine.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    engine.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    engine.execute(`UPDATE t SET data = '{"age": 99}' WHERE data.age > 20 AND data.age < 30`);

    const [result] = engine.execute("SELECT id, data.age FROM t ORDER BY id");
    expect(result.rows![0]).toMatchObject({ id: 1 });
    expect(result.rows![1]).toMatchObject({ id: 2 });
  });

  it("CONCAT with JSON column produces JSON string", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    engine.execute(`INSERT INTO t (id, data) VALUES (1, '{"name": "Alice"}')`);

    const [result] = engine.execute("SELECT data.name || '!' AS greeting FROM t");
    expect(result.rows![0].greeting).toBe("Alice!");
  });

  it("CAST number/boolean to JSON preserves value", async () => {
    await createEngine();
    const [r1] = engine.execute("SELECT CAST(42 AS JSON) AS v");
    expect(r1.rows![0].v).toBe(42);

    const [r2] = engine.execute("SELECT CAST(TRUE AS JSON) AS v");
    expect(r2.rows![0].v).toBe(true);
  });
});

// ============================================================================
// BLOB type
// ============================================================================

describe("BLOB type", () => {
  it("INSERT and SELECT blob literal", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    engine.execute("INSERT INTO t (id, data) VALUES (1, x'DEADBEEF')");

    const [result] = engine.execute("SELECT id, data FROM t");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].data).toBeInstanceOf(Uint8Array);
    expect(result.rows![0].data).toEqual(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]));
  });

  it("CAST blob to TEXT returns hex", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    engine.execute("INSERT INTO t VALUES (1, x'CAFE')");

    const [result] = engine.execute("SELECT CAST(data AS TEXT) AS hex FROM t");
    expect(result.rows![0].hex).toBe("CAFE");
  });

  it("CAST text to BLOB", async () => {
    await createEngine();
    const [result] = engine.execute("SELECT CAST('FF00' AS BLOB) AS v");
    expect(result.rows![0].v).toBeInstanceOf(Uint8Array);
    expect(result.rows![0].v).toEqual(new Uint8Array([0xFF, 0x00]));
  });

  it("blob parameter support", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    engine.execute("INSERT INTO t VALUES (1, $1)", [new Uint8Array([0xAA, 0xBB])]);

    const [result] = engine.execute("SELECT data FROM t");
    expect(result.rows![0].data).toEqual(new Uint8Array([0xAA, 0xBB]));
  });

  it("blob comparison in WHERE", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    engine.execute("INSERT INTO t VALUES (1, x'AA')");
    engine.execute("INSERT INTO t VALUES (2, x'BB')");

    const [result] = engine.execute("SELECT id FROM t WHERE data = x'BB'");
    expect(result.rows!.map(r => r.id)).toEqual([2]);
  });

  it("ORDER BY blob column", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    engine.execute("INSERT INTO t VALUES (1, x'FF')");
    engine.execute("INSERT INTO t VALUES (2, x'00')");
    engine.execute("INSERT INTO t VALUES (3, x'AA')");

    const [result] = engine.execute("SELECT id FROM t ORDER BY data");
    expect(result.rows!.map(r => r.id)).toEqual([2, 3, 1]);
  });

  it("UPDATE blob column", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    engine.execute("INSERT INTO t VALUES (1, x'AA')");
    engine.execute("UPDATE t SET data = x'BB' WHERE id = 1");

    const [result] = engine.execute("SELECT data FROM t");
    expect(result.rows![0].data).toEqual(new Uint8Array([0xBB]));
  });

  it("NULL blob", async () => {
    await createEngine();
    engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    engine.execute("INSERT INTO t VALUES (1, NULL)");

    const [result] = engine.execute("SELECT data FROM t WHERE id = 1");
    expect(result.rows![0].data).toBeNull();
  });
});

import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MisuseError } from "../../errors.js";
import type { Value } from "../../values/index.js";
import { bindStatement } from "../../sql/binder/index.js";
import type * as BinderModule from "../../sql/binder/index.js";
import { Engine, PreparedStatement, type Session } from "../index.js";
import { openTestEngine } from "./test-engine.js";

// Binding is counted: a prepared statement binds again only for a new schema.
vi.mock("../../sql/binder/index.js", async (importOriginal) => {
  const binder: typeof BinderModule = await importOriginal();
  return { ...binder, bindStatement: vi.fn(binder.bindStatement) };
});

let engine: Engine;
let session: Session;
let seq = 0;

function newDatabaseName(): string {
  return `engine-test-${seq++}`;
}

async function createEngine(name?: string): Promise<Engine> {
  const databaseName = name ?? newDatabaseName();
  engine = await openTestEngine(databaseName);
  session = engine.connect();
  return engine;
}

beforeEach(() => {
  resetMockOPFS();
});

afterEach(async () => {
  await session?.close();
  await engine?.close();
});

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

describe("initialization", () => {
  it("Engine.create opens storage and returns an engine", async () => {
    const opened = await createEngine();
    expect(opened).toBeInstanceOf(Engine);
  });

  it("Engine.create loads catalog from storage", async () => {
    const name = newDatabaseName();
    const opened = await createEngine(name);
    const s = opened.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("INSERT INTO t (id) VALUES (42)");
    await s.close();
    await opened.close();

    engine = await openTestEngine(name);
    session = engine.connect();
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 42 }]);
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
    expect(result.objects()).toHaveLength(1);
  });

  it("INSERT persists immediately in autocommit", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 1, name: "a" }]);
  });

  it("error in INSERT rolls back catalog", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await expect(
      session.execute("INSERT INTO nonexistent (id) VALUES (1)"),
    ).rejects.toThrow();

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(0);
  });

  it("DDL persists in autocommit", async () => {
    const name = newDatabaseName();
    const opened = await createEngine(name);
    const s = opened.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.close();
    await opened.close();

    engine = await openTestEngine(name);
    session = engine.connect();
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(0);
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
    expect(result.objects()).toEqual([{ id: 1, name: "a" }]);
  });

  it("BEGIN + INSERT + ROLLBACK discards data", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await session.execute("ROLLBACK");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(0);
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
    expect(result.objects()).toEqual([{ id: 1, name: "a" }]);
  });

  it("COMMIT without BEGIN is a no-op", async () => {
    await createEngine();
    const [result] = await session.execute("COMMIT");
    expect(result.columns).toEqual([]);
    expect(session.inTransaction).toBe(false);
  });

  it("ROLLBACK without BEGIN is a no-op", async () => {
    await createEngine();
    const [result] = await session.execute("ROLLBACK");
    expect(result.changes).toBe(0);
  });

  it("nested BEGIN fails with MISUSE and leaves the transaction open", async () => {
    await createEngine();
    await session.execute("BEGIN");
    await expect(session.execute("BEGIN")).rejects.toThrow(MisuseError);
    expect(session.inTransaction).toBe(true);
    await session.execute("ROLLBACK");
  });
});

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

describe("DROP TABLE", () => {
  it("DROP TABLE removes table and persists", async () => {
    const name = newDatabaseName();
    const opened = await createEngine(name);
    const s = opened.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("INSERT INTO t (id) VALUES (1)");
    await s.execute("DROP TABLE t");
    await s.close();
    await opened.close();

    engine = await openTestEngine(name);
    session = engine.connect();
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
    expect(result.objects()).toHaveLength(1);
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
    expect(result.objects()).toHaveLength(3);
  });

  it("statement execution order is preserved", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");

    await session.execute(
      "INSERT INTO t (id, val) VALUES (1, 'first'); INSERT INTO t (id, val) VALUES (2, 'second'); INSERT INTO t (id, val) VALUES (3, 'third')",
    );

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(3);
    const ids = result
      .objects()
      .map((r) => r.id)
      .sort();
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
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0]).toHaveProperty("username");
    expect(result.objects()[0].username).toBe("alice");
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
    expect(result.objects()[0]).toEqual({ n: "alice", a: 30 });
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
    expect(result.objects()[0]).toHaveProperty("total");
    expect(result.objects()[0].total).toBeCloseTo(31.5);
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
    expect(result.objects()).toHaveLength(2);
    const names = result
      .objects()
      .map((r) => r.name)
      .sort();
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
    expect(result.objects().every((r: any) => r.age === 99)).toBe(true);
  });

  it("DELETE all rows without WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await session.execute("INSERT INTO t (id, name) VALUES (2, 'b')");

    await session.execute("DELETE FROM t");
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(0);
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

    expect(updateResult.changes).toBe(2);
    expect(after.objects()).toHaveLength(2);
    expect(
      after
        .objects()
        .map((r: any) => r.age)
        .sort(),
    ).toEqual([99, 99]);
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
    const alice = result.objects().find((r: any) => r.id === 1);
    expect(alice!.name).toBe("X");
    const bob = result.objects().find((r: any) => r.id === 2);
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
    expect(result.objects().map((r: any) => r.n)).toEqual([1, 2, 3, 4, 5]);
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe("error handling", () => {
  it("a failed statement in a transaction is rolled back alone", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (id) VALUES (1)");
    await expect(
      session.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).rejects.toMatchObject({ code: "SEMANTIC_ERROR" });
    expect(session.inTransaction).toBe(true);

    await session.execute("INSERT INTO t (id) VALUES (3)");
    await session.execute("COMMIT");
    expect(session.inTransaction).toBe(false);

    const [result] = await session.execute("SELECT id FROM t ORDER BY id");
    expect(result.rows).toEqual([[1], [3]]);
  });

  it("a caught UNIQUE violation leaves the transaction going on", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
    );

    await session.execute("BEGIN");
    await session.query("INSERT INTO users (email) VALUES ('a@x')");
    await expect(
      session.query("INSERT INTO users (email) VALUES ('a@x')"),
    ).rejects.toMatchObject({ code: "CONSTRAINT_VIOLATION" });
    await session.query("INSERT INTO users (email) VALUES ('b@x')");
    await session.execute("COMMIT");

    const [result] = await session.execute(
      "SELECT email FROM users ORDER BY email",
    );
    expect(result.rows).toEqual([["a@x"], ["b@x"]]);
  });

  it("a multi-row INSERT that fails at row 3 leaves rows 1 and 2 out", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    await session.execute("BEGIN");
    await session.query("INSERT INTO t VALUES (10, 'kept')");
    await expect(
      session.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (10, 'c')"),
    ).rejects.toMatchObject({ code: "CONSTRAINT_VIOLATION" });
    await session.execute("COMMIT");

    const [result] = await session.execute("SELECT id, v FROM t");
    expect(result.rows).toEqual([[10, "kept"]]);
  });

  it("a failed DDL statement leaves the transaction's schema as it was", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'x'), (2, 'x')");

    await session.execute("BEGIN");
    await session.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)");
    await expect(
      session.execute("CREATE UNIQUE INDEX t_v ON t (v)"),
    ).rejects.toMatchObject({ code: "CONSTRAINT_VIOLATION" });
    await session.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)");
    await session.execute("COMMIT");

    const schema = await session.schema();
    expect(schema.tables.map((table) => table.name)).toEqual(["t", "a", "b"]);
    expect(schema.tables[0].indexes).toEqual([]);
  });

  it("ROLLBACK after a failed statement discards the rest", async () => {
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
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0].id).toBe(10);
  });

  it("error in autocommit rolls back only that statement", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t (id) VALUES (1)");

    await expect(
      session.execute("INSERT INTO nonexistent (id) VALUES (2)"),
    ).rejects.toThrow();

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0].id).toBe(1);
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
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0].name).toBe("b");
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
    expect(result.objects()[0].name).toBe("reused");
  });

  it("PK index survives transaction rollback", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");

    await session.execute("BEGIN");
    await session.execute("INSERT INTO t VALUES (2)");
    await session.execute("ROLLBACK");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(1);

    await session.execute("INSERT INTO t VALUES (2)");
    const [result2] = await session.execute("SELECT * FROM t WHERE id = 2");
    expect(result2.objects()).toHaveLength(1);
  });

  it("DROP TABLE cleans up PK index", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("DROP TABLE t");

    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(1);
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
    expect(result.objects()).toEqual([
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
    expect(result.objects()).toEqual([{ id: 1, val: null }]);
  });

  it("new rows after ADD COLUMN get explicit value", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");

    await session.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    await session.execute("INSERT INTO t (id, val) VALUES (2, 42)");

    const [result] = await session.execute("SELECT id, val FROM t ORDER BY id");
    expect(result.objects()).toEqual([
      { id: 1, val: 99 },
      { id: 2, val: 42 },
    ]);
  });

  it("existing rows each get their own copy of a JSON DEFAULT", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("INSERT INTO t VALUES (2)");
    await session.execute(
      `ALTER TABLE t ADD COLUMN tags JSON DEFAULT '["new"]'`,
    );

    const select = "SELECT tags FROM t ORDER BY id";
    const [result] = await session.execute(select);
    const [first, second] = result.rows.map(([tags]) => tags);
    if (Array.isArray(first)) first.push("changed");
    expect(first).toEqual(["new", "changed"]);
    expect(second).toEqual(["new"]);

    const [later] = await session.execute(select);
    expect(later.rows).toEqual([[["new"]], [["new"]]]);
  });

  it("UPDATE works on rows with default-filled column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");

    await session.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    await session.execute("UPDATE t SET val = 50 WHERE id = 1");

    const [result] = await session.execute("SELECT val FROM t");
    expect(result.objects()[0].val).toBe(50);
  });

  it("DELETE works on rows with default-filled column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("INSERT INTO t VALUES (2)");

    await session.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    await session.execute("DELETE FROM t WHERE val = 99");

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(0);
  });

  it("ADD COLUMN defaults persist across reopen", async () => {
    const name = newDatabaseName();
    const opened = await createEngine(name);
    const s = opened.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("INSERT INTO t VALUES (1)");
    await s.execute("ALTER TABLE t ADD COLUMN val INTEGER DEFAULT 99");
    await s.close();
    await opened.close();

    engine = await openTestEngine(name);
    session = engine.connect();
    const [result] = await session.execute("SELECT id, val FROM t");
    expect(result.objects()).toEqual([{ id: 1, val: 99 }]);
  });
});

// ---------------------------------------------------------------------------
// CREATE INDEX — reserved names
// ---------------------------------------------------------------------------

describe("CREATE INDEX — reserved names", () => {
  it("allows names starting with __pk_, which mean nothing special", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER, name TEXT)");
    await session.execute("CREATE INDEX __pk_t ON t (name)");

    const schema = await session.schema();
    expect(schema.tables[0].indexes.map((index) => index.name)).toEqual([
      "__pk_t",
    ]);
  });

  it("rejects names starting with opfsql_", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER, name TEXT)");

    await expect(
      session.execute("CREATE INDEX opfsql_t ON t (name)"),
    ).rejects.toThrow(/reserved/);
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
    ).rejects.toThrow(/cannot drop column name of t: index idx_name/);
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
    expect(result.objects()).toHaveLength(0);
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
    expect(r1.objects()[0].v).toBe("o");

    const [r2] = await session.execute("SELECT SUBSTR(s, -2, 2) AS v FROM t");
    expect(r2.objects()[0].v).toBe("lo");
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
    expect(result.objects()).toHaveLength(2);
    const sorted = result
      .objects()
      .sort((a: any, b: any) => a.category.localeCompare(b.category));
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
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0]).toEqual({ cnt: 3, total: 60, avg_val: 20 });
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
    expect(desc.objects().map((r: any) => r.val)).toEqual([90, 80, 70]);
  });
});

// ---------------------------------------------------------------------------
// Parameterized queries
// ---------------------------------------------------------------------------

describe("parameterized queries", () => {
  it("execute() with $1 in INSERT", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.query("INSERT INTO t (id, name) VALUES ($1, $2)", [
      1,
      "alice",
    ]);

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 1, name: "alice" }]);
  });

  it("execute() with $1 in WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'alice')");
    await session.execute("INSERT INTO t VALUES (2, 'bob')");

    const result = await session.query("SELECT * FROM t WHERE id = $1", [1]);
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0]).toEqual({ id: 1, name: "alice" });
  });

  it("prepare() + run() executes multiple times with different params", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    const statement = await session.prepare(
      "INSERT INTO t (id, name) VALUES ($1, $2)",
    );
    expect(statement).toBeInstanceOf(PreparedStatement);

    await statement.query([1, "alice"]);
    await statement.query([2, "bob"]);
    await statement.query([3, "charlie"]);

    const [result] = await session.execute("SELECT * FROM t ORDER BY id");
    expect(result.objects()).toHaveLength(3);
    expect(result.objects()[0]).toEqual({ id: 1, name: "alice" });
    expect(result.objects()[1]).toEqual({ id: 2, name: "bob" });
    expect(result.objects()[2]).toEqual({ id: 3, name: "charlie" });
  });

  it("prepare() SELECT with $1 in WHERE returns correct row", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    await session.execute("INSERT INTO t VALUES (1, 100)");
    await session.execute("INSERT INTO t VALUES (2, 200)");
    await session.execute("INSERT INTO t VALUES (3, 300)");

    const statement = await session.prepare("SELECT val FROM t WHERE id = $1");
    const r1 = await statement.query([2]);
    expect(r1.objects()).toHaveLength(1);
    expect(r1.objects()[0].val).toBe(200);

    const r3 = await statement.query([3]);
    expect(r3.objects()[0].val).toBe(300);
  });

  it("prepare() requires exactly one statement", async () => {
    await createEngine();
    await expect(session.prepare("SELECT 1; SELECT 2")).rejects.toThrow(
      MisuseError,
    );
  });

  it("UPDATE with parameterized SET and WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'old')");

    await session.query("UPDATE t SET name = $1 WHERE id = $2", ["new", 1]);

    const [result] = await session.execute("SELECT name FROM t WHERE id = 1");
    expect(result.objects()[0].name).toBe("new");
  });

  it("DELETE with parameterized WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    await session.execute("INSERT INTO t VALUES (2)");

    await session.query("DELETE FROM t WHERE id = $1", [1]);

    const [result] = await session.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0].id).toBe(2);
  });
});

async function parameterTable(): Promise<void> {
  await createEngine();
  await session.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, m INTEGER, name TEXT)",
  );
  await session.execute(
    "INSERT INTO t VALUES (1, 9, 'nine'), (2, 10, '10'), (3, NULL, NULL)",
  );
}

describe("parameters: count and conversion", () => {
  it("fails with MISUSE for a missing parameter, wherever it is used", async () => {
    await parameterTable();
    for (const sql of [
      "SELECT id FROM t WHERE m = $1",
      "SELECT id FROM t WHERE m = $1 OR id = $1",
      "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE m = $2)",
      "SELECT id FROM t WHERE false AND m = $1",
      "UPDATE t SET name = $1 WHERE id = 99",
    ]) {
      await expect(session.query(sql, [])).rejects.toMatchObject({
        code: "MISUSE",
      });
    }
    await expect(
      session.query(
        "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE m = $2)",
        [1],
      ),
    ).rejects.toMatchObject({ code: "MISUSE" });
  });

  it("fails with MISUSE for a value that is not one, before running anything", async () => {
    await parameterTable();
    const prepared = await session.prepare(
      "INSERT INTO t (id, m) VALUES ($1, $2)",
    );
    // What a JavaScript caller can pass, whatever the types say.
    const notValues = [
      new Date(0),
      new Map(),
      10n,
      undefined,
      new Int8Array(1),
    ];
    for (const bad of notValues as unknown as Value[]) {
      await expect(
        session.query("INSERT INTO t (id, m) VALUES ($1, $2)", [50, bad]),
      ).rejects.toMatchObject({ code: "MISUSE", message: /^parameter \$2 / });
      await expect(prepared.query([51, bad])).rejects.toMatchObject({
        code: "MISUSE",
      });
    }
    const [count] = await session.execute(
      "SELECT count(*) FROM t WHERE id >= 50",
    );
    expect(count.rows).toEqual([[0]]);
  });

  it("copies parameters at the call: a BLOB changed while the call waits is stored as it was", async () => {
    await createEngine();
    await session.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, data BLOB)");
    await session.begin({ mode: "immediate" });
    const other = engine.connect();
    const blob = new Uint8Array([1, 2, 3]);
    // Waits for the lock the open transaction holds.
    const insert = other.query("INSERT INTO b VALUES (1, $1)", [blob]);
    blob.fill(0);
    await session.commit();
    await insert;
    const [stored] = await session.execute("SELECT data FROM b");
    expect(stored.rows).toEqual([[new Uint8Array([1, 2, 3])]]);
    await other.close();
  });

  it("fails with MISUSE for more parameters than a statement uses", async () => {
    // As in SQLite, where binding past the last parameter is SQLITE_RANGE.
    await parameterTable();
    await expect(
      session.query("SELECT id FROM t WHERE m = $1", [9, "unused"]),
    ).rejects.toMatchObject({ code: "MISUSE" });
  });

  it("converts a text parameter compared with a number column", async () => {
    await parameterTable();
    const byScan = await session.query("SELECT id FROM t WHERE m = $1", ["10"]);
    expect(byScan.objects()).toEqual([{ id: 2 }]);
    const byKey = await session.query("SELECT id FROM t WHERE id = $1", ["2"]);
    expect(byKey.objects()).toEqual([{ id: 2 }]);
    const none = await session.query("SELECT id FROM t WHERE m = $1", ["ten"]);
    expect(none.objects()).toEqual([]);
  });

  it("answers IN the same way for short and long lists", async () => {
    await parameterTable();
    const result = await session.query(
      "SELECT $1 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AS short, $1 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) AS long, $1 IN (m, 7) AS columns FROM t WHERE id = 2",
      ["10"],
    );
    expect(result.objects()).toEqual([
      { short: true, long: true, columns: true },
    ]);
  });

  it("converts a number parameter compared with a text column", async () => {
    await parameterTable();
    const result = await session.query(
      "SELECT id FROM t WHERE name = $1",
      [10],
    );
    expect(result.objects()).toEqual([{ id: 2 }]);
  });

  it("turns a NaN parameter into NULL", async () => {
    await parameterTable();
    const result = await session.query("SELECT $1 IS NULL AS missing", [NaN]);
    expect(result.objects()).toEqual([{ missing: true }]);
  });

  it("divides parameters by their values: integers truncate", async () => {
    await parameterTable();
    const result = await session.query(
      "SELECT $1 / $2 AS q, $3 / $2 AS r",
      [7, 2, 7.5],
    );
    expect(result.objects()).toEqual([{ q: 3, r: 3.75 }]);
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
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0]).toMatchObject({ id: 1 });
  });

  it("WHERE filter with JSON path range comparison", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    await session.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    const [result] = await session.execute(
      "SELECT id FROM t WHERE data.age > 20 AND data.age < 30",
    );
    expect(result.objects().map((r) => r.id)).toEqual([2]);
  });

  it("WHERE with single JSON path less-than", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)");
    await session.execute(`INSERT INTO t (id, data) VALUES (1, '{"age": 30}')`);
    await session.execute(`INSERT INTO t (id, data) VALUES (2, '{"age": 25}')`);

    const [r1] = await session.execute("SELECT id FROM t WHERE data.age < 30");
    expect(r1.objects().map((r) => r.id)).toEqual([2]);

    const [r2] = await session.execute("SELECT id FROM t WHERE data.age > 20");
    expect(
      r2
        .objects()
        .map((r) => r.id)
        .sort(),
    ).toEqual([1, 2]);
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
    expect(result.objects().map((r) => r.id)).toEqual([1, 3]);
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
    expect(result.objects()[0]).toMatchObject({ id: 1 });
    expect(result.objects()[1]).toMatchObject({ id: 2 });
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
    expect(result.objects()[0].greeting).toBe("Alice!");
  });

  it("CAST number/boolean to JSON preserves value", async () => {
    await createEngine();
    const [r1] = await session.execute("SELECT CAST(42 AS JSON) AS v");
    expect(r1.objects()[0].v).toBe(42);

    const [r2] = await session.execute("SELECT CAST(TRUE AS JSON) AS v");
    expect(r2.objects()[0].v).toBe(true);
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
    expect(result.objects()).toHaveLength(1);
    expect(result.objects()[0].data).toBeInstanceOf(Uint8Array);
    expect(result.objects()[0].data).toEqual(
      new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
    );
  });

  it("CAST blob to TEXT returns lowercase hex", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'CAFE')");

    const [result] = await session.execute(
      "SELECT CAST(data AS TEXT) AS hex FROM t",
    );
    expect(result.objects()[0].hex).toBe("cafe");
  });

  it("CAST text to BLOB", async () => {
    await createEngine();
    const [result] = await session.execute("SELECT CAST('FF00' AS BLOB) AS v");
    expect(result.objects()[0].v).toBeInstanceOf(Uint8Array);
    expect(result.objects()[0].v).toEqual(new Uint8Array([0xff, 0x00]));
  });

  it("blob parameter support", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.query("INSERT INTO t VALUES (1, $1)", [
      new Uint8Array([0xaa, 0xbb]),
    ]);

    const [result] = await session.execute("SELECT data FROM t");
    expect(result.objects()[0].data).toEqual(new Uint8Array([0xaa, 0xbb]));
  });

  it("blob comparison in WHERE", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'AA')");
    await session.execute("INSERT INTO t VALUES (2, x'BB')");

    const [result] = await session.execute(
      "SELECT id FROM t WHERE data = x'BB'",
    );
    expect(result.objects().map((r) => r.id)).toEqual([2]);
  });

  it("ORDER BY blob column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'FF')");
    await session.execute("INSERT INTO t VALUES (2, x'00')");
    await session.execute("INSERT INTO t VALUES (3, x'AA')");

    const [result] = await session.execute("SELECT id FROM t ORDER BY data");
    expect(result.objects().map((r) => r.id)).toEqual([2, 3, 1]);
  });

  it("UPDATE blob column", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, x'AA')");
    await session.execute("UPDATE t SET data = x'BB' WHERE id = 1");

    const [result] = await session.execute("SELECT data FROM t");
    expect(result.objects()[0].data).toEqual(new Uint8Array([0xbb]));
  });

  it("NULL blob", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)");
    await session.execute("INSERT INTO t VALUES (1, NULL)");

    const [result] = await session.execute("SELECT data FROM t WHERE id = 1");
    expect(result.objects()[0].data).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Schema changes and prepared statements
// ---------------------------------------------------------------------------

describe("schema changes", () => {
  it("DDL commits persist across reopen, one after another", async () => {
    const name = newDatabaseName();
    const opened = await createEngine(name);
    const s = opened.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await s.execute("CREATE INDEX idx ON t (id)");
    await s.execute("BEGIN");
    await s.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)");
    await s.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)");
    await s.execute("COMMIT");
    await s.close();
    await opened.close();

    engine = await openTestEngine(name);
    session = engine.connect();
    const schema = await session.schema();
    expect(schema.tables.map((table) => table.name)).toEqual(["t", "a", "b"]);
    expect(schema.tables[0].indexes.map((index) => index.name)).toEqual([
      "idx",
    ]);
  });

  it("DDL rolled back leaves the schema as it was", async () => {
    await createEngine();
    const before = await session.schema();

    await session.execute("BEGIN");
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const during = await session.schema();
    expect(during.tables).toHaveLength(1);
    await session.execute("ROLLBACK");

    expect(await session.schema()).toEqual(before);
  });
});

describe("schema()", () => {
  it("hands out copies of BLOB and JSON defaults, which the caller may change", async () => {
    await createEngine();
    await session.execute(
      `CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB DEFAULT x'0102', tags JSON DEFAULT '["a"]')`,
    );
    const first = await session.schema();
    const [, data, tags] = first.tables[0].columns;
    (data.defaultValue as Uint8Array).fill(0);
    (tags.defaultValue as string[]).push("changed");

    const second = await session.schema();
    expect(
      second.tables[0].columns.map((column) => column.defaultValue),
    ).toEqual([null, new Uint8Array([1, 2]), ["a"]]);
    await session.execute("INSERT INTO t (id) VALUES (1)");
    const [row] = await session.execute("SELECT data, tags FROM t");
    expect(row.rows).toEqual([[new Uint8Array([1, 2]), ["a"]]]);
  });

  it("describes columns from the model, leaving out opfsql's own tables", async () => {
    await createEngine();
    await session.execute(`
      CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL DEFAULT 'x');
      CREATE TABLE b (code TEXT PRIMARY KEY, tag TEXT UNIQUE, v REAL);
    `);
    const schema = await session.schema();
    expect(schema.tables.map((table) => table.name)).toEqual(["a", "b"]);
    expect(schema.tables[0].columns).toEqual([
      {
        name: "id",
        type: "INTEGER",
        nullable: false,
        primaryKey: true,
        unique: false,
        autoIncrement: true,
        defaultValue: null,
      },
      {
        name: "name",
        type: "TEXT",
        nullable: false,
        primaryKey: false,
        unique: false,
        autoIncrement: false,
        defaultValue: "x",
      },
    ]);
    expect(schema.tables[1].columns.map((column) => column.primaryKey)).toEqual(
      [true, false, false],
    );
    expect(schema.tables[1].columns.map((column) => column.unique)).toEqual([
      false,
      true,
      false,
    ]);
    // The columns show the constraints' indexes.
    expect(schema.tables[1].indexes).toEqual([]);
  });

  it("shows a primary key over several columns on each of them, and lists CREATE INDEX's indexes", async () => {
    await createEngine();
    await session.execute(
      "CREATE TABLE t (a INTEGER, b TEXT, c TEXT, PRIMARY KEY (a, b)); CREATE UNIQUE INDEX t_c ON t (lower(c), a)",
    );
    const { tables } = await session.schema();
    const [table] = tables;
    expect(table.columns.map((column) => column.primaryKey)).toEqual([
      true,
      true,
      false,
    ]);
    expect(table.columns.map((column) => column.nullable)).toEqual([
      false,
      false,
      true,
    ]);
    expect(table.indexes).toEqual([
      { name: "t_c", unique: true, expressions: ["lower(c)", "a"] },
    ]);
  });
});

describe("prepared statements", () => {
  it("bind again, transparently, when the schema changes", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await session.execute("INSERT INTO t VALUES (1, 'alice')");

    const statement = await session.prepare("SELECT * FROM t WHERE id = $1");
    expect(statement.columns).toEqual(["id", "name"]);

    await session.execute("ALTER TABLE t ADD COLUMN age INTEGER DEFAULT 0");

    const result = await statement.query([1]);
    expect(result.objects()).toEqual([{ id: 1, name: "alice", age: 0 }]);
    expect(statement.columns).toEqual(["id", "name", "age"]);
  });

  it("bind again after DDL in another session of the engine", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    const s2 = engine.connect();
    const statement = await s2.prepare("SELECT * FROM t");

    await session.execute("ALTER TABLE t ADD COLUMN v INTEGER DEFAULT 0");
    await session.execute("INSERT INTO t (id) VALUES (1)");

    const result = await statement.query();
    expect(result.objects()).toEqual([{ id: 1, v: 0 }]);
    await s2.close();
  });

  it("bind once while the schema stays the same, whatever else commits", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    await session.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)");
    const statement = await session.prepare("SELECT v FROM t WHERE id = $1");
    const binds = vi.mocked(bindStatement);
    binds.mockClear();

    for (let id = 1; id <= 5; id++) {
      await session.query("INSERT INTO t VALUES ($1, 'x')", [id]);
      await statement.query([id]);
    }
    const insertBinds = 5;
    expect(binds).toHaveBeenCalledTimes(insertBinds);

    // DDL on an unrelated table changes the schema: one more bind, then none.
    await session.execute("DROP TABLE other");
    binds.mockClear();
    await statement.query([1]);
    await statement.query([2]);
    expect(binds).toHaveBeenCalledTimes(1);
  });

  it("run against the schema of the transaction they run in, its own DDL included", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.execute("INSERT INTO t VALUES (1)");
    const statement = await session.prepare("SELECT * FROM t");

    await session.begin();
    await session.execute("ALTER TABLE t ADD COLUMN v TEXT DEFAULT 'x'");
    const inside = await statement.query();
    expect(inside.columns).toEqual(["id", "v"]);
    await session.rollback();

    const after = await statement.query();
    expect(after.columns).toEqual(["id"]);
    expect(after.rows).toEqual([[1]]);
  });

  it("fail with the binding error when the schema change broke them", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const statement = await session.prepare("SELECT * FROM t");
    await session.execute("DROP TABLE t");
    await expect(statement.query()).rejects.toMatchObject({
      code: "SEMANTIC_ERROR",
    });
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, w TEXT)");
    const rebound = await statement.query();
    expect(rebound.columns).toEqual(["id", "w"]);
  });

  it("fail at prepare on unknown tables", async () => {
    await createEngine();
    await expect(session.prepare("SELECT * FROM nope")).rejects.toMatchObject({
      code: "SEMANTIC_ERROR",
    });
  });

  it("fail with MISUSE once closed", async () => {
    await createEngine();
    const statement = await session.prepare("SELECT 1");
    statement.close();
    await expect(statement.query()).rejects.toThrow(MisuseError);
  });
});

// ---------------------------------------------------------------------------
// Use after close
// ---------------------------------------------------------------------------

describe("use after close", () => {
  it("execute() throws after session is closed", async () => {
    await createEngine();
    await session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await session.close();

    await expect(session.execute("SELECT * FROM t")).rejects.toThrow(
      MisuseError,
    );
  });
});

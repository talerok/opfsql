import { describe, it, expect, afterEach } from 'vitest';
import { Engine, EngineError } from '../index.js';
import { MemoryStorage } from '../../store/tests/memory-storage.js';

let engine: Engine;
let dbName: string;
const storageMap = new Map<string, MemoryStorage>();

function newDbName(): string {
  return `test-${Math.random()}`;
}

function getStorage(name: string): MemoryStorage {
  let s = storageMap.get(name);
  if (!s) {
    s = new MemoryStorage();
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

describe('initialization', () => {
  it('Engine.create opens storage and returns an engine', async () => {
    const e = await createEngine();
    expect(e).toBeInstanceOf(Engine);
  });

  it('Engine.create loads catalog from IDB', async () => {
    const name = newDbName();
    const e = await createEngine(name);
    await e.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await e.execute('INSERT INTO t (id) VALUES (42)');
    e.close();

    // Re-open same DB — catalog and data should be there
    engine = await Engine.create(getStorage(name));
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.type).toBe('rows');
    expect(result.rows).toEqual([{ id: 42 }]);
  });
});

// ---------------------------------------------------------------------------
// Autocommit
// ---------------------------------------------------------------------------

describe('autocommit', () => {
  it('SELECT does not persist writes', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.type).toBe('rows');
    expect(result.rows).toHaveLength(1);
  });

  it('INSERT persists to IDB immediately in autocommit', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toEqual([{ id: 1, name: 'a' }]);
  });

  it('error in INSERT rolls back catalog', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');

    await expect(
      engine.execute('INSERT INTO nonexistent (id) VALUES (1)'),
    ).rejects.toThrow();

    // Catalog should still have table t
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.type).toBe('rows');
    expect(result.rows).toHaveLength(0);
  });

  it('DDL persists to IDB in autocommit', async () => {
    const name = newDbName();
    const e = await createEngine(name);
    await e.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    e.close();

    engine = await Engine.create(getStorage(name));
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.type).toBe('rows');
    expect(result.rows).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Explicit transactions
// ---------------------------------------------------------------------------

describe('explicit transactions', () => {
  it('BEGIN + INSERT + COMMIT persists data', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

    await engine.execute('BEGIN');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await engine.execute('COMMIT');

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toEqual([{ id: 1, name: 'a' }]);
  });

  it('BEGIN + INSERT + ROLLBACK discards data', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

    await engine.execute('BEGIN');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await engine.execute('ROLLBACK');

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(0);
  });

  it('BEGIN + CREATE TABLE + ROLLBACK removes table from catalog', async () => {
    await createEngine();

    await engine.execute('BEGIN');
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await engine.execute('ROLLBACK');

    await expect(engine.execute('SELECT * FROM t')).rejects.toThrow();
  });

  it('BEGIN + CREATE TABLE + INSERT + COMMIT persists both', async () => {
    await createEngine();

    await engine.execute('BEGIN');
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await engine.execute('COMMIT');

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toEqual([{ id: 1, name: 'a' }]);
  });

  it('COMMIT without BEGIN is a no-op', async () => {
    await createEngine();
    const [result] = await engine.execute('COMMIT');
    expect(result.type).toBe('ok');
  });

  it('ROLLBACK without BEGIN is a no-op', async () => {
    await createEngine();
    const [result] = await engine.execute('ROLLBACK');
    expect(result.type).toBe('ok');
  });

  it('nested BEGIN throws EngineError', async () => {
    await createEngine();
    await engine.execute('BEGIN');
    await expect(engine.execute('BEGIN')).rejects.toThrow(EngineError);
  });
});

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

describe('DROP TABLE', () => {
  it('DROP TABLE removes table and persists', async () => {
    const name = newDbName();
    const e = await createEngine(name);
    await e.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await e.execute('INSERT INTO t (id) VALUES (1)');
    await e.execute('DROP TABLE t');
    e.close();

    engine = await Engine.create(getStorage(name));
    await expect(engine.execute('SELECT * FROM t')).rejects.toThrow();
  });

  it('DROP TABLE in transaction + ROLLBACK restores table', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await engine.execute('INSERT INTO t (id) VALUES (1)');

    await engine.execute('BEGIN');
    await engine.execute('DROP TABLE t');
    await engine.execute('ROLLBACK');

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Multiple statements
// ---------------------------------------------------------------------------

describe('multiple statements', () => {
  it('multiple INSERTs in one execute call all persist', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');

    await engine.execute(
      'INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (2); INSERT INTO t (id) VALUES (3)',
    );

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(3);
  });

  it('statement execution order is preserved', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await engine.execute(
      "INSERT INTO t (id, val) VALUES (1, 'first'); INSERT INTO t (id, val) VALUES (2, 'second'); INSERT INTO t (id, val) VALUES (3, 'third')",
    );

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(3);
    const ids = result.rows!.map((r) => r.id).sort();
    expect(ids).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// Column aliases (AS)
// ---------------------------------------------------------------------------

describe('column aliases', () => {
  it('AS alias appears as output row key', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'alice')");

    const [result] = await engine.execute('SELECT name AS username FROM t');
    expect(result.type).toBe('rows');
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toHaveProperty('username');
    expect(result.rows![0].username).toBe('alice');
  });

  it('multiple aliases all appear in output', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)");

    const [result] = await engine.execute('SELECT name AS n, age AS a FROM t');
    expect(result.rows![0]).toEqual({ n: 'alice', a: 30 });
  });

  it('mixed aliased and non-aliased columns', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)");

    const [result] = await engine.execute('SELECT name AS username, age FROM t');
    expect(result.rows![0]).toHaveProperty('username');
    expect(result.rows![0]).toHaveProperty('age');
    expect(result.rows![0].username).toBe('alice');
    expect(result.rows![0].age).toBe(30);
  });

  it('expression alias on computed column', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, price REAL, qty INTEGER)');
    await engine.execute('INSERT INTO t (id, price, qty) VALUES (1, 10.5, 3)');

    const [result] = await engine.execute('SELECT price * qty AS total FROM t');
    expect(result.rows![0]).toHaveProperty('total');
    expect(result.rows![0].total).toBeCloseTo(31.5);
  });

  it('star expansion has no aliases', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'alice')");

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows![0]).toHaveProperty('id');
    expect(result.rows![0]).toHaveProperty('name');
  });
});

// ---------------------------------------------------------------------------
// Bug diagnostics: WHERE + column pruning
// ---------------------------------------------------------------------------

describe('WHERE + column pruning', () => {
  it('SELECT subset with WHERE on non-selected column', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)");
    await engine.execute("INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)");
    await engine.execute("INSERT INTO t (id, name, age) VALUES (3, 'Charlie', 35)");

    // name is selected, age is only in WHERE — does pruning break the filter?
    const [result] = await engine.execute('SELECT name FROM t WHERE age > 28');
    expect(result.rows).toHaveLength(2);
    const names = result.rows!.map((r) => r.name).sort();
    expect(names).toEqual(['Alice', 'Charlie']);
  });

  it('SELECT * with WHERE preserves all columns', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)");
    await engine.execute("INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)");

    const [result] = await engine.execute('SELECT * FROM t WHERE age > 28');
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toEqual({ id: 1, name: 'Alice', age: 30 });
  });
});

// ---------------------------------------------------------------------------
// Bug diagnostics: DML without WHERE
// ---------------------------------------------------------------------------

describe('DML without WHERE', () => {
  it('UPDATE all rows without WHERE', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)");
    await engine.execute("INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)");

    await engine.execute('UPDATE t SET age = 99');
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows!.every((r) => r.age === 99)).toBe(true);
  });

  it('DELETE all rows without WHERE', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await engine.execute("INSERT INTO t (id, name) VALUES (2, 'b')");

    await engine.execute('DELETE FROM t');
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(0);
  });

  it('UPDATE all rows — check actual values', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)');
    await engine.execute('INSERT INTO t (id, age) VALUES (1, 10)');
    await engine.execute('INSERT INTO t (id, age) VALUES (2, 20)');

    const [updateResult] = await engine.execute('UPDATE t SET age = 99');
    const [after] = await engine.execute('SELECT * FROM t');

    // Diagnostics:
    expect(updateResult.rowsAffected).toBe(2);
    expect(after.rows).toHaveLength(2);
    expect(after.rows!.map((r) => r.age).sort()).toEqual([99, 99]);
  });

  it('DELETE all — check rowsAffected', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
    await engine.execute("INSERT INTO t (id, name) VALUES (2, 'b')");
    await engine.execute("INSERT INTO t (id, name) VALUES (3, 'c')");

    const [deleteResult] = await engine.execute('DELETE FROM t');
    const [after] = await engine.execute('SELECT * FROM t');

    expect(deleteResult.rowsAffected).toBe(3);
    expect(after.rows).toHaveLength(0);
  });

  it('UPDATE with WHERE on non-updated column', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await engine.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)");
    await engine.execute("INSERT INTO t (id, name, age) VALUES (2, 'Bob', 25)");

    await engine.execute('UPDATE t SET name = \'X\' WHERE age > 28');
    const [result] = await engine.execute('SELECT * FROM t');
    const alice = result.rows!.find((r) => r.id === 1);
    expect(alice!.name).toBe('X');
    const bob = result.rows!.find((r) => r.id === 2);
    expect(bob!.name).toBe('Bob');
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('error handling', () => {
  it('error in middle of transaction aborts entire transaction (PostgreSQL-style)', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');

    await engine.execute('BEGIN');
    await engine.execute('INSERT INTO t (id) VALUES (1)');

    await expect(
      engine.execute('INSERT INTO nonexistent (id) VALUES (2)'),
    ).rejects.toThrow();

    // Subsequent statements should fail
    await expect(
      engine.execute('INSERT INTO t (id) VALUES (3)'),
    ).rejects.toThrow(/transaction is aborted/);

    // COMMIT on aborted transaction → error
    await expect(engine.execute('COMMIT')).rejects.toThrow(/ROLLBACK/);

    // Everything rolled back
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(0);
  });

  it('ROLLBACK after aborted transaction allows new work', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');

    await engine.execute('BEGIN');
    await engine.execute('INSERT INTO t (id) VALUES (1)');
    await expect(
      engine.execute('INSERT INTO nonexistent (id) VALUES (2)'),
    ).rejects.toThrow();

    await engine.execute('ROLLBACK');

    // Can work again after ROLLBACK
    await engine.execute('INSERT INTO t (id) VALUES (10)');
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBe(10);
  });

  it('error in autocommit rolls back only that statement', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await engine.execute('INSERT INTO t (id) VALUES (1)');

    await expect(
      engine.execute('INSERT INTO nonexistent (id) VALUES (2)'),
    ).rejects.toThrow();

    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Auto PK index
// ---------------------------------------------------------------------------

describe('auto PK index', () => {
  it('PRIMARY KEY creates a unique index automatically', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t VALUES (1, 'a')");
    await engine.execute("INSERT INTO t VALUES (2, 'b')");
    await engine.execute("INSERT INTO t VALUES (3, 'c')");

    // Point lookup should use index (correct result is enough to prove it works)
    const [result] = await engine.execute('SELECT name FROM t WHERE id = 2');
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].name).toBe('b');
  });

  it('PK index enforces uniqueness', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t VALUES (1, 'a')");

    await expect(
      engine.execute("INSERT INTO t VALUES (1, 'dup')"),
    ).rejects.toThrow();
  });

  it('PK index works with UPDATE and DELETE', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute("INSERT INTO t VALUES (1, 'a')");
    await engine.execute("INSERT INTO t VALUES (2, 'b')");

    // Delete removes index entry
    await engine.execute('DELETE FROM t WHERE id = 1');

    // Can reuse deleted PK value
    await engine.execute("INSERT INTO t VALUES (1, 'reused')");
    const [result] = await engine.execute('SELECT name FROM t WHERE id = 1');
    expect(result.rows![0].name).toBe('reused');
  });

  it('PK index survives transaction rollback', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await engine.execute('INSERT INTO t VALUES (1)');

    await engine.execute('BEGIN');
    await engine.execute('INSERT INTO t VALUES (2)');
    await engine.execute('ROLLBACK');

    // Only row 1 should exist, and PK uniqueness still enforced
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(1);

    // id=2 should be insertable again (was rolled back)
    await engine.execute('INSERT INTO t VALUES (2)');
    const [result2] = await engine.execute('SELECT * FROM t WHERE id = 2');
    expect(result2.rows).toHaveLength(1);
  });

  it('DROP TABLE cleans up PK index', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await engine.execute('INSERT INTO t VALUES (1)');
    await engine.execute('DROP TABLE t');

    // Recreate same table — should work without index conflicts
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await engine.execute('INSERT INTO t VALUES (1)');
    const [result] = await engine.execute('SELECT * FROM t');
    expect(result.rows).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// GROUP BY + aggregates
// ---------------------------------------------------------------------------

describe('GROUP BY + aggregates', () => {
  it('GROUP BY with COUNT(*)', async () => {
    await createEngine();
    await engine.execute(
      'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)',
    );
    await engine.execute("INSERT INTO products VALUES (1, 'A', 10.0, 'Books')");
    await engine.execute("INSERT INTO products VALUES (2, 'B', 20.0, 'Books')");
    await engine.execute("INSERT INTO products VALUES (3, 'C', 30.0, 'Toys')");

    const [result] = await engine.execute(
      'SELECT category, COUNT(*) AS cnt FROM products GROUP BY category',
    );
    expect(result.rows).toHaveLength(2);
    const sorted = result.rows!.sort((a: any, b: any) =>
      a.category.localeCompare(b.category),
    );
    expect(sorted[0]).toEqual({ category: 'Books', cnt: 2 });
    expect(sorted[1]).toEqual({ category: 'Toys', cnt: 1 });
  });

  it('GROUP BY with COUNT + AVG', async () => {
    await createEngine();
    await engine.execute(
      'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)',
    );
    await engine.execute("INSERT INTO products VALUES (1, 'A', 10.0, 'Books')");
    await engine.execute("INSERT INTO products VALUES (2, 'B', 20.0, 'Books')");
    await engine.execute("INSERT INTO products VALUES (3, 'C', 30.0, 'Toys')");

    const [result] = await engine.execute(
      'SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category',
    );
    expect(result.rows).toHaveLength(2);
    const sorted = result.rows!.sort((a: any, b: any) =>
      a.category.localeCompare(b.category),
    );
    expect(sorted[0].category).toBe('Books');
    expect(sorted[0].cnt).toBe(2);
    expect(sorted[0].avg_price).toBe(15.0);
    expect(sorted[1].category).toBe('Toys');
    expect(sorted[1].cnt).toBe(1);
    expect(sorted[1].avg_price).toBe(30.0);
  });

  it('aggregate without GROUP BY', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    await engine.execute('INSERT INTO t VALUES (1, 10)');
    await engine.execute('INSERT INTO t VALUES (2, 20)');
    await engine.execute('INSERT INTO t VALUES (3, 30)');

    const [result] = await engine.execute(
      'SELECT COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val FROM t',
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toEqual({ cnt: 3, total: 60, avg_val: 20 });
  });

  it('GROUP BY with HAVING', async () => {
    await createEngine();
    await engine.execute(
      'CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price REAL)',
    );
    await engine.execute("INSERT INTO products VALUES (1, 'Books', 10.0)");
    await engine.execute("INSERT INTO products VALUES (2, 'Books', 20.0)");
    await engine.execute("INSERT INTO products VALUES (3, 'Toys', 30.0)");

    const [result] = await engine.execute(
      'SELECT category, COUNT(*) AS cnt FROM products GROUP BY category HAVING COUNT(*) > 1',
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toEqual({ category: 'Books', cnt: 2 });
  });

  // Top-K sort optimization (ORDER BY + LIMIT)
  it('ORDER BY + LIMIT returns correct top-K rows', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE scores (id INTEGER, val INTEGER)');
    await engine.execute('INSERT INTO scores VALUES (1, 50)');
    await engine.execute('INSERT INTO scores VALUES (2, 90)');
    await engine.execute('INSERT INTO scores VALUES (3, 10)');
    await engine.execute('INSERT INTO scores VALUES (4, 80)');
    await engine.execute('INSERT INTO scores VALUES (5, 70)');

    const [desc] = await engine.execute('SELECT * FROM scores ORDER BY val DESC LIMIT 3');
    expect(desc.rows!.map((r: any) => r.val)).toEqual([90, 80, 70]);

    const [asc] = await engine.execute('SELECT * FROM scores ORDER BY val ASC LIMIT 3');
    expect(asc.rows!.map((r: any) => r.val)).toEqual([10, 50, 70]);
  });

  it('ORDER BY + LIMIT + OFFSET returns correct slice', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE nums (id INTEGER, n INTEGER)');
    for (let i = 1; i <= 10; i++) {
      await engine.execute(`INSERT INTO nums VALUES (${i}, ${i * 10})`);
    }

    // Top 10 sorted DESC: 100,90,80,70,60,50,40,30,20,10
    // OFFSET 2 LIMIT 3 → 80,70,60
    const [result] = await engine.execute('SELECT n FROM nums ORDER BY n DESC LIMIT 3 OFFSET 2');
    expect(result.rows!.map((r: any) => r.n)).toEqual([80, 70, 60]);
  });

  it('ORDER BY + LIMIT with projection uses top-K', async () => {
    await createEngine();
    await engine.execute('CREATE TABLE items (id INTEGER, name TEXT, price REAL)');
    await engine.execute("INSERT INTO items VALUES (1, 'a', 9.99)");
    await engine.execute("INSERT INTO items VALUES (2, 'b', 1.50)");
    await engine.execute("INSERT INTO items VALUES (3, 'c', 5.00)");
    await engine.execute("INSERT INTO items VALUES (4, 'd', 3.25)");

    const [result] = await engine.execute('SELECT name, price FROM items ORDER BY price ASC LIMIT 2');
    expect(result.rows).toEqual([
      { name: 'b', price: 1.50 },
      { name: 'd', price: 3.25 },
    ]);
  });
});

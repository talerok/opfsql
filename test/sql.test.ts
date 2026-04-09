import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Engine } from '../src/index.js';
import { MemoryStorage } from '../src/store/tests/memory-storage.js';

let engine: Engine;

beforeEach(async () => {
  engine = await Engine.create(new MemoryStorage());
});

afterEach(() => {
  engine?.close();
});

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

describe('CREATE TABLE', () => {
  it('creates a table and allows selecting from it', async () => {
    await engine.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    const [result] = await engine.execute('SELECT * FROM users');
    expect(result.type).toBe('rows');
    expect(result.rows).toEqual([]);
  });

  it('CREATE TABLE IF NOT EXISTS does not error on duplicate', async () => {
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await expect(
      engine.execute('CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)'),
    ).resolves.toBeDefined();
  });

  it('CREATE TABLE with duplicate name throws', async () => {
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await expect(
      engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)'),
    ).rejects.toThrow();
  });
});

// ---------------------------------------------------------------------------
// INSERT + SELECT
// ---------------------------------------------------------------------------

describe('INSERT + SELECT', () => {
  beforeEach(async () => {
    await engine.execute(
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)',
    );
  });

  it('inserts a single row and selects it back', async () => {
    await engine.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
    const [result] = await engine.execute('SELECT * FROM users');
    expect(result.rows).toEqual([{ id: 1, name: 'Alice', age: 30 }]);
  });

  it('inserts multiple rows', async () => {
    await engine.execute(`
      INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
      INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
      INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);
    `);
    const [result] = await engine.execute('SELECT * FROM users');
    expect(result.rows).toHaveLength(3);
  });

  it('selects specific columns', async () => {
    await engine.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
    const [result] = await engine.execute('SELECT name, age FROM users');
    expect(result.rows).toEqual([{ name: 'Alice', age: 30 }]);
  });

  it('returns rowsAffected for INSERT', async () => {
    const [result] = await engine.execute(
      "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
    );
    expect(result.type).toBe('ok');
    expect(result.rowsAffected).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// WHERE
// ---------------------------------------------------------------------------

describe('WHERE clause', () => {
  beforeEach(async () => {
    await engine.execute(
      'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, active BOOLEAN)',
    );
    await engine.execute(`
      INSERT INTO products (id, name, price, active) VALUES (1, 'Apple', 100, true);
      INSERT INTO products (id, name, price, active) VALUES (2, 'Banana', 50, true);
      INSERT INTO products (id, name, price, active) VALUES (3, 'Cherry', 200, false);
      INSERT INTO products (id, name, price, active) VALUES (4, 'Date', 150, true);
    `);
  });

  it('filters with equality', async () => {
    const [result] = await engine.execute("SELECT * FROM products WHERE name = 'Apple'");
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].name).toBe('Apple');
  });

  it('filters with comparison operators', async () => {
    const [result] = await engine.execute('SELECT * FROM products WHERE price > 100');
    expect(result.rows).toHaveLength(2);
    const names = result.rows!.map((r) => r.name).sort();
    expect(names).toEqual(['Cherry', 'Date']);
  });

  it('filters with AND', async () => {
    const [result] = await engine.execute(
      'SELECT * FROM products WHERE price > 50 AND active = true',
    );
    expect(result.rows).toHaveLength(2);
    const names = result.rows!.map((r) => r.name).sort();
    expect(names).toEqual(['Apple', 'Date']);
  });

  it('filters with OR', async () => {
    const [result] = await engine.execute(
      "SELECT * FROM products WHERE name = 'Apple' OR name = 'Cherry'",
    );
    expect(result.rows).toHaveLength(2);
  });

  it('filters with BETWEEN', async () => {
    const [result] = await engine.execute(
      'SELECT * FROM products WHERE price BETWEEN 50 AND 150',
    );
    expect(result.rows).toHaveLength(3);
  });
});

// ---------------------------------------------------------------------------
// ORDER BY + LIMIT
// ---------------------------------------------------------------------------

describe('ORDER BY + LIMIT', () => {
  beforeEach(async () => {
    await engine.execute(
      'CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER)',
    );
    await engine.execute(`
      INSERT INTO items (id, val) VALUES (1, 30);
      INSERT INTO items (id, val) VALUES (2, 10);
      INSERT INTO items (id, val) VALUES (3, 50);
      INSERT INTO items (id, val) VALUES (4, 20);
      INSERT INTO items (id, val) VALUES (5, 40);
    `);
  });

  // BUG: ORDER BY fails — PhysicalSort resolves sort expressions against
  // the projection's output layout, but the expressions still reference
  // original scan bindings (tableIndex:0) instead of projection bindings.
  it.fails('ORDER BY ascending', async () => {
    const [result] = await engine.execute('SELECT * FROM items ORDER BY val ASC');
    const vals = result.rows!.map((r) => r.val);
    expect(vals).toEqual([10, 20, 30, 40, 50]);
  });

  it.fails('ORDER BY descending', async () => {
    const [result] = await engine.execute('SELECT * FROM items ORDER BY val DESC');
    const vals = result.rows!.map((r) => r.val);
    expect(vals).toEqual([50, 40, 30, 20, 10]);
  });

  it('LIMIT without ORDER BY restricts row count', async () => {
    const [result] = await engine.execute('SELECT * FROM items LIMIT 3');
    expect(result.rows).toHaveLength(3);
  });

  it.fails('LIMIT with ORDER BY', async () => {
    const [result] = await engine.execute('SELECT * FROM items ORDER BY val ASC LIMIT 3');
    expect(result.rows).toHaveLength(3);
    const vals = result.rows!.map((r) => r.val);
    expect(vals).toEqual([10, 20, 30]);
  });
});

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

describe('UPDATE', () => {
  beforeEach(async () => {
    await engine.execute(
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)',
    );
    await engine.execute(`
      INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
      INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
      INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);
    `);
  });

  it('updates all rows without WHERE', async () => {
    await engine.execute('UPDATE users SET age = 99');
    const [result] = await engine.execute('SELECT * FROM users');
    expect(result.rows!.every((r) => r.age === 99)).toBe(true);
  });

  it('updates rows matching WHERE', async () => {
    await engine.execute("UPDATE users SET age = 50 WHERE name = 'Bob'");
    const [result] = await engine.execute('SELECT * FROM users');
    const bob = result.rows!.find((r) => r.name === 'Bob');
    expect(bob).toBeDefined();
    expect(bob!.age).toBe(50);
    // other rows unchanged
    const alice = result.rows!.find((r) => r.name === 'Alice');
    expect(alice!.age).toBe(30);
  });

  it('returns rowsAffected', async () => {
    const [result] = await engine.execute('UPDATE users SET age = 0 WHERE age > 28');
    expect(result.rowsAffected).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

describe('DELETE', () => {
  beforeEach(async () => {
    await engine.execute(
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)',
    );
    await engine.execute(`
      INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
      INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
      INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);
    `);
  });

  it('deletes rows matching WHERE', async () => {
    await engine.execute("DELETE FROM users WHERE name = 'Bob'");
    const [result] = await engine.execute('SELECT * FROM users');
    expect(result.rows).toHaveLength(2);
    expect(result.rows!.find((r) => r.name === 'Bob')).toBeUndefined();
  });

  it('deletes all rows without WHERE', async () => {
    await engine.execute('DELETE FROM users');
    const [result] = await engine.execute('SELECT * FROM users');
    expect(result.rows).toHaveLength(0);
  });

  it('returns rowsAffected', async () => {
    const [result] = await engine.execute('DELETE FROM users WHERE age >= 30');
    expect(result.rowsAffected).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Aggregates
// ---------------------------------------------------------------------------

describe('aggregates', () => {
  beforeEach(async () => {
    await engine.execute(
      'CREATE TABLE scores (id INTEGER PRIMARY KEY, player TEXT, score INTEGER)',
    );
    await engine.execute(`
      INSERT INTO scores (id, player, score) VALUES (1, 'Alice', 100);
      INSERT INTO scores (id, player, score) VALUES (2, 'Bob', 200);
      INSERT INTO scores (id, player, score) VALUES (3, 'Alice', 150);
      INSERT INTO scores (id, player, score) VALUES (4, 'Bob', 250);
      INSERT INTO scores (id, player, score) VALUES (5, 'Charlie', 300);
    `);
  });

  // BUG: Scalar aggregates (without GROUP BY) crash — the projection above
  // the aggregate references aggregate output bindings that are undefined,
  // causing "Cannot read properties of undefined (reading 'tableIndex')".
  it.fails('COUNT(*)', async () => {
    const [result] = await engine.execute('SELECT COUNT(*) AS cnt FROM scores');
    expect(result.rows).toEqual([{ cnt: 5 }]);
  });

  it.fails('SUM', async () => {
    const [result] = await engine.execute('SELECT SUM(score) AS total FROM scores');
    expect(result.rows).toEqual([{ total: 1000 }]);
  });

  it.fails('MIN / MAX', async () => {
    const [result] = await engine.execute(
      'SELECT MIN(score) AS lo, MAX(score) AS hi FROM scores',
    );
    expect(result.rows).toEqual([{ lo: 100, hi: 300 }]);
  });

  // BUG: GROUP BY + ORDER BY — combines two broken features
  it.fails('GROUP BY with ORDER BY', async () => {
    const [result] = await engine.execute(
      'SELECT player, SUM(score) AS total FROM scores GROUP BY player ORDER BY player',
    );
    expect(result.rows).toEqual([
      { player: 'Alice', total: 250 },
      { player: 'Bob', total: 450 },
      { player: 'Charlie', total: 300 },
    ]);
  });

  it.fails('GROUP BY with HAVING', async () => {
    const [result] = await engine.execute(
      'SELECT player, SUM(score) AS total FROM scores GROUP BY player HAVING SUM(score) > 300',
    );
    expect(result.rows!.length).toBeGreaterThan(0);
  });

  // BUG: GROUP BY without ORDER BY — projection can't resolve aggregate
  // output bindings against the scan layout.
  it.fails('GROUP BY without ORDER BY', async () => {
    const [result] = await engine.execute(
      'SELECT player, SUM(score) FROM scores GROUP BY player',
    );
    expect(result.rows).toHaveLength(3);
  });
});

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

describe('expressions', () => {
  it('arithmetic in SELECT with aliases', async () => {
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)');
    await engine.execute('INSERT INTO t (id, a, b) VALUES (1, 10, 3)');
    const [result] = await engine.execute('SELECT a + b AS sum, a * b AS product FROM t');
    expect(result.rows).toEqual([{ sum: 13, product: 30 }]);
  });

  // BUG: CASE + ORDER BY — same ORDER BY binding resolution issue
  it.fails('CASE expression with ORDER BY', async () => {
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    await engine.execute(`
      INSERT INTO t (id, val) VALUES (1, 10);
      INSERT INTO t (id, val) VALUES (2, 50);
      INSERT INTO t (id, val) VALUES (3, 90);
    `);
    const [result] = await engine.execute(`
      SELECT id, CASE WHEN val < 30 THEN 'low' WHEN val < 70 THEN 'mid' ELSE 'high' END AS label
      FROM t ORDER BY id
    `);
    expect(result.rows).toHaveLength(3);
  });

  it('CASE expression without ORDER BY', async () => {
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    await engine.execute(`
      INSERT INTO t (id, val) VALUES (1, 10);
      INSERT INTO t (id, val) VALUES (2, 50);
      INSERT INTO t (id, val) VALUES (3, 90);
    `);
    const [result] = await engine.execute(`
      SELECT val, CASE WHEN val < 30 THEN 'low' WHEN val < 70 THEN 'mid' ELSE 'high' END
      FROM t
    `);
    expect(result.rows).toHaveLength(3);
    const labels = result.rows!.map((r) => r.column1);
    expect(labels).toContain('low');
    expect(labels).toContain('mid');
    expect(labels).toContain('high');
  });
});

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

describe('DROP TABLE', () => {
  it('drops a table so it no longer exists', async () => {
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    await engine.execute('DROP TABLE t');
    await expect(engine.execute('SELECT * FROM t')).rejects.toThrow();
  });

  it('DROP TABLE IF EXISTS does not error on missing table', async () => {
    await expect(
      engine.execute('DROP TABLE IF EXISTS nonexistent'),
    ).resolves.toBeDefined();
  });
});

import { describe, it, expect } from 'vitest';
import { Engine } from '../engine.js';
import { MemoryStorage } from '../../store/storage/memory-storage.js';

describe('complex benchmark queries', () => {
  async function setup() {
    const engine = await Engine.create(new MemoryStorage());
    await engine.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)');
    await engine.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, customer_id INTEGER, quantity INTEGER, total REAL)');
    await engine.execute(`BEGIN;
      INSERT INTO products VALUES (0, 'P0', 10.0, 'Electronics');
      INSERT INTO products VALUES (1, 'P1', 20.0, 'Books');
      INSERT INTO products VALUES (2, 'P2', 30.0, 'Electronics');
      COMMIT`);
    await engine.execute(`BEGIN;
      INSERT INTO orders VALUES (0, 0, 1, 15, 150.0);
      INSERT INTO orders VALUES (1, 0, 2, 3, 30.0);
      INSERT INTO orders VALUES (2, 1, 1, 12, 240.0);
      INSERT INTO orders VALUES (3, 2, 3, 8, 240.0);
      INSERT INTO orders VALUES (4, 2, 1, 2, 60.0);
      COMMIT`);
    return engine;
  }

  it('subquery EXISTS', async () => {
    const engine = await setup();
    const [r] = await engine.execute(
      "SELECT p.name, p.price FROM products p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 10)"
    );
    // P0 has order with qty 15, P1 has order with qty 12
    expect(r.rows).toHaveLength(2);
    const names = r.rows!.map((r) => r.name).sort();
    expect(names).toEqual(['P0', 'P1']);
    engine.close();
  });

  it('CTE + JOIN + ORDER BY + LIMIT', async () => {
    const engine = await setup();
    const [r] = await engine.execute(
      "WITH top_products AS (SELECT product_id, SUM(total) AS revenue FROM orders GROUP BY product_id) SELECT p.name, p.category, tp.revenue FROM top_products tp INNER JOIN products p ON tp.product_id = p.id WHERE tp.revenue > 10 ORDER BY tp.revenue DESC LIMIT 10"
    );
    expect(r.rows).toHaveLength(3);
    // Verify column names
    expect(Object.keys(r.rows![0])).toEqual(['name', 'category', 'revenue']);
    // Verify ORDER BY DESC: 300, 240, 180
    expect(r.rows![0]).toEqual({ name: 'P2', category: 'Electronics', revenue: 300 });
    expect(r.rows![1]).toEqual({ name: 'P1', category: 'Books', revenue: 240 });
    expect(r.rows![2]).toEqual({ name: 'P0', category: 'Electronics', revenue: 180 });
    engine.close();
  });

  it('JOIN + GROUP BY', async () => {
    const engine = await setup();
    const [r] = await engine.execute(
      "SELECT p.name, SUM(o.quantity) AS sold, SUM(o.total) AS revenue FROM orders o INNER JOIN products p ON o.product_id = p.id GROUP BY p.name"
    );
    expect(r.rows).toHaveLength(3);
    engine.close();
  });

  it('JOIN + filter', async () => {
    const engine = await setup();
    const [r] = await engine.execute(
      "SELECT p.name, o.quantity, o.total FROM orders o INNER JOIN products p ON o.product_id = p.id WHERE p.category = 'Electronics' AND o.quantity > 5"
    );
    // P0 qty 15, P2 qty 8
    expect(r.rows).toHaveLength(2);
    engine.close();
  });

  it('COUNT DISTINCT + HAVING', async () => {
    const engine = await setup();
    const [r] = await engine.execute(
      "SELECT p.category, COUNT(DISTINCT o.customer_id) AS customers, AVG(o.total) AS avg_order FROM orders o INNER JOIN products p ON o.product_id = p.id GROUP BY p.category HAVING COUNT(DISTINCT o.customer_id) > 0"
    );
    expect(r.rows).toHaveLength(2); // Electronics and Books
    engine.close();
  });
});

describe('ORDER BY', () => {
  async function createEngine() {
    const engine = await Engine.create(new MemoryStorage());
    await engine.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, score REAL)');
    await engine.execute(`BEGIN;
      INSERT INTO t VALUES (1, 'Alice', 30, 85.5);
      INSERT INTO t VALUES (2, 'Bob', 25, 92.0);
      INSERT INTO t VALUES (3, 'Charlie', 35, 78.0);
      INSERT INTO t VALUES (4, 'Diana', 28, 92.0);
      INSERT INTO t VALUES (5, 'Eve', 30, 88.0);
      COMMIT`);
    return engine;
  }

  it('ORDER BY ASC', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT name, age FROM t ORDER BY age ASC');
    const ages = r.rows!.map((r) => r.age);
    expect(ages).toEqual([25, 28, 30, 30, 35]);
    engine.close();
  });

  it('ORDER BY DESC', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT name, age FROM t ORDER BY age DESC');
    const ages = r.rows!.map((r) => r.age);
    expect(ages).toEqual([35, 30, 30, 28, 25]);
    engine.close();
  });

  it('ORDER BY column not in select list', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT name FROM t ORDER BY age ASC');
    // Should return only name column, sorted by age
    expect(Object.keys(r.rows![0])).toEqual(['name']);
    expect(r.rows![0].name).toBe('Bob');      // age 25
    expect(r.rows![4].name).toBe('Charlie');   // age 35
    engine.close();
  });

  it('ORDER BY multiple columns', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT name, score, age FROM t ORDER BY score DESC, age ASC');
    // score DESC: 92, 92, 88, 85.5, 78
    // Within score 92: age ASC → Bob(25), Diana(28)
    expect(r.rows![0].name).toBe('Bob');
    expect(r.rows![1].name).toBe('Diana');
    expect(r.rows![2].name).toBe('Eve');
    engine.close();
  });

  it('ORDER BY with LIMIT', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT name FROM t ORDER BY age ASC LIMIT 3');
    expect(r.rows).toHaveLength(3);
    expect(r.rows![0].name).toBe('Bob');    // age 25
    expect(r.rows![1].name).toBe('Diana');  // age 28
    engine.close();
  });

  it('ORDER BY with LIMIT and OFFSET', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT name, age FROM t ORDER BY age ASC LIMIT 2 OFFSET 2');
    expect(r.rows).toHaveLength(2);
    // After offset 2 (skip Bob=25, Diana=28), take 2: Alice=30, Eve=30
    expect(r.rows!.map((r) => r.age)).toEqual([30, 30]);
    engine.close();
  });

  it('ORDER BY with GROUP BY (aggregate in select list)', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT age, COUNT(*) AS cnt FROM t GROUP BY age ORDER BY COUNT(*) DESC');
    // age 30 appears twice, others once
    expect(r.rows![0]).toEqual({ age: 30, cnt: 2 });
    expect(r.rows![1].cnt).toBe(1);
    engine.close();
  });

  it('ORDER BY group column with GROUP BY', async () => {
    const engine = await createEngine();
    const [r] = await engine.execute('SELECT age, COUNT(*) AS cnt FROM t GROUP BY age ORDER BY age ASC');
    const ages = r.rows!.map((row) => row.age);
    expect(ages).toEqual([25, 28, 30, 35]);
    engine.close();
  });

  it('ORDER BY with JOIN', async () => {
    const engine = await createEngine();
    await engine.execute('CREATE TABLE dept (id INTEGER PRIMARY KEY, dname TEXT)');
    await engine.execute(`BEGIN;
      INSERT INTO dept VALUES (1, 'Engineering');
      INSERT INTO dept VALUES (2, 'Sales');
      COMMIT`);
    const [r] = await engine.execute(
      'SELECT t.name, d.dname FROM t INNER JOIN dept d ON t.id = d.id ORDER BY t.name ASC'
    );
    expect(r.rows![0].name).toBe('Alice');
    expect(r.rows![1].name).toBe('Bob');
    engine.close();
  });

  it('ORDER BY with NULL values NULLS LAST (default for ASC)', async () => {
    const engine = await createEngine();
    await engine.execute('INSERT INTO t VALUES (6, NULL, 20, 70.0)');
    const [r] = await engine.execute('SELECT name FROM t ORDER BY name ASC');
    // NULL should be last for ASC
    expect(r.rows![r.rows!.length - 1].name).toBe(null);
    engine.close();
  });
});

describe('EXISTS subquery', () => {
  async function setupExists() {
    const engine = await Engine.create(new MemoryStorage());
    await engine.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
    await engine.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER)');
    await engine.execute(`BEGIN;
      INSERT INTO products VALUES (1, 'Widget', 10.0);
      INSERT INTO products VALUES (2, 'Gadget', 20.0);
      INSERT INTO products VALUES (3, 'Doohickey', 30.0);
      INSERT INTO orders VALUES (1, 1, 15);
      INSERT INTO orders VALUES (2, 1, 3);
      INSERT INTO orders VALUES (3, 2, 12);
      INSERT INTO orders VALUES (4, 3, 2);
      COMMIT`);
    return engine;
  }

  it('EXISTS returns rows that have matching subquery rows', async () => {
    const engine = await setupExists();
    const [r] = await engine.execute(
      "SELECT p.name FROM products p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 10)"
    );
    const names = r.rows!.map((r) => r.name).sort();
    // Widget has qty 15, Gadget has qty 12
    expect(names).toEqual(['Gadget', 'Widget']);
    engine.close();
  });

  it('NOT EXISTS returns rows without matching subquery rows', async () => {
    const engine = await setupExists();
    const [r] = await engine.execute(
      "SELECT p.name FROM products p WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 10)"
    );
    const names = r.rows!.map((r) => r.name);
    // Only Doohickey has no orders with qty > 10
    expect(names).toEqual(['Doohickey']);
    engine.close();
  });

  it('EXISTS with no matching rows returns empty', async () => {
    const engine = await setupExists();
    const [r] = await engine.execute(
      "SELECT p.name FROM products p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 100)"
    );
    expect(r.rows).toHaveLength(0);
    engine.close();
  });

  it('EXISTS early termination does not scan all inner rows', async () => {
    // Insert many orders for product 1 — without early termination this would
    // drain all of them; with limit=1 it stops after the first match.
    const engine = await Engine.create(new MemoryStorage());
    await engine.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)');
    await engine.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, item_id INTEGER, qty INTEGER)');
    await engine.execute("INSERT INTO items VALUES (1, 'A')");
    // Insert 500 matching sales rows
    const inserts: string[] = [];
    for (let i = 0; i < 500; i++) {
      inserts.push(`INSERT INTO sales VALUES (${i}, 1, 20)`);
    }
    await engine.execute(`BEGIN; ${inserts.join('; ')}; COMMIT`);

    const [r] = await engine.execute(
      "SELECT i.name FROM items i WHERE EXISTS (SELECT 1 FROM sales s WHERE s.item_id = i.id AND s.qty > 10)"
    );
    expect(r.rows).toHaveLength(1);
    expect(r.rows![0].name).toBe('A');
    engine.close();
  });

  it('EXISTS with ORDER BY', async () => {
    const engine = await setupExists();
    const [r] = await engine.execute(
      "SELECT p.name FROM products p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 10) ORDER BY p.name ASC"
    );
    expect(r.rows!.map((r) => r.name)).toEqual(['Gadget', 'Widget']);
    engine.close();
  });
});

describe('CTE performance profile', () => {
  it('CTE+JOIN vs plain JOIN at scale', async () => {
    const N_PRODUCTS = 1000;
    const N_ORDERS = 5000;

    const engine = await Engine.create(new MemoryStorage());
    await engine.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)');
    await engine.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, customer_id INTEGER, quantity INTEGER, total REAL)');

    // Insert products
    const pStmts = ['BEGIN'];
    for (let i = 0; i < N_PRODUCTS; i++) {
      pStmts.push(`INSERT INTO products VALUES (${i}, 'P${i}', ${(Math.random() * 100).toFixed(2)}, 'Cat${i % 5}')`);
    }
    pStmts.push('COMMIT');
    await engine.execute(pStmts.join(';\n'));

    // Insert orders
    const oStmts = ['BEGIN'];
    for (let i = 0; i < N_ORDERS; i++) {
      const pid = Math.floor(Math.random() * N_PRODUCTS);
      const qty = 1 + Math.floor(Math.random() * 20);
      oStmts.push(`INSERT INTO orders VALUES (${i}, ${pid}, ${Math.floor(Math.random() * 500)}, ${qty}, ${(qty * Math.random() * 100).toFixed(2)})`);
    }
    oStmts.push('COMMIT');
    await engine.execute(oStmts.join(';\n'));

    // Benchmark: plain JOIN + GROUP BY
    let t0 = performance.now();
    await engine.execute(
      'SELECT p.name, SUM(o.quantity) AS sold, SUM(o.total) AS revenue FROM orders o INNER JOIN products p ON o.product_id = p.id GROUP BY p.name'
    );
    const joinMs = performance.now() - t0;

    // Benchmark: CTE + JOIN + ORDER
    t0 = performance.now();
    await engine.execute(
      `WITH top_products AS (SELECT product_id, SUM(total) AS revenue FROM orders GROUP BY product_id)
       SELECT p.name, p.category, tp.revenue
       FROM top_products tp INNER JOIN products p ON tp.product_id = p.id
       WHERE tp.revenue > 1000
       ORDER BY tp.revenue DESC
       LIMIT 10`
    );
    const cteMs = performance.now() - t0;

    // Benchmark: just GROUP BY (CTE definition part)
    t0 = performance.now();
    await engine.execute('SELECT product_id, SUM(total) AS revenue FROM orders GROUP BY product_id');
    const groupByMs = performance.now() - t0;

    // Benchmark: CTE + JOIN only (no filter/sort/limit)
    t0 = performance.now();
    await engine.execute(
      `WITH top_products AS (SELECT product_id, SUM(total) AS revenue FROM orders GROUP BY product_id)
       SELECT p.name, tp.revenue
       FROM top_products tp INNER JOIN products p ON tp.product_id = p.id`
    );
    const cteJoinMs = performance.now() - t0;

    // Benchmark: CTE + JOIN + WHERE (no sort)
    t0 = performance.now();
    await engine.execute(
      `WITH top_products AS (SELECT product_id, SUM(total) AS revenue FROM orders GROUP BY product_id)
       SELECT p.name, tp.revenue
       FROM top_products tp INNER JOIN products p ON tp.product_id = p.id
       WHERE tp.revenue > 1000`
    );
    const cteFilterMs = performance.now() - t0;

    // Benchmark: CTE + JOIN + ORDER (no filter, no limit)
    t0 = performance.now();
    await engine.execute(
      `WITH top_products AS (SELECT product_id, SUM(total) AS revenue FROM orders GROUP BY product_id)
       SELECT p.name, tp.revenue
       FROM top_products tp INNER JOIN products p ON tp.product_id = p.id
       ORDER BY tp.revenue DESC`
    );
    const cteSortMs = performance.now() - t0;

    // Benchmark: CTE + JOIN + ORDER + LIMIT (no filter)
    t0 = performance.now();
    await engine.execute(
      `WITH top_products AS (SELECT product_id, SUM(total) AS revenue FROM orders GROUP BY product_id)
       SELECT p.name, tp.revenue
       FROM top_products tp INNER JOIN products p ON tp.product_id = p.id
       ORDER BY tp.revenue DESC LIMIT 10`
    );
    const cteSortLimitMs = performance.now() - t0;

    console.log([
      `JOIN+GROUP: ${joinMs.toFixed(1)}ms`,
      `GROUP_BY: ${groupByMs.toFixed(1)}ms`,
      `CTE+JOIN: ${cteJoinMs.toFixed(1)}ms`,
      `CTE+JOIN+WHERE: ${cteFilterMs.toFixed(1)}ms`,
      `CTE+JOIN+SORT: ${cteSortMs.toFixed(1)}ms`,
      `CTE+JOIN+SORT+LIMIT: ${cteSortLimitMs.toFixed(1)}ms`,
      `CTE+JOIN+WHERE+SORT+LIMIT: ${cteMs.toFixed(1)}ms`,
    ].join('\n'));

    // CTE should not be more than 25x slower than plain join
    expect(cteMs).toBeLessThan(joinMs * 25);

    engine.close();
  });
});

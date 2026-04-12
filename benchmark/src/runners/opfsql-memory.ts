import { Engine, PreparedStatement } from '../../../opfsql-lib/src/index.js';
import { MemoryStorage } from '../../../opfsql-lib/src/store/storage/memory-storage.js';
import type { BenchmarkRunner, Row, OrderRow } from '../types.js';

export function createOpfsqlMemoryRunner(): BenchmarkRunner {
  let engine: Engine;
  let insertStmt: PreparedStatement;
  let selectPointStmt: PreparedStatement;

  return {
    name: 'opfsql-mem',
    storage: 'In-memory',

    async setup() {
      const storage = new MemoryStorage();
      engine = await Engine.create(storage);
      await engine.execute(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          name TEXT,
          price REAL,
          category TEXT
        )
      `);
      insertStmt = engine.prepare('INSERT INTO products VALUES ($1, $2, $3, $4)');
      selectPointStmt = engine.prepare('SELECT * FROM products WHERE id = $1');
    },

    async teardown() {
      engine.close();
    },

    async insertBatch(rows: Row[]) {
      await engine.execute('BEGIN');
      for (const r of rows) {
        await insertStmt.run([r.id, r.name, r.price, r.category]);
      }
      await engine.execute('COMMIT');
    },

    async selectAll() {
      const [r] = await engine.execute('SELECT * FROM products');
      return r.rows!;
    },

    async selectPoint(id: number) {
      const r = await selectPointStmt.run([id]);
      return r.rows![0];
    },

    async selectRange(low: number, high: number) {
      const [r] = await engine.execute(
        `SELECT * FROM products WHERE price BETWEEN ${low} AND ${high}`,
      );
      return r.rows!;
    },

    async aggregate() {
      const [r] = await engine.execute(
        'SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category',
      );
      return r.rows!;
    },

    async setupComplex(productRows: Row[], orderRows: OrderRow[]) {
      const storage = new MemoryStorage();
      engine = await Engine.create(storage);

      await engine.execute(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          name TEXT,
          price REAL,
          category TEXT
        )
      `);
      await engine.execute(`
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          product_id INTEGER,
          customer_id INTEGER,
          quantity INTEGER,
          total REAL
        )
      `);
      await engine.execute('CREATE INDEX idx_orders_product ON orders(product_id)');
      await engine.execute('CREATE INDEX idx_orders_customer ON orders(customer_id)');

      const pStmt = engine.prepare('INSERT INTO products VALUES ($1, $2, $3, $4)');
      await engine.execute('BEGIN');
      for (const r of productRows) {
        await pStmt.run([r.id, r.name, r.price, r.category]);
      }
      await engine.execute('COMMIT');

      const oStmt = engine.prepare('INSERT INTO orders VALUES ($1, $2, $3, $4, $5)');
      for (let i = 0; i < orderRows.length; i += 10000) {
        const batch = orderRows.slice(i, i + 10000);
        await engine.execute('BEGIN');
        for (const r of batch) {
          await oStmt.run([r.id, r.product_id, r.customer_id, r.quantity, r.total]);
        }
        await engine.execute('COMMIT');
      }
    },

    async teardownComplex() {
      engine.close();
    },

    async joinAgg() {
      const [r] = await engine.execute(`
        SELECT p.name, SUM(o.quantity) AS sold, SUM(o.total) AS revenue
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.name
      `);
      return r.rows!;
    },

    async joinFilter() {
      const [r] = await engine.execute(`
        SELECT p.name, o.quantity, o.total
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        WHERE p.category = 'Electronics' AND o.quantity > 5
      `);
      return r.rows!;
    },

    async subqueryExists() {
      const [r] = await engine.execute(`
        SELECT p.name, p.price FROM products p
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 10)
      `);
      return r.rows!;
    },

    async cteJoin() {
      const [r] = await engine.execute(`
        WITH top_products AS (
          SELECT product_id, SUM(total) AS revenue
          FROM orders GROUP BY product_id
        )
        SELECT p.name, p.category, tp.revenue
        FROM top_products tp INNER JOIN products p ON tp.product_id = p.id
        WHERE tp.revenue > 1000
        ORDER BY tp.revenue DESC
        LIMIT 10
      `);
      return r.rows!;
    },

    async multiAgg() {
      const [r] = await engine.execute(`
        SELECT p.category, COUNT(DISTINCT o.customer_id) AS customers, AVG(o.total) AS avg_order
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.category
        HAVING COUNT(DISTINCT o.customer_id) > 10
      `);
      return r.rows!;
    },
  };
}

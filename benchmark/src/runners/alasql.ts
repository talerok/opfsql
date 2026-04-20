import alasql from "alasql";
import type { BenchmarkRunner, Row, OrderRow } from "../types.js";

const DB_NAME = "bench";

export function createAlasqlRunner(): BenchmarkRunner {
  return {
    name: "alasql",
    storage: "IndexedDB",

    async setup() {
      await alasql.promise(`CREATE INDEXEDDB DATABASE IF NOT EXISTS ${DB_NAME}`);
      await alasql.promise(`ATTACH INDEXEDDB DATABASE ${DB_NAME}`);
      await alasql.promise(`USE ${DB_NAME}`);

      await alasql.promise("DROP TABLE IF EXISTS products");
      await alasql.promise(`
        CREATE TABLE products (
          id INT PRIMARY KEY,
          name STRING,
          price DECIMAL,
          category STRING
        )
      `);
    },

    async teardown() {
      await alasql.promise("DROP TABLE IF EXISTS products");
      await alasql.promise(`DETACH DATABASE ${DB_NAME}`);
      await alasql.promise(`DROP INDEXEDDB DATABASE IF EXISTS ${DB_NAME}`);
    },

    async insertBatch(rows: Row[]) {
      await alasql.promise("BEGIN TRANSACTION");
      for (const r of rows) {
        await alasql.promise("INSERT INTO products VALUES (?, ?, ?, ?)", [
          r.id, r.name, r.price, r.category,
        ]);
      }
      await alasql.promise("COMMIT TRANSACTION");
    },

    async selectAll() {
      return alasql.promise("SELECT * FROM products");
    },

    async selectPoint(id: number) {
      const rows = await alasql.promise("SELECT * FROM products WHERE id = ?", [id]) as unknown[];
      return rows[0];
    },

    async selectRange(low: number, high: number) {
      return alasql.promise("SELECT * FROM products WHERE price BETWEEN ? AND ?", [
        low,
        high,
      ]);
    },

    async aggregate() {
      return alasql.promise(
        "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category",
      );
    },

    async setupComplex(productRows: Row[], orderRows: OrderRow[]) {
      await alasql.promise(`CREATE INDEXEDDB DATABASE IF NOT EXISTS ${DB_NAME}`);
      await alasql.promise(`ATTACH INDEXEDDB DATABASE ${DB_NAME}`);
      await alasql.promise(`USE ${DB_NAME}`);

      await alasql.promise("DROP TABLE IF EXISTS products");
      await alasql.promise("DROP TABLE IF EXISTS orders");

      await alasql.promise(`
        CREATE TABLE products (
          id INT PRIMARY KEY,
          name STRING,
          price DECIMAL,
          category STRING
        )
      `);
      await alasql.promise(`
        CREATE TABLE orders (
          id INT PRIMARY KEY,
          product_id INT,
          customer_id INT,
          quantity INT,
          [total] DECIMAL
        )
      `);

      await alasql.promise("INSERT INTO products SELECT * FROM ?", [productRows]);
      await alasql.promise("INSERT INTO orders SELECT * FROM ?", [orderRows]);

      await alasql.promise("CREATE INDEX idx_orders_product ON orders(product_id)");
      await alasql.promise("CREATE INDEX idx_orders_customer ON orders(customer_id)");
    },

    async teardownComplex() {
      await alasql.promise("DROP TABLE IF EXISTS products");
      await alasql.promise("DROP TABLE IF EXISTS orders");
      await alasql.promise(`DETACH DATABASE ${DB_NAME}`);
      await alasql.promise(`DROP INDEXEDDB DATABASE IF EXISTS ${DB_NAME}`);
    },

    async joinAgg() {
      return alasql.promise(`
        SELECT p.name, SUM(o.quantity) AS sold, SUM(o.[total]) AS revenue
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.name
      `);
    },

    async joinFilter() {
      return alasql.promise(`
        SELECT p.name, o.quantity, o.[total]
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        WHERE p.category = 'Electronics' AND o.quantity > 5
      `);
    },

    async subqueryExists() {
      // alasql + IndexedDB doesn't support correlated subqueries — rewrite as JOIN
      return alasql.promise(`
        SELECT DISTINCT p.name, p.price FROM products p
        INNER JOIN orders o ON o.product_id = p.id
        WHERE o.quantity > 10
      `);
    },

    async cteJoin() {
      // alasql doesn't support CTEs — rewrite as subquery
      return alasql.promise(`
        SELECT p.name, p.category, tp.revenue
        FROM (
          SELECT product_id, SUM([total]) AS revenue
          FROM orders GROUP BY product_id
        ) tp INNER JOIN products p ON tp.product_id = p.id
        WHERE tp.revenue > 1000
        ORDER BY tp.revenue DESC
        LIMIT 10
      `);
    },

    async multiAgg() {
      return alasql.promise(`
        SELECT p.category, COUNT(DISTINCT o.customer_id) AS customers, AVG(o.[total]) AS avg_order
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.category
        HAVING COUNT(DISTINCT o.customer_id) > 10
      `);
    },
  };
}

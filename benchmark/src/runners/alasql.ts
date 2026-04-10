import alasql from "alasql";
import type { BenchmarkRunner, Row, OrderRow } from "../types.js";

export function createAlasqlRunner(): BenchmarkRunner {
  return {
    name: "alasql",
    storage: "In-memory",

    async setup() {
      alasql("DROP TABLE IF EXISTS products");
      alasql(`
        CREATE TABLE products (
          id INT PRIMARY KEY,
          name STRING,
          price DECIMAL,
          category STRING
        )
      `);
    },

    async teardown() {
      alasql("DROP TABLE IF EXISTS products");
    },

    async insertBatch(rows: Row[]) {
      for (const r of rows) {
        alasql("INSERT INTO products VALUES (?, ?, ?, ?)", [
          r.id,
          r.name,
          r.price,
          r.category,
        ]);
      }
    },

    async selectAll() {
      return alasql("SELECT * FROM products");
    },

    async selectPoint(id: number) {
      const rows = alasql("SELECT * FROM products WHERE id = ?", [id]) as unknown[];
      return rows[0];
    },

    async selectRange(low: number, high: number) {
      return alasql("SELECT * FROM products WHERE price BETWEEN ? AND ?", [
        low,
        high,
      ]);
    },

    async aggregate() {
      return alasql(
        "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category",
      );
    },

    async setupComplex(productRows: Row[], orderRows: OrderRow[]) {
      alasql("DROP TABLE IF EXISTS products");
      alasql("DROP TABLE IF EXISTS orders");

      alasql(`
        CREATE TABLE products (
          id INT PRIMARY KEY,
          name STRING,
          price DECIMAL,
          category STRING
        )
      `);
      alasql(`
        CREATE TABLE orders (
          id INT PRIMARY KEY,
          product_id INT,
          customer_id INT,
          quantity INT,
          [total] DECIMAL
        )
      `);

      for (const r of productRows) {
        alasql("INSERT INTO products VALUES (?, ?, ?, ?)", [
          r.id, r.name, r.price, r.category,
        ]);
      }
      for (const r of orderRows) {
        alasql("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", [
          r.id, r.product_id, r.customer_id, r.quantity, r.total,
        ]);
      }
    },

    async teardownComplex() {
      alasql("DROP TABLE IF EXISTS products");
      alasql("DROP TABLE IF EXISTS orders");
    },

    async joinAgg() {
      return alasql(`
        SELECT p.name, SUM(o.quantity) AS sold, SUM(o.[total]) AS revenue
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.name
      `);
    },

    async joinFilter() {
      return alasql(`
        SELECT p.name, o.quantity, o.[total]
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        WHERE p.category = 'Electronics' AND o.quantity > 5
      `);
    },

    async subqueryExists() {
      return alasql(`
        SELECT p.name, p.price FROM products p
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 10)
      `);
    },

    async cteJoin() {
      // alasql doesn't support CTEs — rewrite as subquery
      return alasql(`
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
      return alasql(`
        SELECT p.category, COUNT(DISTINCT o.customer_id) AS customers, AVG(o.[total]) AS avg_order
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.category
        HAVING COUNT(DISTINCT o.customer_id) > 10
      `);
    },
  };
}

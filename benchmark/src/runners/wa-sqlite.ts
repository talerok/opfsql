import type { BenchmarkRunner, OrderRow, Row } from "../types.js";

import waWorkerUrl from './wa-sqlite-worker.ts?worker&url';

let worker: Worker;
let msgId = 0;
const pending = new Map<
  number,
  { resolve: (v: any) => void; reject: (e: Error) => void }
>();

function initWorker() {
  worker = new Worker(waWorkerUrl, { type: "module" });
  worker.onmessage = (e: MessageEvent) => {
    const { id, ok, rows, stmtId, error } = e.data;
    const p = pending.get(id);
    if (!p) return;
    pending.delete(id);
    if (ok) p.resolve(stmtId ?? rows);
    else p.reject(new Error(error));
  };
}

function send(
  type: string,
  sql?: string,
  params?: unknown[],
  stmtId?: number,
): Promise<any> {
  return new Promise((resolve, reject) => {
    const id = ++msgId;
    pending.set(id, { resolve, reject });
    worker.postMessage({ id, type, sql, params, stmtId });
  });
}

async function exec(sql: string): Promise<unknown[]> {
  return (await send("exec", sql)) as unknown[];
}

async function run(sql: string, params?: unknown[]): Promise<void> {
  await send("run", sql, params);
}

async function prepare(sql: string): Promise<number> {
  return (await send("prepare", sql)) as number;
}

async function stmtRun(stmtId: number, params?: unknown[]): Promise<unknown[]> {
  return (await send("stmt_run", undefined, params, stmtId)) as unknown[];
}

async function stmtFree(stmtId: number): Promise<void> {
  await send("stmt_free", undefined, undefined, stmtId);
}

export function createWaSqliteRunner(): BenchmarkRunner {
  let insertStmt: number;
  let selectPointStmt: number;

  return {
    name: "wa-sqlite",
    storage: "OPFS",

    async setup() {
      if (!worker) {
        initWorker();
        await send("init");
      }
      await exec("DROP TABLE IF EXISTS products");
      await exec(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          name TEXT,
          price REAL,
          category TEXT
        )
      `);
      insertStmt = await prepare("INSERT INTO products VALUES (?, ?, ?, ?)");
      selectPointStmt = await prepare("SELECT * FROM products WHERE id = ?");
    },

    async teardown() {
      await stmtFree(insertStmt);
      await stmtFree(selectPointStmt);
      await exec("DROP TABLE IF EXISTS products");
    },

    async insertBatch(rows: Row[]) {
      await exec("BEGIN");
      for (const r of rows) {
        await stmtRun(insertStmt, [r.id, r.name, r.price, r.category]);
      }
      await exec("COMMIT");
    },

    async selectAll() {
      return exec("SELECT * FROM products");
    },

    async selectPoint(id: number) {
      const rows = await stmtRun(selectPointStmt, [id]);
      return rows[0];
    },

    async selectRange(low: number, high: number) {
      return exec(
        `SELECT * FROM products WHERE price BETWEEN ${low} AND ${high}`,
      );
    },

    async aggregate() {
      return exec(
        "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category",
      );
    },

    async setupComplex(productRows: Row[], orderRows: OrderRow[]) {
      await exec("DROP TABLE IF EXISTS orders");
      await exec("DROP TABLE IF EXISTS products");
      await exec(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          name TEXT,
          price REAL,
          category TEXT
        )
      `);
      await exec(`
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          product_id INTEGER,
          customer_id INTEGER,
          quantity INTEGER,
          total REAL
        )
      `);
      await exec("CREATE INDEX idx_orders_product ON orders(product_id)");
      await exec("CREATE INDEX idx_orders_customer ON orders(customer_id)");

      const pStmt = await prepare("INSERT INTO products VALUES (?, ?, ?, ?)");
      await exec("BEGIN");
      for (const r of productRows) {
        await stmtRun(pStmt, [r.id, r.name, r.price, r.category]);
      }
      await exec("COMMIT");
      await stmtFree(pStmt);

      const oStmt = await prepare("INSERT INTO orders VALUES (?, ?, ?, ?, ?)");
      for (let i = 0; i < orderRows.length; i += 10000) {
        const batch = orderRows.slice(i, i + 10000);
        await exec("BEGIN");
        for (const r of batch) {
          await stmtRun(oStmt, [r.id, r.product_id, r.customer_id, r.quantity, r.total]);
        }
        await exec("COMMIT");
      }
      await stmtFree(oStmt);
    },

    async teardownComplex() {
      await exec("DROP TABLE IF EXISTS orders");
      await exec("DROP TABLE IF EXISTS products");
    },

    async joinAgg() {
      return exec(`
        SELECT p.name, SUM(o.quantity) AS sold, SUM(o.total) AS revenue
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.name
      `);
    },

    async joinFilter() {
      return exec(`
        SELECT p.name, o.quantity, o.total
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        WHERE p.category = 'Electronics' AND o.quantity > 5
      `);
    },

    async subqueryExists() {
      return exec(`
        SELECT p.name, p.price FROM products p
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.quantity > 10)
      `);
    },

    async cteJoin() {
      return exec(`
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
    },

    async multiAgg() {
      return exec(`
        SELECT p.category, COUNT(DISTINCT o.customer_id) AS customers, AVG(o.total) AS avg_order
        FROM orders o INNER JOIN products p ON o.product_id = p.id
        GROUP BY p.category
        HAVING COUNT(DISTINCT o.customer_id) > 10
      `);
    },
  };
}

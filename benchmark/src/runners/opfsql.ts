import { Engine } from "../../../opfsql-lib/src/index.js";
import { OPFSStorage } from "../../../opfsql-lib/src/store/storage/opfs-storage.js";
import type { BenchmarkRunner, Row } from "../types.js";

const DB_NAME = "bench-opfsql";

export function createOpfsqlRunner(): BenchmarkRunner {
  let engine: Engine;

  return {
    name: "opfsql",
    storage: "OPFS",

    async setup() {
      const root = await navigator.storage.getDirectory();
      try {
        await root.removeEntry(DB_NAME, { recursive: true });
      } catch {}

      const storage = new OPFSStorage(DB_NAME);
      engine = await Engine.create(storage);
      await engine.execute(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          name TEXT,
          price REAL,
          category TEXT
        )
      `);
    },

    async begin() {
      await engine.execute("BEGIN");
    },

    async commit() {
      await engine.execute("COMMIT");
    },

    async teardown() {
      engine.close();
      const root = await navigator.storage.getDirectory();
      try {
        await root.removeEntry(DB_NAME, { recursive: true });
      } catch {}
    },

    async insertRow(row: Row) {
      await engine.execute(
        `INSERT INTO products (id, name, price, category) VALUES (${row.id}, '${row.name}', ${row.price}, '${row.category}')`,
      );
    },

    async selectAll() {
      const [r] = await engine.execute("SELECT * FROM products");
      return r.rows!;
    },

    async selectPoint(id: number) {
      const [r] = await engine.execute(
        `SELECT * FROM products WHERE id = ${id}`,
      );
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
        "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category",
      );
      return r.rows!;
    },
  };
}

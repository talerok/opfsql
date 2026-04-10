import { Engine } from '../../../opfsql-lib/src/index.js';
import { MemoryStorage } from '../../../opfsql-lib/src/store/storage/memory-storage.js';
import type { BenchmarkRunner, Row } from '../types.js';

export function createOpfsqlMemoryRunner(): BenchmarkRunner {
  let engine: Engine;

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
    },

    async begin() {
      await engine.execute('BEGIN');
    },

    async commit() {
      await engine.execute('COMMIT');
    },

    async teardown() {
      engine.close();
    },

    async insertRow(row: Row) {
      await engine.execute(
        `INSERT INTO products (id, name, price, category) VALUES (${row.id}, '${row.name}', ${row.price}, '${row.category}')`,
      );
    },

    async selectAll() {
      const [r] = await engine.execute('SELECT * FROM products');
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
        'SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category',
      );
      return r.rows!;
    },
  };
}

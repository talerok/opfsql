import alasql from 'alasql';
import type { BenchmarkRunner, Row } from '../types.js';

export function createAlasqlRunner(): BenchmarkRunner {
  return {
    name: 'alasql',
    storage: 'In-memory',

    async setup() {
      alasql('DROP TABLE IF EXISTS products');
      alasql(`
        CREATE TABLE products (
          id INT PRIMARY KEY,
          name STRING,
          price DECIMAL,
          category STRING
        )
      `);
    },

    async begin() {},
    async commit() {},

    async teardown() {
      alasql('DROP TABLE IF EXISTS products');
    },

    async insertRow(row: Row) {
      alasql('INSERT INTO products VALUES (?, ?, ?, ?)', [
        row.id,
        row.name,
        row.price,
        row.category,
      ]);
    },

    async selectAll() {
      return alasql('SELECT * FROM products');
    },

    async selectPoint(id: number) {
      const rows = alasql('SELECT * FROM products WHERE id = ?', [id]);
      return rows[0];
    },

    async selectRange(low: number, high: number) {
      return alasql(
        'SELECT * FROM products WHERE price BETWEEN ? AND ?',
        [low, high],
      );
    },

    async aggregate() {
      return alasql(
        'SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category',
      );
    },
  };
}

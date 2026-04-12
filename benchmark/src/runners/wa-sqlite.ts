import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite-async.mjs';
import * as SQLite from 'wa-sqlite';
import { IDBBatchAtomicVFS } from 'wa-sqlite/src/examples/IDBBatchAtomicVFS.js';
import wasmUrl from 'wa-sqlite/dist/wa-sqlite-async.wasm?url';
import type { BenchmarkRunner, Row } from '../types.js';

const DB_NAME = 'bench-wasqlite';
const IDB_NAME = 'bench-wasqlite-idb';

let sqlite3: any;
let db: number;
let ready = false;

async function ensureInit() {
  if (ready) return;
  const module = await SQLiteESMFactory({
    locateFile: (file: string) => file.endsWith('.wasm') ? wasmUrl : file,
  });
  sqlite3 = SQLite.Factory(module);
  const vfs = new IDBBatchAtomicVFS(IDB_NAME);
  sqlite3.vfs_register(vfs, true);
  db = await sqlite3.open_v2(DB_NAME);
  ready = true;
}

async function exec(sql: string): Promise<unknown[]> {
  const rows: unknown[] = [];
  await sqlite3.exec(db, sql, (row: unknown[], columns: string[]) => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => (obj[col] = row[i]));
    rows.push(obj);
  });
  return rows;
}

async function execPrepared(sql: string, bindFn: (stmt: number) => void): Promise<void> {
  for await (const stmt of sqlite3.statements(db, sql)) {
    bindFn(stmt);
    await sqlite3.step(stmt);
  }
}

export function createWaSqliteRunner(): BenchmarkRunner {
  return {
    name: 'wa-sqlite',
    storage: 'IndexedDB',

    async setup() {
      await ensureInit();
      await exec('DROP TABLE IF EXISTS products');
      await exec(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          name TEXT,
          price REAL,
          category TEXT
        )
      `);
    },

    async teardown() {
      await exec('DROP TABLE IF EXISTS products');
    },

    async insertBatch(rows: Row[]) {
      await exec('BEGIN');
      for (const r of rows) {
        await execPrepared(
          'INSERT INTO products VALUES (?, ?, ?, ?)',
          (stmt) => {
            sqlite3.bind_int(stmt, 1, r.id);
            sqlite3.bind_text(stmt, 2, r.name);
            sqlite3.bind_double(stmt, 3, r.price);
            sqlite3.bind_text(stmt, 4, r.category);
          },
        );
      }
      await exec('COMMIT');
    },

    async selectAll() {
      return exec('SELECT * FROM products');
    },

    async selectPoint(id: number) {
      const rows: unknown[] = [];
      for await (const stmt of sqlite3.statements(db, 'SELECT * FROM products WHERE id = ?')) {
        sqlite3.bind_int(stmt, 1, id);
        const result = await sqlite3.step(stmt);
        if (result === SQLite.SQLITE_ROW) {
          const columns = sqlite3.column_names(stmt);
          const obj: Record<string, unknown> = {};
          columns.forEach((col: string, i: number) => (obj[col] = sqlite3.column(stmt, i)));
          rows.push(obj);
        }
      }
      return rows[0];
    },

    async selectRange(low: number, high: number) {
      return exec(
        `SELECT * FROM products WHERE price BETWEEN ${low} AND ${high}`,
      );
    },

    async aggregate() {
      return exec(
        'SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products GROUP BY category',
      );
    },
  };
}

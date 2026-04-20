import * as SQLite from "wa-sqlite";
import SQLiteESMFactory from "wa-sqlite/dist/wa-sqlite.mjs";
import wasmUrl from "wa-sqlite/dist/wa-sqlite.wasm?url";
import { AccessHandlePoolVFS } from "wa-sqlite/src/examples/AccessHandlePoolVFS.js";

let sqlite3: any;
let db: number;
const stmts = new Map<number, number>(); // stmtId → stmt handle
let stmtIdCounter = 0;

async function init() {
  const module = await SQLiteESMFactory({
    locateFile: (file: string) => (file.endsWith(".wasm") ? wasmUrl : file),
  });
  sqlite3 = SQLite.Factory(module);
  const vfs = new AccessHandlePoolVFS("/wa-sqlite-bench");
  await vfs.isReady;
  sqlite3.vfs_register(vfs, true);
  db = await sqlite3.open_v2("bench-wasqlite");
  await sqlite3.exec(db, "PRAGMA journal_mode=MEMORY");
  await sqlite3.exec(db, "PRAGMA temp_store=MEMORY");
}

async function execQuery(sql: string): Promise<unknown[]> {
  const rows: unknown[] = [];
  await sqlite3.exec(db, sql, (row: unknown[], columns: string[]) => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col: string, i: number) => (obj[col] = row[i]));
    rows.push(obj);
  });
  return rows;
}

async function runStmt(sql: string, params?: unknown[]): Promise<void> {
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params) {
      sqlite3.bind_collection(stmt, params);
    }
    await sqlite3.step(stmt);
  }
}

async function prepare(sql: string): Promise<number> {
  const str = sqlite3.str_new(db, sql);
  const prepared = await sqlite3.prepare_v2(db, sqlite3.str_value(str));
  sqlite3.str_finish(str);
  const id = ++stmtIdCounter;
  stmts.set(id, prepared.stmt);
  return id;
}

async function stmtRun(stmtId: number, params?: unknown[]): Promise<unknown[]> {
  const stmt = stmts.get(stmtId)!;
  if (params) {
    sqlite3.bind_collection(stmt, params);
  }
  const rows: unknown[] = [];
  const columns: string[] = [];
  while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
    if (columns.length === 0) {
      const colCount = sqlite3.column_count(stmt);
      for (let i = 0; i < colCount; i++) {
        columns.push(sqlite3.column_name(stmt, i));
      }
    }
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => {
      obj[col] = sqlite3.column(stmt, i);
    });
    rows.push(obj);
  }
  sqlite3.reset(stmt);
  return rows;
}

function stmtFree(stmtId: number): void {
  const stmt = stmts.get(stmtId);
  if (stmt !== undefined) {
    sqlite3.finalize(stmt);
    stmts.delete(stmtId);
  }
}

self.onmessage = async (e: MessageEvent) => {
  const { id, type, sql, params, stmtId } = e.data;
  try {
    switch (type) {
      case "init":
        await init();
        self.postMessage({ id, ok: true });
        break;
      case "exec": {
        const rows = await execQuery(sql);
        self.postMessage({ id, ok: true, rows });
        break;
      }
      case "run":
        await runStmt(sql, params);
        self.postMessage({ id, ok: true });
        break;
      case "prepare": {
        const sid = await prepare(sql);
        self.postMessage({ id, ok: true, stmtId: sid });
        break;
      }
      case "stmt_run": {
        const rows = await stmtRun(stmtId, params);
        self.postMessage({ id, ok: true, rows });
        break;
      }
      case "stmt_free":
        stmtFree(stmtId);
        self.postMessage({ id, ok: true });
        break;
      default:
        self.postMessage({ id, ok: false, error: `unknown type: ${type}` });
    }
  } catch (err: any) {
    self.postMessage({ id, ok: false, error: err.message });
  }
};

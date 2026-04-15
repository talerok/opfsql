import * as SQLite from "wa-sqlite";
import SQLiteESMFactory from "wa-sqlite/dist/wa-sqlite.mjs";
import wasmUrl from "wa-sqlite/dist/wa-sqlite.wasm?url";
import { AccessHandlePoolVFS } from "wa-sqlite/src/examples/AccessHandlePoolVFS.js";

let sqlite3: any;
let db: number;

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

self.onmessage = async (e: MessageEvent) => {
  const { id, type, sql, params } = e.data;
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
      default:
        self.postMessage({ id, ok: false, error: `unknown type: ${type}` });
    }
  } catch (err: any) {
    self.postMessage({ id, ok: false, error: err.message });
  }
};

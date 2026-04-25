import {
  WorkerEngine,
  type Connection,
} from "../../../src/worker/client.js";

const WORKER_URL = new URL(
  "../../../src/worker/worker.ts",
  import.meta.url,
);

let engine: WorkerEngine | null = null;
let conn: Connection | null = null;

const api = {
  async open(dbName: string) {
    engine = new WorkerEngine(WORKER_URL);
    await engine.open(dbName);
  },

  async connect() {
    if (!engine) throw new Error("Engine not opened");
    conn = await engine.connect();
  },

  async exec(sql: string, params?: unknown[]) {
    if (!conn) throw new Error("Not connected");
    return conn.exec(sql, params);
  },

  async disconnect() {
    await conn?.disconnect();
    conn = null;
  },

  async close() {
    await conn?.disconnect();
    conn = null;
    await engine?.close();
    engine = null;
  },

  async cleanOpfs(dbName: string) {
    const root = await navigator.storage.getDirectory();
    try {
      await root.removeEntry(`${dbName}.opfsql`);
    } catch {}
    try {
      await root.removeEntry(`${dbName}.opfsql-wal`);
    } catch {}
  },
};

(window as any).__opfsql = api;
document.getElementById("status")!.textContent = "ready";

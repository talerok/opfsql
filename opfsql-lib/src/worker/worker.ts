import { Engine, PreparedStatement, type Result } from "../engine/index.js";
import { OPFSSyncStorage } from "../store/opfs-storage.js";
import { Value } from "../types.js";

console.log("[opfsql worker] loaded");

// ---------------------------------------------------------------------------
// Message protocol (worker side)
// ---------------------------------------------------------------------------

type InMsg =
  | { id: number; type: "open"; dbName: string }
  | { id: number; type: "close" }
  | { id: number; type: "exec"; sql: string; params?: Value[] }
  | { id: number; type: "prepare"; sql: string }
  | { id: number; type: "run"; stmtId: number; params?: Value[] }
  | { id: number; type: "free"; stmtId: number };

type OutMsg =
  | { id: number; ok: true }
  | { id: number; results: Result[] }
  | { id: number; stmtId: number }
  | { id: number; error: string };

// ---------------------------------------------------------------------------

let engine: Engine | null = null;
const prepared = new Map<number, PreparedStatement>();
let stmtSeq = 0;

function reply(msg: OutMsg): void {
  (self as unknown as Worker).postMessage(msg);
}

(self as unknown as Worker).onmessage = async ({
  data,
}: MessageEvent<InMsg>) => {
  const { id } = data;

  try {
    switch (data.type) {
      case "open": {
        if (engine) engine.close();
        console.log("[opfsql worker] opening", data.dbName);
        engine = await Engine.create(new OPFSSyncStorage(data.dbName));
        console.log("[opfsql worker] opened", data.dbName);
        reply({ id, ok: true });
        break;
      }

      case "close": {
        engine?.close();
        engine = null;
        reply({ id, ok: true });
        break;
      }

      case "exec": {
        if (!engine) throw new Error("Engine not opened");
        const results = engine.execute(data.sql, data.params);
        reply({ id, results });
        break;
      }

      case "prepare": {
        if (!engine) throw new Error("Engine not opened");
        const stmt = engine.prepare(data.sql);
        const stmtId = stmtSeq++;
        prepared.set(stmtId, stmt);
        reply({ id, stmtId });
        break;
      }

      case "run": {
        const stmt = prepared.get(data.stmtId);
        if (!stmt)
          throw new Error(`Prepared statement ${data.stmtId} not found`);
        const result = stmt.run(data.params);
        reply({ id, results: [result] });
        break;
      }

      case "free": {
        prepared.delete(data.stmtId);
        reply({ id, ok: true });
        break;
      }
    }
  } catch (err) {
    reply({ id, error: (err as Error).message });
  }
};

import { Engine, PreparedStatement, type Result } from "../engine/index.js";
import type { Session } from "../engine/session.js";
import type { CatalogData } from "../store/types.js";
import { Value } from "../types.js";

console.log("[opfsql worker] loaded");

// ---------------------------------------------------------------------------
// Message protocol (worker side)
// ---------------------------------------------------------------------------

type InMsg =
  | { id: number; type: "open"; dbName: string }
  | { id: number; type: "close" }
  | { id: number; type: "connect" }
  | { id: number; type: "disconnect"; sessionId: string }
  | { id: number; type: "exec"; sessionId: string; sql: string; params?: Value[] }
  | { id: number; type: "prepare"; sessionId: string; sql: string }
  | { id: number; type: "run"; sessionId: string; stmtId: number; params?: Value[] }
  | { id: number; type: "free"; sessionId: string; stmtId: number }
  | { id: number; type: "schema"; sessionId: string };

type OutMsg =
  | { id: number; ok: true }
  | { id: number; results: Result[] }
  | { id: number; sessionId: string }
  | { id: number; stmtId: number }
  | { id: number; schema: CatalogData }
  | { id: number; error: string };

// ---------------------------------------------------------------------------

let engine: Engine | null = null;
const sessions = new Map<string, Session>();
const prepared = new Map<string, Map<number, PreparedStatement>>();
let sessionSeq = 0;
let stmtSeq = 0;

function reply(msg: OutMsg): void {
  (self as unknown as Worker).postMessage(msg);
}

function getSession(sessionId: string): Session {
  const s = sessions.get(sessionId);
  if (!s) throw new Error(`Session "${sessionId}" not found`);
  return s;
}

(self as unknown as Worker).onmessage = async ({
  data,
}: MessageEvent<InMsg>) => {
  if (!data || typeof data.id !== "number" || typeof data.type !== "string") {
    console.error("[opfsql worker] malformed message", data);
    return;
  }
  const { id } = data;

  try {
    switch (data.type) {
      case "open": {
        if (engine) {
          // Close all sessions before re-opening
          for (const s of sessions.values()) s.close();
          sessions.clear();
          prepared.clear();
          engine.close();
        }
        console.log("[opfsql worker] opening", data.dbName);
        engine = await Engine.open(data.dbName);
        console.log("[opfsql worker] opened", data.dbName);
        reply({ id, ok: true });
        break;
      }

      case "close": {
        for (const s of sessions.values()) s.close();
        sessions.clear();
        prepared.clear();
        engine?.close();
        engine = null;
        reply({ id, ok: true });
        break;
      }

      case "connect": {
        if (!engine) throw new Error("Engine not opened");
        const sessionId = `s${sessionSeq++}`;
        sessions.set(sessionId, engine.createSession());
        prepared.set(sessionId, new Map());
        reply({ id, sessionId });
        break;
      }

      case "disconnect": {
        const session = getSession(data.sessionId);
        session.close();
        sessions.delete(data.sessionId);
        // Free all prepared statements for this session
        prepared.delete(data.sessionId);
        reply({ id, ok: true });
        break;
      }

      case "exec": {
        const session = getSession(data.sessionId);
        const results = session.execute(data.sql, data.params);
        reply({ id, results });
        break;
      }

      case "prepare": {
        const session = getSession(data.sessionId);
        const stmt = session.prepare(data.sql);
        const stmtId = stmtSeq++;
        prepared.get(data.sessionId)!.set(stmtId, stmt);
        reply({ id, stmtId });
        break;
      }

      case "run": {
        const stmts = prepared.get(data.sessionId);
        if (!stmts) throw new Error(`Session "${data.sessionId}" not found`);
        const stmt = stmts.get(data.stmtId);
        if (!stmt)
          throw new Error(`Prepared statement ${data.stmtId} not found`);
        const result = stmt.run(data.params);
        reply({ id, results: [result] });
        break;
      }

      case "free": {
        prepared.get(data.sessionId)?.delete(data.stmtId);
        reply({ id, ok: true });
        break;
      }

      case "schema": {
        const session = getSession(data.sessionId);
        reply({ id, schema: session.getSchema() });
        break;
      }

      default:
        throw new Error(`Unknown message type: ${(data as { type: string }).type}`);
    }
  } catch (err) {
    reply({ id, error: (err as Error).message });
  }
};

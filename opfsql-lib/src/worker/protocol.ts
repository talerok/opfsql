import type { Result } from "../engine/index.js";
import type { CatalogData } from "../store/types.js";
import type { Value } from "../types.js";

// ---------------------------------------------------------------------------
// Worker message protocol — shared between worker and client
// ---------------------------------------------------------------------------

export type RequestPayload =
  | { type: "open"; dbName: string }
  | { type: "close" }
  | { type: "connect" }
  | { type: "disconnect"; sessionId: string }
  | { type: "disconnect-all" }
  | { type: "exec"; sessionId: string; sql: string; params?: Value[] }
  | { type: "prepare"; sessionId: string; sql: string }
  | { type: "run"; sessionId: string; stmtId: number; params?: Value[] }
  | { type: "free"; sessionId: string; stmtId: number }
  | { type: "schema"; sessionId: string };

export type RequestMessage = { id: number } & RequestPayload;

export type ResponseMessage =
  | { id: number; ok: true }
  | { id: number; results: Result[] }
  | { id: number; sessionId: string }
  | { id: number; stmtId: number }
  | { id: number; schema: CatalogData }
  | { id: number; error: string };

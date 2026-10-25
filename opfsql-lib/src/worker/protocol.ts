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
  | { id: number; error: WireError };

/** An error crossing the worker boundary keeps its name, like DOMException. */
export interface WireError {
  name: string;
  message: string;
}

export function toWireError(err: unknown): WireError {
  return err instanceof Error
    ? { name: err.name, message: err.message }
    : { name: "Error", message: String(err) };
}

/** Rebuilds an error on the client side; check it by `name`, not instanceof. */
export function fromWireError({ name, message }: WireError): Error {
  const err = new Error(message);
  err.name = name;
  return err;
}

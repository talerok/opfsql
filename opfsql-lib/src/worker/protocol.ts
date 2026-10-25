import type {
  OpenOptions,
  ResultData,
  Schema,
  TransactionMode,
} from "../engine/api/index.js";
import type { Value } from "../values/index.js";
import type { WireError } from "./wire-error.js";

// The messages between the client (the page) and the host (the worker).
// Every request has an id and gets exactly one reply with that id; a cancel
// asks the host to stop waiting on the request it names; events come
// unasked.

/**
 * The version of these messages. The page and the worker must load the same
 * opfsql; the host refuses an open from a client of another version.
 */
export const PROTOCOL_VERSION = 1;

/** Open options that can cross the boundary: callbacks stay in the page. */
export type WireOpenOptions = Omit<OpenOptions, "onCheckpointError">;

// A request without `connection` runs on a session of the database's own:
// one from a small pool, so calls on the database do not wait for each
// other. A request with one runs on that connection, in order.

export type Request =
  | { readonly type: "hello" }
  | {
      readonly type: "open";
      readonly protocol: number;
      readonly name: string;
      readonly options: WireOpenOptions;
    }
  | { readonly type: "connect" }
  | { readonly type: "disconnect"; readonly connection: number }
  | {
      readonly type: "query";
      readonly connection?: number;
      readonly sql: string;
      readonly parameters: readonly Value[];
    }
  | {
      readonly type: "execute";
      readonly connection?: number;
      readonly script: string;
    }
  | {
      readonly type: "prepare";
      readonly connection?: number;
      readonly sql: string;
    }
  | {
      readonly type: "run";
      readonly connection: number;
      readonly statement: number;
      readonly parameters: readonly Value[];
    }
  | {
      readonly type: "finalize";
      readonly connection: number;
      readonly statement: number;
    }
  | {
      readonly type: "begin";
      readonly connection: number;
      readonly mode: TransactionMode;
    }
  | { readonly type: "commit"; readonly connection: number }
  | { readonly type: "rollback"; readonly connection: number }
  | { readonly type: "schema"; readonly connection?: number }
  | { readonly type: "close" };

/** One request type's message. */
export type RequestOf<K extends Request["type"]> = Extract<
  Request,
  { type: K }
> & { readonly id: number };

/** What each request's reply carries. */
export interface Replies {
  readonly hello: null;
  readonly open: null;
  readonly connect: { readonly connection: number };
  readonly disconnect: null;
  readonly query: ResultData;
  readonly execute: readonly ResultData[];
  readonly prepare: {
    readonly statement: number;
    readonly columns: readonly string[];
    /** The connection the statement runs on; its own, if none was given. */
    readonly connection: number;
  };
  readonly run: {
    readonly result: ResultData;
    /** The statement's columns after this run, which may have rebound it. */
    readonly columns: readonly string[];
  };
  readonly finalize: null;
  readonly begin: null;
  readonly commit: null;
  readonly rollback: null;
  readonly schema: Schema;
  readonly close: null;
}

export type RequestMessage = Request & { readonly id: number };

export interface CancelMessage {
  readonly type: "cancel";
  /** The request to stop waiting on. */
  readonly id: number;
}

/** Client → host. */
export type ClientMessage = RequestMessage | CancelMessage;

export type ReplyMessage =
  | { readonly id: number; readonly ok: true; readonly value: unknown }
  | { readonly id: number; readonly ok: false; readonly error: WireError };

export interface DatabaseEvent {
  readonly type: "checkpoint-error";
  readonly error: WireError;
}

export interface EventMessage {
  readonly type: "event";
  readonly event: DatabaseEvent;
}

/** Host → client. */
export type HostMessage = ReplyMessage | EventMessage;

const REQUEST_TYPES = new Set<string>([
  "hello",
  "open",
  "connect",
  "disconnect",
  "query",
  "execute",
  "prepare",
  "run",
  "finalize",
  "begin",
  "commit",
  "rollback",
  "schema",
  "close",
  "cancel",
]);

/** A message has at least the shape the host dispatches on. */
export function isClientMessage(data: unknown): data is ClientMessage {
  if (typeof data !== "object" || data === null) return false;
  const { type, id } = data as { type?: unknown; id?: unknown };
  return (
    typeof type === "string" &&
    REQUEST_TYPES.has(type) &&
    typeof id === "number"
  );
}

export function isReply(message: HostMessage): message is ReplyMessage {
  return "id" in message;
}

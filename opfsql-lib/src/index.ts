// opfsql's public API; docs/api.md describes it.

import { Engine, engineContract } from "./engine/index.js";
import { type HostScope, startHost } from "./worker/index.js";

// --- In the page: the database, served by a worker -------------------------

export {
  type Connection,
  type Database,
  type DatabaseOptions,
  openDatabase,
  type Statement,
  type WorkerHandle,
} from "./worker/index.js";

// --- Shared by the page and the worker -------------------------------------

export {
  type CallOptions,
  type ColumnInfo,
  type IndexInfo,
  type OpenOptions,
  Result,
  type ResultData,
  type ResultRow,
  type Schema,
  type TableInfo,
  type TransactionMode,
  type TransactionOptions,
} from "./engine/index.js";
export {
  AbortError,
  BusyError,
  DatabaseInUseError,
  DatatypeMismatchError,
  type ErrorCode,
  IntegerOverflowError,
  InternalError,
  MisuseError,
  NotSupportedError,
  OpfsqlError,
  SemanticError,
  SerializationError,
  SqlSyntaxError,
  TransactionAbortedError,
} from "./errors.js";
export type { JsonValue, SqlType, Value } from "./values/index.js";

// --- In the worker ----------------------------------------------------------

/**
 * Makes this dedicated worker serve a database to openDatabase() in the page.
 * Call it once, in the worker's script.
 */
export function serveDatabase(scope?: HostScope): void {
  startHost(scope ?? globalThis, async (name, options) =>
    engineContract(await Engine.open(name, options)),
  );
}

export type { HostScope } from "./worker/index.js";

// The engine itself, for code that already runs in a worker (advanced).
export {
  Engine,
  type PreparedStatement,
  type Session,
} from "./engine/index.js";

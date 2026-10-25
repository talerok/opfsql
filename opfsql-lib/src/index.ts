// "opfsql": what the page imports — the client of a database served by a
// worker, results, errors and types. It must not load the engine; the
// worker's side is "opfsql/worker" (worker-entry.ts). docs/api.md
// describes both.

export {
  type Connection,
  type Database,
  type DatabaseOptions,
  openDatabase,
  type Statement,
  type WorkerHandle,
} from "./worker/index.js";
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
} from "./engine/api/index.js";
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

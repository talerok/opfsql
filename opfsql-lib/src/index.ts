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
  ConstraintError,
  type ConstraintKind,
  CorruptError,
  DatabaseInUseError,
  DatatypeMismatchError,
  type ErrorCode,
  IndexKeyTooLargeError,
  type InUseReason,
  IntegerOverflowError,
  InternalError,
  IoError,
  MisuseError,
  NotSupportedError,
  OpfsqlError,
  RowTooLargeError,
  SemanticError,
  SerializationError,
  SqlSyntaxError,
  StorageFullError,
  UnsupportedFormatError,
  WorkerError,
} from "./errors.js";
export type { JsonValue, SqlType, Value } from "./values/index.js";

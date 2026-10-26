// ---------------------------------------------------------------------------
// Every error opfsql throws is an OpfsqlError with a stable code. Callers
// branch on `code`, which survives the worker boundary; the message is for
// people and may change. Subclasses add typed details.
// ---------------------------------------------------------------------------

export type ErrorCode =
  // The statement
  /** The SQL text does not parse. */
  | "SYNTAX_ERROR"
  /** It parses, but names, types or clauses do not make sense. */
  | "SEMANTIC_ERROR"
  /** Valid SQL that opfsql does not implement. */
  | "NOT_SUPPORTED"
  // The data
  /** A UNIQUE, PRIMARY KEY or NOT NULL constraint would be broken. */
  | "CONSTRAINT_VIOLATION"
  /** A value does not fit the type it is stored as or cast to. */
  | "DATATYPE_MISMATCH"
  /** An integer result is outside the safe integer range. */
  | "INTEGER_OVERFLOW"
  /** A row, key or value is larger than opfsql can store. */
  | "TOO_LARGE"
  // Concurrency
  /** The write lock was not granted in time. */
  | "BUSY"
  /** Another writer committed since this transaction's snapshot. */
  | "SERIALIZATION_FAILURE"
  /** Another tab has the database open, and this browser allows one. */
  | "DATABASE_IN_USE"
  /** The caller aborted the call, or closed what it ran on, while it waited. */
  | "ABORTED"
  // Storage
  /** Reading or writing the database files failed. */
  | "IO_ERROR"
  /** The browser refused to store more data. */
  | "STORAGE_FULL"
  /** The files are damaged. */
  | "CORRUPT"
  /** The files were written in a format this version does not read. */
  | "UNSUPPORTED_FORMAT"
  // The environment
  /**
   * The worker serving the database failed: it crashed or did not start, or
   * a message between it and the page could not be read or sent.
   */
  | "WORKER_ERROR"
  // The API
  /** The API was used wrongly: a closed session, a missing parameter. */
  | "MISUSE"
  /** A bug in opfsql: a contract inside it was broken. */
  | "INTERNAL";

export class OpfsqlError extends Error {
  override readonly name: string = "OpfsqlError";

  /**
   * In a script of several statements, the position of the one that failed,
   * counting from zero. Set by the engine; absent everywhere else.
   */
  declare readonly statementIndex?: number;

  constructor(
    readonly code: ErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

/** The SQL text does not parse; `offset` and `length` locate the problem. */
export class SqlSyntaxError extends OpfsqlError {
  override readonly name = "SqlSyntaxError";

  constructor(
    message: string,
    readonly offset: number,
    readonly length: number,
  ) {
    super("SYNTAX_ERROR", message);
  }
}

/** The SQL parses, but its names, types or clauses do not make sense. */
export class SemanticError extends OpfsqlError {
  override readonly name = "SemanticError";

  constructor(message: string, options?: ErrorOptions) {
    super("SEMANTIC_ERROR", message, options);
  }
}

/** Valid SQL that opfsql does not implement. */
export class NotSupportedError extends OpfsqlError {
  override readonly name = "NotSupportedError";

  /** `message` when the feature's name alone does not say enough. */
  constructor(
    readonly feature: string,
    message = `${feature} is not supported`,
  ) {
    super("NOT_SUPPORTED", message);
  }
}

/** The API was used wrongly. */
export class MisuseError extends OpfsqlError {
  override readonly name = "MisuseError";

  constructor(message: string) {
    super("MISUSE", message);
  }
}

/** A value does not fit the type it is stored as or cast to. */
export class DatatypeMismatchError extends OpfsqlError {
  override readonly name = "DatatypeMismatchError";

  constructor(message: string, options?: ErrorOptions) {
    super("DATATYPE_MISMATCH", message, options);
  }
}

/** An integer result is outside the safe integer range. */
export class IntegerOverflowError extends OpfsqlError {
  override readonly name = "IntegerOverflowError";

  constructor() {
    super("INTEGER_OVERFLOW", "integer overflow");
  }
}

export type ConstraintKind = "not-null" | "unique" | "primary-key";

/**
 * A row would break a constraint. `columns` names the columns it covers;
 * a unique index over expressions has none, and `index` names it instead.
 */
export class ConstraintError extends OpfsqlError {
  override readonly name = "ConstraintError";

  constructor(
    readonly constraint: ConstraintKind,
    readonly table: string,
    readonly columns: readonly string[],
    readonly index?: string,
  ) {
    super(
      "CONSTRAINT_VIOLATION",
      constraintMessage(constraint, table, columns, index),
    );
  }
}

/** As SQLite words it: "UNIQUE constraint failed: users.email". */
function constraintMessage(
  constraint: ConstraintKind,
  table: string,
  columns: readonly string[],
  index: string | undefined,
): string {
  const kind = constraint === "not-null" ? "NOT NULL" : "UNIQUE";
  return `${kind} constraint failed: ${constrained(table, columns, index)}`;
}

function constrained(
  table: string,
  columns: readonly string[],
  index: string | undefined,
): string {
  if (columns.length === 0) return `index ${index ?? table}`;
  return columns.map((column) => `${table}.${column}`).join(", ");
}

/** A row is larger than a page can hold, even with its values in overflow pages. */
export class RowTooLargeError extends OpfsqlError {
  override readonly name = "RowTooLargeError";

  constructor(
    readonly table: string,
    readonly size: number,
    readonly limit: number,
  ) {
    super(
      "TOO_LARGE",
      `row of table "${table}" is ${String(size)} bytes, exceeds the limit of ${String(limit)} bytes`,
    );
  }
}

/** An index key is larger than its index allows. */
export class IndexKeyTooLargeError extends OpfsqlError {
  override readonly name = "IndexKeyTooLargeError";

  constructor(
    readonly index: string,
    readonly size: number,
    readonly limit: number,
  ) {
    super(
      "TOO_LARGE",
      `key of index "${index}" is ${String(size)} bytes, exceeds the limit of ${String(limit)} bytes`,
    );
  }
}

/** Reading or writing the database files failed. */
export class IoError extends OpfsqlError {
  override readonly name = "IoError";

  constructor(message: string, options?: ErrorOptions) {
    super("IO_ERROR", message, options);
  }
}

/** The browser refused to store more data: the origin's quota is used up. */
export class StorageFullError extends OpfsqlError {
  override readonly name = "StorageFullError";

  constructor(message: string, options?: ErrorOptions) {
    super("STORAGE_FULL", message, options);
  }
}

/** The database files are damaged. */
export class CorruptError extends OpfsqlError {
  override readonly name = "CorruptError";

  constructor(message: string, options?: ErrorOptions) {
    super("CORRUPT", message, options);
  }
}

/** A file was written in a format this version of opfsql does not read. */
export class UnsupportedFormatError extends OpfsqlError {
  override readonly name = "UnsupportedFormatError";

  constructor(
    readonly file: string,
    readonly version: number,
    readonly supported: number,
  ) {
    super(
      "UNSUPPORTED_FORMAT",
      `"${file}" uses file format ${String(version)}, but this opfsql reads format ${String(supported)}`,
    );
  }
}

/**
 * The worker serving the database failed: it crashed or did not start, or a
 * message between it and the page could not be read or sent.
 */
export class WorkerError extends OpfsqlError {
  override readonly name = "WorkerError";

  constructor(message: string, options?: ErrorOptions) {
    super("WORKER_ERROR", message, options);
  }
}

/** A bug: an invariant opfsql relies on does not hold. */
export class InternalError extends OpfsqlError {
  override readonly name = "InternalError";

  constructor(message: string, options?: ErrorOptions) {
    super("INTERNAL", message, options);
  }
}

/** The write lock was not granted within the busy timeout. */
export class BusyError extends OpfsqlError {
  override readonly name = "BusyError";

  constructor(readonly timeout: number) {
    super(
      "BUSY",
      `database is busy: the write lock was not granted within ${String(timeout)} ms`,
    );
  }
}

/**
 * A transaction tried to write after another writer committed since its
 * snapshot, so what it read may be stale. Roll back and run it again.
 */
export class SerializationError extends OpfsqlError {
  override readonly name = "SerializationError";

  constructor() {
    super(
      "SERIALIZATION_FAILURE",
      "could not serialize: the database changed since this transaction began; roll back and retry",
    );
  }
}

/**
 * Why other tabs keep this one from opening a database: this browser lets
 * only one tab open its files (only Chromium allows several), or every
 * read-mark slot is taken.
 */
export type InUseReason = "one-tab-only" | "too-many-tabs";

export class DatabaseInUseError extends OpfsqlError {
  override readonly name = "DatabaseInUseError";

  constructor(
    readonly database: string,
    readonly reason: InUseReason,
    options?: ErrorOptions,
  ) {
    super("DATABASE_IN_USE", inUseMessage(database, reason), options);
  }
}

function inUseMessage(database: string, reason: InUseReason): string {
  if (reason === "too-many-tabs") {
    return `database "${database}" is open in too many tabs`;
  }
  return `database "${database}" is open in another tab, and this browser allows only one tab per database`;
}

/**
 * The call was cancelled while it waited: its signal was aborted, or what it
 * ran on was closed. Named like the DOM's AbortError, so either check works.
 */
export class AbortError extends OpfsqlError {
  override readonly name = "AbortError";

  constructor(message: string, options?: ErrorOptions) {
    super("ABORTED", message, options);
  }
}

/**
 * The OpfsqlError to report for `error`, by its class, never its message:
 * an OpfsqlError itself; a DOMException — how the browser's file and lock
 * APIs fail — STORAGE_FULL when out of quota, IO_ERROR otherwise; anything
 * else is a bug, INTERNAL. The engine and the worker host both translate
 * through here, so one failure gets one code on every path.
 */
export function toOpfsqlError(error: unknown): OpfsqlError {
  if (error instanceof OpfsqlError) return error;
  if (error instanceof DOMException) return fileFailure(error.message, error);
  const message = error instanceof Error ? error.message : String(error);
  return new InternalError(message, { cause: error });
}

/**
 * A file operation the platform failed: STORAGE_FULL when the browser
 * refused more data, which freeing space can cure; IO_ERROR otherwise.
 */
export function fileFailure(
  message: string,
  cause: unknown,
): StorageFullError | IoError {
  const isQuotaExceeded =
    cause instanceof DOMException && cause.name === "QuotaExceededError";
  if (isQuotaExceeded) return new StorageFullError(message, { cause });
  return new IoError(message, { cause });
}

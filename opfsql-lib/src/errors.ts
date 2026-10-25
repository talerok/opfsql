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
  /** A statement failed earlier in this transaction; only ROLLBACK works. */
  | "TRANSACTION_ABORTED"
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
  // The API
  /** The API was used wrongly: a closed session, a missing parameter. */
  | "MISUSE"
  /** A bug in opfsql. */
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

  constructor(message: string) {
    super("SEMANTIC_ERROR", message);
  }
}

/** Valid SQL that opfsql does not implement. */
export class NotSupportedError extends OpfsqlError {
  override readonly name = "NotSupportedError";

  constructor(readonly feature: string) {
    super("NOT_SUPPORTED", `${feature} is not supported`);
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

  constructor(message: string) {
    super("DATATYPE_MISMATCH", message);
  }
}

/** An integer result is outside the safe integer range. */
export class IntegerOverflowError extends OpfsqlError {
  override readonly name = "IntegerOverflowError";

  constructor() {
    super("INTEGER_OVERFLOW", "integer overflow");
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

/** A statement failed earlier in this transaction; only ROLLBACK works. */
export class TransactionAbortedError extends OpfsqlError {
  override readonly name = "TransactionAbortedError";

  constructor(
    message = "the transaction was aborted by an earlier error; roll it back",
  ) {
    super("TRANSACTION_ABORTED", message);
  }
}

/**
 * Another tab has the database open, and this browser lets only one tab open
 * its files: only Chromium supports several tabs per database.
 */
export class DatabaseInUseError extends OpfsqlError {
  override readonly name = "DatabaseInUseError";

  constructor(
    readonly database: string,
    options?: ErrorOptions,
  ) {
    super(
      "DATABASE_IN_USE",
      `database "${database}" is open in another tab, and this browser allows only one tab per database`,
      options,
    );
  }
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

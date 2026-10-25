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

  constructor(
    readonly code: ErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
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

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

/**
 * Another tab has the database open and this browser allows only one, or
 * too many tabs have it open.
 */
export class DatabaseInUseError extends OpfsqlError {
  override readonly name = "DatabaseInUseError";

  constructor(message: string, options?: ErrorOptions) {
    super("DATABASE_IN_USE", message, options);
  }
}

/** Reading or writing the database files failed. */
export class IoError extends OpfsqlError {
  override readonly name = "IoError";

  constructor(message: string, options?: ErrorOptions) {
    super("IO_ERROR", message, options);
  }
}

/** The database files are damaged. */
export class CorruptError extends OpfsqlError {
  override readonly name = "CorruptError";

  constructor(message: string) {
    super("CORRUPT", message);
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

/** A bug: an invariant opfsql relies on does not hold. */
export class InternalError extends OpfsqlError {
  override readonly name = "InternalError";

  constructor(message: string, options?: ErrorOptions) {
    super("INTERNAL", message, options);
  }
}

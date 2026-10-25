import {
  type ErrorCode,
  InternalError,
  OpfsqlError,
  SemanticError,
  SqlSyntaxError,
} from "../errors.js";
import { ExecutorError } from "../executor/errors.js";
import { BindError } from "../sql/binder/index.js";
import { ParseError } from "../sql/parser/index.js";
import {
  IndexKeyTooLargeError,
  PageOverflowError,
  RowTooLargeError,
  StorageError,
  UnsupportedFormatError,
  WalUnknownStateError,
} from "../store/errors.js";

// Everything the engine's API throws is an OpfsqlError. The layers below are
// being rebuilt to throw them directly; until they all do, this is where
// their older errors get a code. An OpfsqlError passes through unchanged.

/**
 * The OpfsqlError to report for `error`, thrown while running `sql` (which
 * locates syntax errors).
 */
export function toOpfsqlError(error: unknown, sql = ""): OpfsqlError {
  if (error instanceof OpfsqlError) return error;
  if (error instanceof ParseError) return syntaxError(error, sql);
  if (error instanceof BindError) return new SemanticError(error.message);
  if (error instanceof ExecutorError) return executorError(error);
  if (error instanceof StorageError) return storageError(error);
  if (error instanceof DOMException) return domError(error);
  if (error instanceof Error && error.message.includes("constraint failed")) {
    return withCode("CONSTRAINT_VIOLATION", error);
  }
  return new InternalError(
    error instanceof Error ? error.message : String(error),
    { cause: error },
  );
}

/** Records which statement of a script `error` came from. */
export function locateStatement(
  error: OpfsqlError,
  statementIndex: number,
): OpfsqlError {
  Object.defineProperty(error, "statementIndex", {
    value: statementIndex,
    enumerable: true,
    configurable: true,
    writable: false,
  });
  return error;
}

function withCode(code: ErrorCode, error: Error): OpfsqlError {
  return new OpfsqlError(code, error.message, { cause: error });
}

function syntaxError(error: ParseError, sql: string): SqlSyntaxError {
  const lines = sql.split("\n").slice(0, Math.max(error.line - 1, 0));
  const lineStart = lines.reduce((offset, line) => offset + line.length + 1, 0);
  return new SqlSyntaxError(
    error.message,
    lineStart + Math.max(error.column - 1, 0),
    error.token.value.length,
  );
}

function executorError(error: ExecutorError): OpfsqlError {
  const message = error.message;
  if (message.includes("Cannot cast") || message.includes("Cannot apply")) {
    return withCode("DATATYPE_MISMATCH", error);
  }
  return message.includes("parameter")
    ? withCode("MISUSE", error)
    : new SemanticError(message);
}

function storageError(error: StorageError): OpfsqlError {
  if (
    error instanceof RowTooLargeError ||
    error instanceof IndexKeyTooLargeError ||
    error instanceof PageOverflowError
  ) {
    return withCode("TOO_LARGE", error);
  }
  if (error instanceof UnsupportedFormatError) {
    return withCode("UNSUPPORTED_FORMAT", error);
  }
  const isForeignFile =
    !(error instanceof WalUnknownStateError) &&
    error.message.includes("is not an opfsql");
  return withCode(isForeignFile ? "CORRUPT" : "IO_ERROR", error);
}

function domError(error: DOMException): OpfsqlError {
  const isFull = error.name === "QuotaExceededError";
  return withCode(isFull ? "STORAGE_FULL" : "IO_ERROR", error);
}

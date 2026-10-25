import {
  type ErrorCode,
  InternalError,
  OpfsqlError,
  SemanticError,
} from "../errors.js";
import { ExecutorError } from "../legacy/executor/errors.js";
import { BindError } from "../legacy/binder/index.js";
import {
  IndexKeyTooLargeError,
  PageOverflowError,
  RowTooLargeError,
  StorageError,
  UnsupportedFormatError,
} from "../store/errors.js";

// Everything the engine's API throws is an OpfsqlError. The layers below are
// being rebuilt to throw them directly; until they all do, this maps their
// older errors to codes — by class, never by message. An OpfsqlError passes
// through unchanged.

/** The OpfsqlError to report for `error`. */
export function toOpfsqlError(error: unknown): OpfsqlError {
  if (error instanceof OpfsqlError) return error;
  if (error instanceof BindError || error instanceof ExecutorError) {
    return new SemanticError(error.message, { cause: error });
  }
  if (error instanceof StorageError) {
    return new OpfsqlError(storageCode(error), error.message, { cause: error });
  }
  if (error instanceof DOMException) {
    return new OpfsqlError(domCode(error), error.message, { cause: error });
  }
  const message = error instanceof Error ? error.message : String(error);
  return new InternalError(message, { cause: error });
}

/**
 * Records which statement of a script `error` came from. Only for an error
 * made for this call: the engine creates every error it reports afresh.
 */
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

const TOO_LARGE = [RowTooLargeError, IndexKeyTooLargeError, PageOverflowError];

function storageCode(error: StorageError): ErrorCode {
  if (TOO_LARGE.some((kind) => error instanceof kind)) return "TOO_LARGE";
  const isOtherFormat = error instanceof UnsupportedFormatError;
  return isOtherFormat ? "UNSUPPORTED_FORMAT" : "IO_ERROR";
}

function domCode(error: DOMException): ErrorCode {
  return error.name === "QuotaExceededError" ? "STORAGE_FULL" : "IO_ERROR";
}

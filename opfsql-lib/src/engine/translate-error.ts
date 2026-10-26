import {
  InternalError,
  IoError,
  OpfsqlError,
  StorageFullError,
} from "../errors.js";

// Everything the engine's API throws is an OpfsqlError. The layers below
// throw them; what else reaches the engine — the browser's DOMExceptions
// from files and locks, a bug's TypeError — is mapped here, by class, never
// by message. An OpfsqlError passes through unchanged.

/** The OpfsqlError to report for `error`. */
export function toOpfsqlError(error: unknown): OpfsqlError {
  if (error instanceof OpfsqlError) return error;
  if (error instanceof DOMException) return fileError(error);
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

/** A file or lock API's failure: STORAGE_FULL when out of quota. */
function fileError(error: DOMException): OpfsqlError {
  const options = { cause: error };
  if (error.name === "QuotaExceededError") {
    return new StorageFullError(error.message, options);
  }
  return new IoError(error.message, options);
}

import type { OpfsqlError } from "../errors.js";

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

import { AbortError, OpfsqlError } from "../errors.js";

/** A signal that follows several others, until disposed. */
export interface LinkedSignal {
  readonly signal: AbortSignal;
  dispose(): void;
}

/**
 * Aborts when the first of `sources` aborts, with that source's reason.
 * Dispose it when the call ends, so long-lived sources keep no listeners.
 */
export function linkSignals(
  sources: readonly (AbortSignal | undefined)[],
): LinkedSignal {
  const controller = new AbortController();
  const present = sources.filter((source) => source !== undefined);
  const aborted = present.find((source) => source.aborted);
  if (aborted !== undefined) {
    controller.abort(aborted.reason);
    return { signal: controller.signal, dispose: nothingToDispose };
  }
  const listeners = present.map((source) => {
    const listener = (): void => {
      controller.abort(source.reason);
    };
    source.addEventListener("abort", listener, { once: true });
    return { source, listener };
  });
  return {
    signal: controller.signal,
    dispose: () => {
      for (const { source, listener } of listeners) {
        source.removeEventListener("abort", listener);
      }
    },
  };
}

function nothingToDispose(): void {
  // no listeners were added
}

/**
 * The error a call fails with when `signal` stops it: a new one for every
 * call, since the engine records on it which statement failed. Our own
 * reasons ("the session was closed") lend it their message.
 */
export function abortErrorOf(signal: AbortSignal): OpfsqlError {
  const reason: unknown = signal.reason;
  const message =
    reason instanceof OpfsqlError ? reason.message : "the call was aborted";
  return new AbortError(message, { cause: reason });
}

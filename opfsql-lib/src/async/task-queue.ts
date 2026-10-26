import { abortErrorOf } from "./signals.js";

/**
 * Runs tasks one at a time, in the order they were queued — what makes a
 * session safe under concurrent async calls. A task whose signal aborts
 * while it waits fails at once and never runs; a running task finishes.
 */
export class TaskQueue {
  private tail: Promise<void> = Promise.resolve();

  async run<T>(task: () => Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) throw abortErrorOf(signal);
    // Queued now, before anything is awaited: the order of calls is kept.
    const previous = this.tail;
    const finished = Promise.withResolvers<undefined>();
    this.tail = inOrder(previous, finished.promise);
    try {
      await turnOf(previous, signal);
      return await task();
    } finally {
      finished.resolve(undefined);
    }
  }

  /** Settles once every task queued so far has finished or been cancelled. */
  async idle(): Promise<void> {
    let tail: Promise<void> | null = null;
    while (tail !== this.tail) {
      tail = this.tail;
      await tail;
    }
  }
}

async function inOrder(
  first: Promise<void>,
  second: Promise<undefined>,
): Promise<void> {
  await first;
  await second;
}

/** Resolves when `previous` has settled; fails early if `signal` aborts. */
async function turnOf(
  previous: Promise<void>,
  signal: AbortSignal,
): Promise<void> {
  const aborted = Promise.withResolvers<never>();
  const abort = (): void => {
    aborted.reject(abortErrorOf(signal));
  };
  signal.addEventListener("abort", abort, { once: true });
  try {
    await Promise.race([previous, aborted.promise]);
  } finally {
    signal.removeEventListener("abort", abort);
  }
  if (signal.aborted) throw abortErrorOf(signal);
}

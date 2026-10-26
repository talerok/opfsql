import { toOpfsqlError } from "../errors.js";
import { abortErrorOf, linkSignals } from "./signals.js";

/** A call waiting for its turn. */
interface Waiter {
  /** Its turn has come: the task before it has finished. */
  start(): void;
}

/**
 * Runs tasks one at a time, in the order they were queued — what makes a
 * session safe under concurrent async calls. A task whose signal aborts
 * while it waits fails at once and never runs; a running task finishes.
 *
 * A task queued on an idle queue starts at once: there is nothing to wait
 * for, so it pays for no promise, listener or race. One queued behind
 * another waits in line, in the order of the calls, until the task before
 * it hands over.
 */
export class TaskQueue {
  /** A task is running: one queued now waits for its turn. */
  private isBusy = false;

  /** Calls waiting for their turn, in the order they were made. */
  private readonly waiting: Waiter[] = [];

  /** What idle() resolves once the queue empties. */
  private readonly onIdle: (() => void)[] = [];

  async run<T>(task: () => Promise<T>, signal: AbortSignal): Promise<T> {
    ensureNotAborted(signal);
    // Queued now, before anything is awaited: the order of calls is kept.
    if (this.isBusy) await this.turn(signal);
    else this.isBusy = true;
    try {
      // Aborted as its turn came: it never runs.
      ensureNotAborted(signal);
      return await task();
    } finally {
      this.handOver();
    }
  }

  /** Settles once the queue is empty: tasks queued after the call included. */
  async idle(): Promise<void> {
    if (!this.isBusy) return;
    const emptied = Promise.withResolvers<undefined>();
    this.onIdle.push(() => {
      emptied.resolve(undefined);
    });
    await emptied.promise;
  }

  /**
   * Waits in line until the task before hands over. Aborting the signal
   * takes the call out of the line and fails it at once.
   */
  private async turn(signal: AbortSignal): Promise<void> {
    const turn = Promise.withResolvers<undefined>();
    const waiter: Waiter = {
      start: () => {
        turn.resolve(undefined);
      },
    };
    const leave = (): void => {
      const position = this.waiting.indexOf(waiter);
      // Already started: a running task finishes.
      if (position === -1) return;
      this.waiting.splice(position, 1);
      turn.reject(abortErrorOf(signal));
    };
    this.waiting.push(waiter);
    signal.addEventListener("abort", leave, { once: true });
    try {
      await turn.promise;
    } finally {
      signal.removeEventListener("abort", leave);
    }
  }

  /** The next call in line starts, or the queue goes idle. */
  private handOver(): void {
    const next = this.waiting.shift();
    if (next !== undefined) {
      next.start();
      return;
    }
    this.isBusy = false;
    for (const resolve of this.onIdle.splice(0)) resolve();
  }
}

/** A call whose signal has aborted never runs. */
function ensureNotAborted(signal: AbortSignal): void {
  if (signal.aborted) throw abortErrorOf(signal);
}

/**
 * Runs `task` in its turn on `queue`, cancelled by any of `signals` while it
 * waits, and given their link while it runs; what it throws becomes an
 * OpfsqlError. How the engine and its sessions run each call.
 */
export async function runInTurn<T>(
  queue: TaskQueue,
  signals: readonly (AbortSignal | undefined)[],
  task: (signal: AbortSignal) => Promise<T> | T,
): Promise<T> {
  const linked = linkSignals(signals);
  try {
    return await queue.run(async () => task(linked.signal), linked.signal);
  } catch (error) {
    throw toOpfsqlError(error);
  } finally {
    linked.dispose();
  }
}

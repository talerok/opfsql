import { BusyError, IoError, OpfsqlError } from "../errors.js";
import { abortErrorOf, linkSignals } from "../async/index.js";

/** How long and until when to wait for the lock. */
export interface Wait {
  readonly signal: AbortSignal;
  /** Milliseconds; Infinity waits for good, 0 only takes a free lock. */
  readonly timeout: number;
}

/** The write lock, held: release it exactly once. */
export interface Lease {
  release(): void;
}

type Grant = PromiseWithResolvers<() => void>;

/**
 * The one writer at a time, among every session of every engine on one
 * database. Each acquisition is a Web Lock request, so waiting is the same
 * within a tab and across tabs: first come, first served, bounded by the busy
 * timeout and cancellable through the signal.
 */
export class WriteLock {
  private isTaken = false;

  constructor(private readonly name: string) {}

  /** True while a session (or the engine) holds the lock. */
  get isHeld(): boolean {
    return this.isTaken;
  }

  async acquire(wait: Wait): Promise<Lease> {
    const release = await requestLock(this.name, wait);
    this.isTaken = true;
    return this.leaseFor(release);
  }

  private leaseFor(release: () => void): Lease {
    let isReleased = false;
    return {
      release: () => {
        if (isReleased) return;
        isReleased = true;
        this.isTaken = false;
        release();
      },
    };
  }
}

/** Resolves with the function that releases the lock, once it is granted. */
async function requestLock(name: string, wait: Wait): Promise<() => void> {
  if (wait.signal.aborted) throw abortErrorOf(wait.signal);
  const isFreeLockOnly = wait.timeout <= 0;
  if (isFreeLockOnly) return holdIfFree(name);
  const timer = busyTimer(wait.timeout);
  const linked = linkSignals([wait.signal, timer.signal]);
  try {
    return await hold(name, { signal: linked.signal });
  } catch (error) {
    throw waitFailure(error, wait, timer.signal);
  } finally {
    timer.dispose();
    linked.dispose();
  }
}

/** Why the wait ended without the lock. */
function waitFailure(
  error: unknown,
  wait: Wait,
  timer: AbortSignal,
): OpfsqlError {
  if (timer.aborted) return new BusyError(wait.timeout);
  if (wait.signal.aborted) return abortErrorOf(wait.signal);
  return lockFailure(error);
}

/** The lock at once, or BUSY. */
async function holdIfFree(name: string): Promise<() => void> {
  try {
    return await hold(name, { ifAvailable: true });
  } catch (error) {
    throw lockFailure(error);
  }
}

/**
 * A lock request the browser failed: the environment's failure, not a bug,
 * so IO_ERROR — as SQLite reports a failed file lock (SQLITE_IOERR_LOCK).
 */
function lockFailure(error: unknown): OpfsqlError {
  if (error instanceof OpfsqlError) return error;
  return new IoError("the write lock could not be requested", {
    cause: error,
  });
}

interface BusyTimer {
  readonly signal: AbortSignal;
  dispose(): void;
}

/** A signal that aborts once `timeout` ms have passed; Infinity never does. */
function busyTimer(timeout: number): BusyTimer {
  const controller = new AbortController();
  if (timeout === Infinity) {
    return { signal: controller.signal, dispose: nothingToDispose };
  }
  const handle = setTimeout(() => {
    controller.abort();
  }, timeout);
  return {
    signal: controller.signal,
    dispose: () => {
      clearTimeout(handle);
    },
  };
}

function nothingToDispose(): void {
  // a timer that never runs has nothing to clear
}

/** Requests the lock; resolves with its release once granted. */
async function hold(name: string, options: LockOptions): Promise<() => void> {
  const grant: Grant = Promise.withResolvers();
  const request = navigator.locks.request(name, options, async (lock) => {
    await keep(lock, grant);
  });
  void rejectOnFailure(request, grant);
  return grant.promise;
}

/**
 * Holds a granted lock until its release is called — the browser keeps the
 * lock until this promise settles. No lock means `ifAvailable` found none.
 */
async function keep(lock: Lock | null, grant: Grant): Promise<void> {
  if (lock === null) {
    grant.reject(new BusyError(0));
    return;
  }
  const released = Promise.withResolvers<undefined>();
  grant.resolve(() => {
    released.resolve(undefined);
  });
  await released.promise;
}

/** A request that fails before it is granted fails the grant. */
async function rejectOnFailure(
  request: Promise<unknown>,
  grant: Grant,
): Promise<void> {
  try {
    await request;
  } catch (error) {
    grant.reject(error);
  }
}

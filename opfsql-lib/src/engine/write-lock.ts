import { BusyError, InternalError, OpfsqlError } from "../errors.js";
import { abortErrorOf, linkSignals } from "./signals.js";

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
  private holder: object | null = null;

  constructor(
    private readonly name: string,
    /** Runs as soon as the lock is granted, before anyone uses it. */
    private readonly onGranted: () => void,
  ) {}

  /** True while a session of this engine holds the lock. */
  get isHeld(): boolean {
    return this.holder !== null;
  }

  async acquire(owner: object, wait: Wait): Promise<Lease> {
    const release = await requestLock(this.name, wait);
    try {
      this.onGranted();
    } catch (error) {
      release();
      throw error;
    }
    this.holder = owner;
    let isReleased = false;
    return {
      release: () => {
        if (isReleased) return;
        isReleased = true;
        this.holder = null;
        release();
      },
    };
  }
}

/** Resolves with the function that releases the lock, once it is granted. */
async function requestLock(name: string, wait: Wait): Promise<() => void> {
  if (wait.signal.aborted) throw abortErrorOf(wait.signal);
  const grant: Grant = Promise.withResolvers();
  if (wait.timeout <= 0) {
    hold(name, { ifAvailable: true }, grant);
    return grant.promise;
  }
  const timer = new AbortController();
  const handle =
    wait.timeout === Infinity
      ? undefined
      : setTimeout(() => {
          timer.abort(new BusyError(wait.timeout));
        }, wait.timeout);
  const linked = linkSignals([wait.signal, timer.signal]);
  hold(name, { signal: linked.signal }, grant);
  try {
    return await grant.promise;
  } catch (error) {
    throw linked.signal.aborted ? abortErrorOf(linked.signal) : error;
  } finally {
    clearTimeout(handle);
    linked.dispose();
  }
}

/** Requests the lock; `grant` settles once it is granted or refused. */
function hold(name: string, options: LockOptions, grant: Grant): void {
  void (async () => {
    try {
      await navigator.locks.request(name, options, async (lock) => {
        if (lock === null) {
          grant.reject(new BusyError(0));
          return;
        }
        const released = Promise.withResolvers<undefined>();
        grant.resolve(() => {
          released.resolve(undefined);
        });
        await released.promise;
      });
    } catch (error) {
      grant.reject(
        error instanceof OpfsqlError
          ? error
          : new InternalError("requesting the write lock failed", {
              cause: error,
            }),
      );
    }
  })();
}

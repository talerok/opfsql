// Node has no Web Locks API. Engines use it to coordinate tabs; tests run in
// one thread, so an in-process lock manager with the spec's grant rules is a
// faithful stand-in: shared and exclusive modes, a fair FIFO queue (a request
// waits behind every earlier one it conflicts with), ifAvailable, and signal
// (aborting it withdraws a request that has not been granted).

type Mode = "shared" | "exclusive";

interface Pending {
  readonly mode: Mode;
  grant(): void;
}

interface LockState {
  readonly held: Mode[];
  readonly queue: Pending[];
}

type RequestArguments =
  [LockGrantedCallback<unknown>] | [LockOptions, LockGrantedCallback<unknown>];

const states = new Map<string, LockState>();

function stateOf(name: string): LockState {
  const known = states.get(name);
  if (known !== undefined) return known;
  const state: LockState = { held: [], queue: [] };
  states.set(name, state);
  return state;
}

function isCompatible(held: readonly Mode[], mode: Mode): boolean {
  return mode === "shared" ? !held.includes("exclusive") : held.length === 0;
}

/** Free now, and no earlier request waits: what ifAvailable may take. */
function isFree(state: LockState, mode: Mode): boolean {
  return state.queue.length === 0 && isCompatible(state.held, mode);
}

/** Grants waiting requests in order, for as long as each fits what is held. */
function pump(state: LockState): void {
  let next = state.queue[0];
  while (next !== undefined && isCompatible(state.held, next.mode)) {
    state.queue.shift();
    next.grant();
    next = state.queue[0];
  }
}

function release(state: LockState, mode: Mode): void {
  state.held.splice(state.held.indexOf(mode), 1);
  pump(state);
}

/**
 * Waits in line for the lock: true once granted, false if the signal
 * withdrew the request first.
 */
async function turnFor(
  state: LockState,
  mode: Mode,
  signal: AbortSignal | undefined,
): Promise<boolean> {
  const granted = Promise.withResolvers<boolean>();
  const pending: Pending = {
    mode,
    grant() {
      signal?.removeEventListener("abort", withdraw);
      state.held.push(mode);
      granted.resolve(true);
    },
  };
  function withdraw(): void {
    const index = state.queue.indexOf(pending);
    // Already granted: the signal no longer matters.
    if (index === -1) return;
    state.queue.splice(index, 1);
    granted.resolve(false);
    pump(state);
  }
  state.queue.push(pending);
  signal?.addEventListener("abort", withdraw, { once: true });
  pump(state);
  return granted.promise;
}

async function request(
  name: string,
  ...rest: RequestArguments
): Promise<unknown> {
  const [options, callback] = rest.length === 1 ? [{}, rest[0]] : rest;
  const mode: Mode = options.mode ?? "exclusive";
  const state = stateOf(name);
  if (options.ifAvailable === true && !isFree(state, mode)) {
    // As in browsers, the callback runs later even when it gets no lock.
    await Promise.resolve();
    return callback(null);
  }
  const { signal } = options;
  // The API rejects with the signal's reason, whatever it is.
  if (signal?.aborted === true) throw signal.reason;
  const isGranted = await turnFor(state, mode, signal);
  if (!isGranted) throw signal?.reason;
  try {
    return await callback({ name, mode });
  } finally {
    release(state, mode);
  }
}

Object.defineProperty(navigator, "locks", {
  value: { request },
  configurable: true,
});

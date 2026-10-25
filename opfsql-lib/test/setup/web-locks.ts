// Node has no Web Locks API. Engines use it to coordinate tabs; tests run in
// one thread, so an in-process lock manager with the spec's grant rules is a
// faithful stand-in: shared and exclusive modes, a fair FIFO queue (a request
// waits behind every earlier one it conflicts with), ifAvailable, and signal
// (aborting it withdraws a request that has not been granted).

type Mode = "shared" | "exclusive";

interface Pending {
  mode: Mode;
  grant(): void;
}

interface LockState {
  held: Mode[];
  queue: Pending[];
}

const states = new Map<string, LockState>();

function stateOf(name: string): LockState {
  let state = states.get(name);
  if (!state) {
    state = { held: [], queue: [] };
    states.set(name, state);
  }
  return state;
}

function compatible(held: Mode[], mode: Mode): boolean {
  return mode === "shared" ? !held.includes("exclusive") : held.length === 0;
}

function pump(state: LockState): void {
  while (state.queue.length && compatible(state.held, state.queue[0].mode)) {
    state.queue.shift()!.grant();
  }
}

function request(
  name: string,
  ...args:
    [LockGrantedCallback<unknown>] | [LockOptions, LockGrantedCallback<unknown>]
): Promise<unknown> {
  const [options, callback] = args.length === 1 ? [{}, args[0]] : args;
  const mode: Mode = options.mode ?? "exclusive";
  const state = stateOf(name);

  if (
    options.ifAvailable &&
    !(state.queue.length === 0 && compatible(state.held, mode))
  ) {
    return Promise.resolve().then(() => callback(null));
  }

  const signal = options.signal;
  if (signal?.aborted === true) return Promise.reject(signal.reason as Error);

  return new Promise((resolve, reject) => {
    const withdraw = () => {
      const index = state.queue.indexOf(pending);
      if (index === -1) return; // already granted: the signal no longer matters
      state.queue.splice(index, 1);
      reject(signal!.reason as Error);
      pump(state);
    };
    const pending: Pending = {
      mode,
      grant() {
        signal?.removeEventListener("abort", withdraw);
        state.held.push(mode);
        const release = () => {
          state.held.splice(state.held.indexOf(mode), 1);
          pump(state);
        };
        Promise.resolve()
          .then(() => callback({ name, mode }))
          .then(
            (value) => {
              release();
              resolve(value);
            },
            (err) => {
              release();
              reject(err);
            },
          );
      },
    };
    state.queue.push(pending);
    signal?.addEventListener("abort", withdraw, { once: true });
    pump(state);
  });
}

Object.defineProperty(navigator, "locks", {
  value: { request },
  configurable: true,
});

// Node has no Web Locks API. Engine.open() uses it to serialize writers across
// tabs; tests are single-threaded, so an in-process exclusive queue per lock
// name is a faithful stand-in.
type LockGrantedCallback = (lock: {
  name: string;
  mode: "exclusive";
}) => unknown;

const tails = new Map<string, Promise<unknown>>();

function request(
  name: string,
  callback: LockGrantedCallback,
): Promise<unknown> {
  const previous = tails.get(name) ?? Promise.resolve();
  const held = previous.then(() => callback({ name, mode: "exclusive" }));
  tails.set(
    name,
    held.catch(() => undefined),
  );
  return held;
}

Object.defineProperty(navigator, "locks", {
  value: { request },
  configurable: true,
});

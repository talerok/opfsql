import type { EngineOpener } from "../../engine/api/index.js";
import { Engine, engineContract } from "../../engine/index.js";
import type { WorkerHandle } from "../channel.js";
import { type HostScope, startHost } from "../host.js";

/**
 * A database worker inside the test's own thread: the real client and the
 * real host, joined by a MessageChannel, whose messages are structured-cloned
 * just as a worker's are.
 */
export interface TestWorker extends WorkerHandle {
  readonly isTerminated: boolean;
  /** Fires the worker's error event, as an exception outside a request does. */
  crash(message: string): void;
  /** Fires messageerror, as a reply that cannot be deserialized does. */
  garble(): void;
}

const openEngine: EngineOpener = async (name, options) =>
  engineContract(await Engine.open(name, options));

export function startTestWorker(open: EngineOpener = openEngine): TestWorker {
  return testWorker((scope) => {
    startHost(scope, open);
  });
}

/** A worker whose script never calls serveDatabase(). */
export function startSilentWorker(): TestWorker {
  return testWorker(() => {
    // the script does nothing
  });
}

function testWorker(script: (scope: HostScope) => void): TestWorker {
  const { port1: page, port2: worker } = new MessageChannel();
  const scope: HostScope = {
    postMessage: (message) => {
      worker.postMessage(message);
    },
    addEventListener: (type, listener) => {
      worker.addEventListener(type, listener);
    },
  };
  worker.start();
  script(scope);

  const events = new EventTarget();
  let isTerminated = false;
  return {
    get isTerminated() {
      return isTerminated;
    },
    postMessage: (message) => {
      page.postMessage(message);
    },
    addEventListener: (type, listener) => {
      if (type === "message") {
        page.addEventListener("message", listener);
        page.start();
      } else {
        events.addEventListener(type, listener);
      }
    },
    terminate: () => {
      isTerminated = true;
      page.close();
      worker.close();
    },
    crash: (message) => {
      events.dispatchEvent(Object.assign(new Event("error"), { message }));
    },
    garble: () => {
      events.dispatchEvent(new Event("messageerror"));
    },
  };
}

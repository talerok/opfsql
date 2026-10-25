import type { OpenOptions } from "../engine/api/index.js";
import {
  AbortError,
  InternalError,
  MisuseError,
  type OpfsqlError,
} from "../errors.js";
import {
  type ClientMessage,
  type HostMessage,
  isReply,
  type Replies,
  type Request,
} from "./protocol.js";
import { fromWireError } from "./wire-error.js";

/**
 * The worker a database talks to: a dedicated `Worker`, or anything that
 * behaves like one.
 */
export interface WorkerHandle {
  postMessage(message: unknown): void;
  addEventListener(type: string, listener: (event: Event) => void): void;
  terminate(): void;
}

interface Pending {
  resolve(value: unknown): void;
  reject(error: OpfsqlError): void;
}

type State = "opening" | "open" | "closing" | "closed" | "failed";

/**
 * How long a worker has to answer its first message. One that never does
 * has a script that does not call serveDatabase(), or failed silently.
 */
export const START_TIMEOUT = 10_000;

/** How long a worker has to close its database before it is terminated. */
export const CLOSE_TIMEOUT = 10_000;

/**
 * Requests and replies over one worker, with the life cycle of the database
 * it serves: opening → open → closing → closed, or failed when the worker
 * breaks. Every pending request settles on every path.
 */
export class Channel {
  private state: State = "opening";
  private failure: OpfsqlError | null = null;
  private readonly pending = new Map<number, Pending>();
  private nextId = 1;
  private closing: Promise<void> | null = null;

  constructor(
    private readonly worker: WorkerHandle,
    private readonly onCheckpointError: OpenOptions["onCheckpointError"],
  ) {
    worker.addEventListener("message", (event) => {
      this.receive(event);
    });
    worker.addEventListener("error", (event) => {
      this.fail(
        new InternalError(`the database worker failed${detail(event)}`),
      );
    });
    worker.addEventListener("messageerror", () => {
      this.fail(
        new InternalError("a message from the database worker was unreadable"),
      );
    });
  }

  /**
   * Asks the worker to close, then terminates it — when it answers, or when
   * it has not within CLOSE_TIMEOUT.
   */
  private async shutdown(): Promise<void> {
    if (this.state === "open") {
      this.state = "closing";
      const deadline = setTimeout(() => {
        this.terminate();
      }, CLOSE_TIMEOUT);
      try {
        await this.request({ type: "close" });
      } catch {
        // the worker is terminated whatever it answers
      } finally {
        clearTimeout(deadline);
      }
    }
    this.state = "closed";
    this.terminate();
  }

  private assertAccepts(request: Request): void {
    if (this.state === "failed" && this.failure !== null) throw this.failure;
    const isOpening = request.type === "hello" || request.type === "open";
    const isAccepted =
      this.state === "open" ||
      (this.state === "opening" && isOpening) ||
      (this.state === "closing" && request.type === "close");
    if (!isAccepted) throw new MisuseError("the database is closed");
  }

  private post(id: number, request: Request): void {
    try {
      this.worker.postMessage({ ...request, id });
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.settle(id, (pending) => {
        pending.reject(
          new MisuseError(
            `the call could not be sent to the worker: ${reason}`,
          ),
        );
      });
    }
  }

  private send(message: ClientMessage): void {
    try {
      this.worker.postMessage(message);
    } catch {
      // a cancel that cannot be sent changes nothing: the call just runs
    }
  }

  private receive(event: Event): void {
    const message = (event as MessageEvent<HostMessage>).data;
    if (!isReply(message)) {
      const error = fromWireError(message.event.error);
      if (this.onCheckpointError === undefined) {
        console.warn("[opfsql] checkpoint failed", error);
      } else {
        this.onCheckpointError(error);
      }
      return;
    }
    this.settle(message.id, (pending) => {
      if (message.ok) pending.resolve(message.value);
      else pending.reject(fromWireError(message.error));
    });
  }

  private settle(id: number, how: (pending: Pending) => void): void {
    const pending = this.pending.get(id);
    if (pending === undefined) return;
    this.pending.delete(id);
    how(pending);
  }

  /** The worker broke: fail everything pending and everything to come. */
  private fail(error: OpfsqlError): void {
    if (this.state === "closed" || this.state === "failed") return;
    this.state = "failed";
    this.failure = error;
    this.worker.terminate();
    this.settleAll(error);
  }

  private settleAll(error: OpfsqlError): void {
    for (const [id, entry] of this.pending) {
      this.pending.delete(id);
      entry.reject(error);
    }
  }

  get isOpen(): boolean {
    return this.state === "open";
  }

  /** Waits for the worker to answer at all, failing after START_TIMEOUT. */
  async greet(): Promise<void> {
    const failure = new InternalError(
      `the database worker did not answer within ${String(START_TIMEOUT)} ms; does its script call serveDatabase()?`,
    );
    const deadline = setTimeout(() => {
      this.fail(failure);
    }, START_TIMEOUT);
    try {
      await this.request({ type: "hello" });
    } finally {
      clearTimeout(deadline);
    }
  }

  /** The database opened: requests other than open may go now. */
  opened(): void {
    if (this.state === "opening") this.state = "open";
  }

  async request<R extends Request>(
    request: R,
    signal?: AbortSignal,
  ): Promise<Replies[R["type"]]> {
    this.assertAccepts(request);
    if (signal?.aborted === true) {
      throw new AbortError("the call was aborted", { cause: signal.reason });
    }
    const id = this.nextId++;
    const reply = Promise.withResolvers<unknown>();
    const cancel = (): void => {
      this.send({ type: "cancel", id });
    };
    signal?.addEventListener("abort", cancel, { once: true });
    this.pending.set(id, { resolve: reply.resolve, reject: reply.reject });
    this.post(id, request);
    try {
      return (await reply.promise) as Replies[R["type"]];
    } finally {
      signal?.removeEventListener("abort", cancel);
    }
  }

  /** Sends a request whose outcome no one waits for: cleanup after GC. */
  release(request: Request): void {
    if (this.state !== "open") return;
    void (async () => {
      try {
        await this.request(request);
      } catch {
        // nothing is waiting for it
      }
    })();
  }

  /** Closes the database and terminates the worker; idempotent. */
  async close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  /** Stops the worker without asking it, as after a failed open. */
  terminate(): void {
    this.worker.terminate();
    this.settleAll(new AbortError("the database was closed"));
  }
}

function detail(event: Event): string {
  return "message" in event && typeof event.message === "string"
    ? `: ${event.message}`
    : "";
}

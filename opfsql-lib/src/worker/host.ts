import { linkSignals, TaskQueue } from "../async/index.js";
import type {
  EngineContract,
  EngineOpener,
  ResultData,
  Schema,
  SessionContract,
  SessionOptions,
  StatementContract,
} from "../engine/api/index.js";
import {
  AbortError,
  MisuseError,
  toOpfsqlError,
  WorkerError,
} from "../errors.js";
import {
  type ClientMessage,
  type HostMessage,
  isClientMessage,
  PROTOCOL_VERSION,
  type Replies,
  type ReplyMessage,
  type Request,
  type RequestMessage,
  type RequestOf,
} from "./protocol.js";
import { toWireError } from "./wire-error.js";

/** The worker's side of the channel: a dedicated worker's global scope. */
export interface HostScope {
  postMessage(message: HostMessage): void;
  addEventListener(
    type: "message",
    listener: (event: MessageEvent<unknown>) => void,
  ): void;
}

/**
 * Serves one database to the client on the other side of `scope`, opening it
 * with `open` when the client asks.
 */
export function startHost(scope: HostScope, open: EngineOpener): void {
  const host = new Host(scope, open);
  scope.addEventListener("message", (event) => {
    host.receive(event.data);
  });
}

interface Connection {
  readonly session: SessionContract;
  readonly statements: Map<number, StatementContract>;
  /** Opened for a statement prepared on the database: it goes with it. */
  readonly isStatementOwned: boolean;
  /** Its calls, run one at a time in the order their messages arrived. */
  readonly calls: TaskQueue;
  /** Aborted when it closes: the calls still waiting fail with ABORTED. */
  readonly lifetime: AbortController;
}

/** Requests that run in their connection's order; the rest run at once. */
const ORDERED = new Set<Request["type"]>([
  "query",
  "execute",
  "prepare",
  "run",
  "finalize",
  "begin",
  "commit",
  "rollback",
  "schema",
]);

type Reply = Replies[Request["type"]];

/** Idle sessions kept for the database's own calls. */
const POOL_SIZE = 4;

/** Sessions that may not keep a transaction past a call. */
const SHARED: SessionOptions = { callScopedTransactions: true };

class Host {
  private state: "idle" | "opening" | "open" | "closing" | "closed" = "idle";

  private engine: EngineContract | null = null;

  private readonly connections = new Map<number, Connection>();

  private readonly pool: SessionContract[] = [];

  /** Requests in progress, to cancel. */
  private readonly running = new Map<number, AbortController>();

  private nextId = 1;

  constructor(
    private readonly scope: HostScope,
    private readonly open: EngineOpener,
  ) {}

  receive(data: unknown): void {
    if (isClientMessage(data)) {
      this.dispatch(data);
      return;
    }
    console.error("[opfsql] the worker got a malformed message", data);
  }

  /**
   * Puts each request in its connection's queue as its message arrives,
   * before anything is awaited: then nothing awaited on the way down to
   * the session can reorder a connection's calls. Other requests run at
   * once.
   */
  private dispatch(message: ClientMessage): void {
    if (message.type === "cancel") {
      this.running
        .get(message.id)
        ?.abort(new AbortError("the call was aborted"));
      return;
    }
    const controller = new AbortController();
    this.running.set(message.id, controller);
    const connection = this.orderedOn(message);
    if (connection === null) {
      void this.reply(message.id, this.answer(message, controller.signal));
      return;
    }
    const linked = linkSignals([controller.signal, connection.lifetime.signal]);
    const turn = connection.calls.run(
      async () => this.answer(message, linked.signal),
      linked.signal,
    );
    void this.reply(message.id, turn, () => {
      linked.dispose();
    });
  }

  /** The connection whose order `message` runs in, if it has one. */
  private orderedOn(message: RequestMessage): Connection | null {
    const id = connectionOf(message);
    if (id === undefined || !ORDERED.has(message.type)) return null;
    return this.connections.get(id) ?? null;
  }

  /** Posts the reply once it is ready; a call that never ran is ABORTED. */
  private async reply(
    id: number,
    answer: Promise<ReplyMessage>,
    dispose: () => void = noDisposal,
  ): Promise<void> {
    let reply: ReplyMessage;
    try {
      reply = await answer;
    } catch (error) {
      reply = { id, ok: false, error: toWireError(toOpfsqlError(error)) };
    } finally {
      dispose();
      this.running.delete(id);
    }
    this.post(reply);
  }

  private async answer(
    message: RequestMessage,
    signal: AbortSignal,
  ): Promise<ReplyMessage> {
    try {
      const value = await this.handle(message, signal);
      return { id: message.id, ok: true, value };
    } catch (error) {
      return {
        id: message.id,
        ok: false,
        error: toWireError(toOpfsqlError(error)),
      };
    }
  }

  private post(message: HostMessage): void {
    try {
      this.scope.postMessage(message);
    } catch (error) {
      if (!("id" in message)) return;
      const failure = new WorkerError("the reply could not be sent", {
        cause: error,
      });
      this.scope.postMessage({
        id: message.id,
        ok: false,
        error: toWireError(failure),
      });
    }
  }

  private async handle(
    message: RequestMessage,
    signal: AbortSignal,
  ): Promise<Reply> {
    switch (message.type) {
      case "hello": {
        return null;
      }
      case "open": {
        return this.openEngine(message);
      }
      case "connect": {
        return { connection: this.connect({}) };
      }
      case "disconnect": {
        return this.disconnect(message.connection);
      }
      case "query": {
        return this.query(message, signal);
      }
      case "execute": {
        return this.execute(message, signal);
      }
      case "prepare": {
        return this.prepare(message, signal);
      }
      case "run": {
        return this.run(message, signal);
      }
      case "finalize": {
        return this.finalize(message);
      }
      case "begin": {
        return this.begin(message, signal);
      }
      case "commit": {
        return this.commit(message, signal);
      }
      case "rollback": {
        return this.rollback(message, signal);
      }
      case "schema": {
        return this.schema(message, signal);
      }
      case "close": {
        return this.close();
      }
    }
  }

  private async openEngine(message: RequestOf<"open">): Promise<null> {
    if (message.protocol !== PROTOCOL_VERSION) {
      throw new MisuseError(
        `the page speaks opfsql protocol ${String(message.protocol)}, this worker ${String(PROTOCOL_VERSION)}: load the same opfsql in both`,
      );
    }
    if (this.state !== "idle") {
      throw new MisuseError("this worker already serves a database");
    }
    this.state = "opening";
    try {
      this.engine = await this.open(message.name, {
        ...message.options,
        onCheckpointError: (error) => {
          this.post({
            type: "event",
            event: { type: "checkpoint-error", error: toWireError(error) },
          });
        },
      });
    } catch (error) {
      this.state = "closed";
      throw error;
    }
    this.state = "open";
    return null;
  }

  private async query(
    message: RequestOf<"query">,
    signal: AbortSignal,
  ): Promise<ResultData> {
    return this.onSession(message.connection, async (session) => {
      const options = { signal };
      const result = await session.query(
        message.sql,
        message.parameters,
        options,
      );
      return result.toData();
    });
  }

  private async execute(
    message: RequestOf<"execute">,
    signal: AbortSignal,
  ): Promise<ResultData[]> {
    return this.onSession(message.connection, async (session) => {
      const results = await session.execute(message.script, { signal });
      return results.map((result) => result.toData());
    });
  }

  /** A statement prepared on the database gets a session of its own. */
  private async prepare(
    message: RequestOf<"prepare">,
    signal: AbortSignal,
  ): Promise<Replies["prepare"]> {
    const isOwned = message.connection === undefined;
    const connection = message.connection ?? this.connect(SHARED, true);
    const { session, statements } = this.connectionAt(connection);
    try {
      const statement = await session.prepare(message.sql, { signal });
      const id = this.nextId++;
      statements.set(id, statement);
      return { statement: id, columns: statement.columns, connection };
    } catch (error) {
      if (isOwned) await this.disconnect(connection);
      throw error;
    }
  }

  private async run(
    message: RequestOf<"run">,
    signal: AbortSignal,
  ): Promise<Replies["run"]> {
    const { statements } = this.connectionAt(message.connection);
    const statement = statements.get(message.statement);
    if (statement === undefined) {
      throw new MisuseError("the statement is closed");
    }
    const result = await statement.query(message.parameters, { signal });
    return { result: result.toData(), columns: statement.columns };
  }

  private async finalize(message: RequestOf<"finalize">): Promise<null> {
    const connection = this.connectionAt(message.connection);
    connection.statements.get(message.statement)?.close();
    connection.statements.delete(message.statement);
    if (connection.isStatementOwned) await this.disconnect(message.connection);
    return null;
  }

  private async begin(
    message: RequestOf<"begin">,
    signal: AbortSignal,
  ): Promise<null> {
    const { session } = this.connectionAt(message.connection);
    await session.begin({ mode: message.mode, signal });
    return null;
  }

  private async commit(
    message: RequestOf<"commit">,
    signal: AbortSignal,
  ): Promise<null> {
    const { session } = this.connectionAt(message.connection);
    await session.commit({ signal });
    return null;
  }

  private async rollback(
    message: RequestOf<"rollback">,
    signal: AbortSignal,
  ): Promise<null> {
    const { session } = this.connectionAt(message.connection);
    await session.rollback({ signal });
    return null;
  }

  private async schema(
    message: RequestOf<"schema">,
    signal: AbortSignal,
  ): Promise<Schema> {
    return this.onSession(message.connection, async (session) =>
      session.schema({ signal }),
    );
  }

  /**
   * Runs `work` on the connection, or — for the database's own calls — on a
   * pooled session, so they never wait for each other.
   */
  private async onSession<T>(
    connection: number | undefined,
    work: (session: SessionContract) => Promise<T>,
  ): Promise<T> {
    if (connection !== undefined) {
      return work(this.connectionAt(connection).session);
    }
    const session = this.pool.pop() ?? this.openSession(SHARED);
    try {
      return await work(session);
    } finally {
      this.returnToPool(session);
    }
  }

  private returnToPool(session: SessionContract): void {
    const isKept = this.state === "open" && this.pool.length < POOL_SIZE;
    if (isKept) this.pool.push(session);
    else void closeQuietly(session);
  }

  private openSession(options: SessionOptions): SessionContract {
    if (this.state !== "open" || this.engine === null) {
      throw new MisuseError("the database is not open");
    }
    return this.engine.connect(options);
  }

  private connect(options: SessionOptions, isStatementOwned = false): number {
    const session = this.openSession(options);
    const id = this.nextId++;
    this.connections.set(id, {
      session,
      statements: new Map<number, StatementContract>(),
      isStatementOwned,
      calls: new TaskQueue(),
      lifetime: new AbortController(),
    });
    return id;
  }

  /** Closes a connection at once: what still waits on it is ABORTED. */
  private async disconnect(id: number): Promise<null> {
    const connection = this.connections.get(id);
    this.connections.delete(id);
    connection?.lifetime.abort(new AbortError("the connection was closed"));
    await connection?.session.close();
    return null;
  }

  private connectionAt(id: number): Connection {
    const connection = this.connections.get(id);
    if (connection === undefined) {
      throw new MisuseError("the connection is closed");
    }
    return connection;
  }

  private async close(): Promise<null> {
    const engine = this.engine;
    this.state = "closing";
    this.engine = null;
    // As disconnect() does for one: calls still waiting on a connection are
    // ABORTED, never run against a connection that is gone. The engine
    // closes the sessions.
    for (const connection of this.connections.values()) {
      connection.lifetime.abort(new AbortError("the database was closed"));
    }
    this.connections.clear();
    this.pool.length = 0;
    await engine?.close();
    this.state = "closed";
    return null;
  }
}

/** The connection a request names; none for the database's own calls. */
function connectionOf(message: RequestMessage): number | undefined {
  return "connection" in message ? message.connection : undefined;
}

function noDisposal(): void {
  // a call outside a connection's order links no signals
}

async function closeQuietly(session: SessionContract): Promise<void> {
  try {
    await session.close();
  } catch {
    // a pooled session that fails to close holds nothing anyone waits for
  }
}

import type {
  EngineContract,
  EngineOpener,
  SessionContract,
  SessionOptions,
  StatementContract,
} from "../engine/api/index.js";
import {
  AbortError,
  InternalError,
  MisuseError,
  OpfsqlError,
} from "../errors.js";
import {
  type ClientMessage,
  type HostMessage,
  isClientMessage,
  type Replies,
  type ReplyMessage,
  type Request,
  type RequestMessage,
  type WireOpenOptions,
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
}

type Reply = Replies[Request["type"]];
type StatementRequest = Extract<
  RequestMessage,
  { type: "prepare" | "run" | "finalize" }
>;
type SessionRequest = Extract<
  RequestMessage,
  { type: "begin" | "commit" | "rollback" | "schema" }
>;

class Host {
  private state: "idle" | "opening" | "open" | "closing" | "closed" = "idle";
  private engine: EngineContract | null = null;
  private readonly connections = new Map<number, Connection>();
  /** Requests in progress, to cancel. */
  private readonly running = new Map<number, AbortController>();
  private nextId = 1;

  constructor(
    private readonly scope: HostScope,
    private readonly open: EngineOpener,
  ) {}

  /**
   * Hands each request to its session at once, before anything is awaited,
   * so a session's calls queue in the order they were sent.
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
    void (async () => {
      const reply = await this.answer(message, controller.signal);
      this.running.delete(message.id);
      this.post(reply);
    })();
  }

  private async answer(
    message: RequestMessage,
    signal: AbortSignal,
  ): Promise<ReplyMessage> {
    try {
      const value = await this.handle(message, signal);
      return { id: message.id, ok: true, value };
    } catch (error) {
      return { id: message.id, ok: false, error: toWireError(asOpfsql(error)) };
    }
  }

  private post(message: HostMessage): void {
    try {
      this.scope.postMessage(message);
    } catch (error) {
      if (!("id" in message)) return;
      const failure = new InternalError("the reply could not be sent", {
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
      case "open": {
        return this.openEngine(message.name, message.options);
      }
      case "connect": {
        return { connection: this.connect({}) };
      }
      case "disconnect": {
        return this.disconnect(message.connection);
      }
      case "query": {
        const { session } = this.connection(message.connection);
        const { sql, parameters } = message;
        const result = await session.query(sql, parameters, { signal });
        return result.toData();
      }
      case "execute": {
        const { session } = this.connection(message.connection);
        const results = await session.execute(message.script, { signal });
        return results.map((result) => result.toData());
      }
      case "prepare":
      case "run":
      case "finalize": {
        return this.handleStatement(message, signal);
      }
      case "begin":
      case "commit":
      case "rollback":
      case "schema": {
        return this.handleSession(message, signal);
      }
      case "close": {
        return this.close();
      }
    }
  }

  private async handleStatement(
    message: StatementRequest,
    signal: AbortSignal,
  ): Promise<Reply> {
    const { session, statements } = this.connection(message.connection);
    switch (message.type) {
      case "prepare": {
        const statement = await session.prepare(message.sql, { signal });
        const id = this.nextId++;
        statements.set(id, statement);
        return { statement: id, columns: statement.columns };
      }
      case "run": {
        const statement = statements.get(message.statement);
        if (statement === undefined) {
          throw new MisuseError("the statement is closed");
        }
        const result = await statement.query(message.parameters, { signal });
        return { result: result.toData(), columns: statement.columns };
      }
      case "finalize": {
        statements.get(message.statement)?.close();
        statements.delete(message.statement);
        return null;
      }
    }
  }

  private async handleSession(
    message: SessionRequest,
    signal: AbortSignal,
  ): Promise<Reply> {
    const { session } = this.connection(message.connection);
    switch (message.type) {
      case "begin": {
        await session.begin({ mode: message.mode, signal });
        return null;
      }
      case "commit": {
        await session.commit({ signal });
        return null;
      }
      case "rollback": {
        await session.rollback({ signal });
        return null;
      }
      case "schema": {
        return session.schema({ signal });
      }
    }
  }

  private async openEngine(
    name: string,
    options: WireOpenOptions,
  ): Promise<Replies["open"]> {
    if (this.state !== "idle") {
      throw new MisuseError("this worker already serves a database");
    }
    this.state = "opening";
    try {
      this.engine = await this.open(name, {
        ...options,
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
    // The database's own connection, shared by everything in the page.
    return { connection: this.connect({ callScopedTransactions: true }) };
  }

  private connect(options: SessionOptions): number {
    if (this.state !== "open" || this.engine === null) {
      throw new MisuseError("the database is not open");
    }
    const id = this.nextId++;
    const session = this.engine.connect(options);
    this.connections.set(id, { session, statements: new Map() });
    return id;
  }

  private async disconnect(id: number): Promise<null> {
    const connection = this.connections.get(id);
    this.connections.delete(id);
    await connection?.session.close();
    return null;
  }

  private connection(id: number): Connection {
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
    this.connections.clear();
    await engine?.close();
    this.state = "closed";
    return null;
  }

  receive(data: unknown): void {
    if (isClientMessage(data)) {
      this.dispatch(data);
      return;
    }
    console.error("[opfsql] the worker got a malformed message", data);
  }
}

function asOpfsql(error: unknown): OpfsqlError {
  if (error instanceof OpfsqlError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new InternalError(message, { cause: error });
}

import {
  type CallOptions,
  type OpenOptions,
  Result,
  type Schema,
  type TransactionOptions,
} from "../engine/api/index.js";
import { MisuseError } from "../errors.js";
import type { Value } from "../values/index.js";
import { Channel, type WorkerHandle } from "./channel.js";
import {
  PROTOCOL_VERSION,
  type Replies,
  type Request,
  type WireOpenOptions,
} from "./protocol.js";

export interface DatabaseOptions extends OpenOptions {
  /**
   * A dedicated module worker whose script calls serveDatabase(). The
   * database owns it: closing the database, or failing to open it,
   * terminates it.
   */
  readonly worker: WorkerHandle;
}

/** Workers that serve a database: one database per worker. */
const claimed = new WeakSet<WorkerHandle>();

/** Runs cleanups for connections and statements dropped without close(). */
const abandoned = new FinalizationRegistry<() => void>(runCleanup);

function runCleanup(cleanup: () => void): void {
  cleanup();
}

/** Opens the database `name` in `options.worker`. */
export async function openDatabase(
  name: string,
  options: DatabaseOptions,
): Promise<Database> {
  if (claimed.has(options.worker)) {
    throw new MisuseError("this worker already serves a database");
  }
  claimed.add(options.worker);
  const channel = new Channel(options.worker, options.onCheckpointError);
  try {
    await channel.greet();
    await channel.request({
      type: "open",
      protocol: PROTOCOL_VERSION,
      name,
      options: wireOptions(options),
    });
    channel.opened();
    return new Database(channel);
  } catch (error) {
    channel.terminate();
    throw error;
  }
}

/** The options that cross to the worker: plain data, no callbacks. */
function wireOptions(options: DatabaseOptions): WireOpenOptions {
  const { busyTimeout, pageSize, checkpointThreshold } = options;
  return {
    ...(busyTimeout !== undefined && { busyTimeout }),
    ...(pageSize !== undefined && { pageSize }),
    ...(checkpointThreshold !== undefined && { checkpointThreshold }),
  };
}

/** A connection's address in the worker. */
interface Link {
  readonly channel: Channel;
  readonly connection: number;
}

/**
 * A database served by a worker. Each of its own calls runs on a session of
 * its own, so they run concurrently and never wait for each other — await
 * one before the next, or use a Connection, where order matters. A call on
 * it must end every transaction it begins.
 */
export class Database implements AsyncDisposable {
  /** openDatabase() opens databases. */
  constructor(private readonly channel: Channel) {}

  async query(
    sql: string,
    parameters: readonly Value[] = [],
    options: CallOptions = {},
  ): Promise<Result> {
    const request = { type: "query", sql, parameters } as const;
    const data = await this.channel.request(request, options.signal);
    return new Result(data);
  }

  async execute(script: string, options: CallOptions = {}): Promise<Result[]> {
    const request = { type: "execute", script } as const;
    const results = await this.channel.request(request, options.signal);
    return results.map((data) => new Result(data));
  }

  /** Prepares a statement on a session of its own, closed with it. */
  async prepare(sql: string, options: CallOptions = {}): Promise<Statement> {
    const request = { type: "prepare", sql } as const;
    const prepared = await this.channel.request(request, options.signal);
    const link = { channel: this.channel, connection: prepared.connection };
    return new Statement(null, link, prepared.statement, prepared.columns);
  }

  /**
   * Runs `work` in a transaction on a connection of its own, which `work`
   * gets and which is closed afterwards: commits, or rolls back if `work`
   * throws.
   */
  async transaction<T>(
    work: (connection: Connection) => Promise<T> | T,
    options?: TransactionOptions,
  ): Promise<T> {
    const connection = await this.connect();
    try {
      return await connection.transaction(work, options);
    } finally {
      await connection.close();
    }
  }

  async connect(): Promise<Connection> {
    const { connection } = await this.channel.request({ type: "connect" });
    return new Connection({ channel: this.channel, connection });
  }

  async schema(options: CallOptions = {}): Promise<Schema> {
    return this.channel.request({ type: "schema" }, options.signal);
  }

  /** Closes every connection and the files, and terminates the worker. */
  async close(): Promise<void> {
    await this.channel.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

/**
 * One session in the worker: its own transaction, its own snapshot. Calls
 * run one at a time in the order they were made. One that is collected
 * without close() is closed for you.
 */
export class Connection implements AsyncDisposable {
  private closing: Promise<void> | null = null;

  constructor(private readonly link: Link) {
    const { channel, connection } = link;
    abandoned.register(
      this,
      () => {
        channel.release({ type: "disconnect", connection });
      },
      this,
    );
  }

  async query(
    sql: string,
    parameters: readonly Value[] = [],
    options: CallOptions = {},
  ): Promise<Result> {
    const { connection } = this.link;
    const data = await this.request(
      { type: "query", connection, sql, parameters },
      options,
    );
    return new Result(data);
  }

  async execute(script: string, options: CallOptions = {}): Promise<Result[]> {
    const { connection } = this.link;
    const results = await this.request(
      { type: "execute", connection, script },
      options,
    );
    return results.map((data) => new Result(data));
  }

  async prepare(sql: string, options: CallOptions = {}): Promise<Statement> {
    const { connection } = this.link;
    const prepared = await this.request(
      { type: "prepare", connection, sql },
      options,
    );
    return new Statement(this, this.link, prepared.statement, prepared.columns);
  }

  /**
   * Begins a transaction (immediate unless `options.mode` says otherwise),
   * runs `work` and commits; rolls back and rethrows if `work` throws.
   */
  async transaction<T>(
    work: (connection: Connection) => Promise<T> | T,
    options: TransactionOptions = {},
  ): Promise<T> {
    const { connection } = this.link;
    const mode = options.mode ?? "immediate";
    await this.request({ type: "begin", connection, mode }, options);
    let value: T;
    try {
      value = await work(this);
    } catch (error) {
      await this.rollbackQuietly();
      throw error;
    }
    await this.request({ type: "commit", connection });
    return value;
  }

  async schema(options: CallOptions = {}): Promise<Schema> {
    const { connection } = this.link;
    return this.request({ type: "schema", connection }, options);
  }

  /** Rolls back its transaction and cancels what waits; idempotent. */
  async close(): Promise<void> {
    this.closing ??= this.disconnect();
    return this.closing;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  /**
   * Sends `request` and waits for its reply on this connection's behalf.
   * While the wait is suspended it keeps the connection reachable, so one
   * nobody else holds is never collected — and closed — under a call still
   * running on it. Every call goes through here.
   */
  private async request<R extends Request>(
    request: R,
    options: CallOptions = {},
  ): Promise<Replies[R["type"]]> {
    if (this.closing !== null) {
      throw new MisuseError("the connection is closed");
    }
    return await this.link.channel.request(request, options.signal);
  }

  private async disconnect(): Promise<void> {
    abandoned.unregister(this);
    const { channel, connection } = this.link;
    if (!channel.isOpen) return;
    await channel.request({ type: "disconnect", connection });
  }

  private async rollbackQuietly(): Promise<void> {
    try {
      await this.request({
        type: "rollback",
        connection: this.link.connection,
      });
    } catch {
      // the error worth reporting is the one that made us roll back
    }
  }
}

/**
 * A statement prepared on a connection, or on the database (then with a
 * session of its own): planned once, planned again by itself when the
 * schema changes.
 */
export class Statement implements AsyncDisposable {
  private closing: Promise<void> | null = null;

  constructor(
    /**
     * The connection it runs on, kept alive while the statement is; null
     * when it was prepared on the database.
     */
    readonly connection: Connection | null,
    private readonly link: Link,
    private readonly id: number,
    private names: readonly string[],
  ) {
    const cleanup = (): void => {
      link.channel.release({
        type: "finalize",
        connection: link.connection,
        statement: id,
      });
    };
    abandoned.register(this, cleanup, this);
  }

  /**
   * Result column names as of the last plan the page has heard of: at
   * prepare, then each run's; empty for writes.
   */
  get columns(): readonly string[] {
    return this.names;
  }

  async query(
    parameters: readonly Value[] = [],
    options: CallOptions = {},
  ): Promise<Result> {
    if (this.closing !== null) {
      throw new MisuseError("the statement is closed");
    }
    const { channel, connection } = this.link;
    const { result, columns } = await channel.request(
      { type: "run", connection, statement: this.id, parameters },
      options.signal,
    );
    this.names = columns;
    return new Result(result);
  }

  async close(): Promise<void> {
    this.closing ??= this.finalize();
    return this.closing;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  private async finalize(): Promise<void> {
    abandoned.unregister(this);
    const { channel, connection } = this.link;
    if (!channel.isOpen) return;
    await channel.request({ type: "finalize", connection, statement: this.id });
  }
}

export type { WorkerHandle } from "./channel.js";

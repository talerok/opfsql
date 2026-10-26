import { runInTurn, TaskQueue } from "../async/index.js";
import {
  AbortError,
  MisuseError,
  type OpfsqlError,
  toOpfsqlError,
} from "../errors.js";
import {
  openOpfsFiles,
  openPager,
  type StorageFiles,
} from "../storage/index.js";
import type {
  CallOptions,
  EngineContract,
  OpenOptions,
  SessionOptions,
} from "./api/index.js";
import { TransactionManager } from "./transaction-manager.js";
import { checkIntegrity } from "./integrity.js";
import { SchemaCache } from "./schemas.js";
import { SessionCore, type SessionHost } from "./session-core.js";
import { Session } from "./session.js";

const DEFAULT_BUSY_TIMEOUT = 5000;

// Set by Engine's static block: the internal ways in, kept off its API.
let construct: (transactions: TransactionManager) => Engine;
let connectWith: (engine: Engine, options: SessionOptions) => Session;

/** Options of an engine on files of one's own: memory files, in tests. */
export interface EngineOptions extends OpenOptions {
  /** The database's name in errors. */
  readonly name?: string;
  /**
   * Prefix of the Web Locks through which engines sharing the files (one
   * per tab) coordinate; files with read marks need one. Without one, the
   * engine's locks are its own.
   */
  readonly lockName?: string;
}

/**
 * The database inside a worker. It makes sessions, owns the write lock they
 * share with every other tab, and closes them all when it closes.
 */
export class Engine implements AsyncDisposable {
  static {
    construct = (transactions) => new Engine(transactions);
    connectWith = (engine, options) => engine.openSession(options);
  }

  private state: "open" | "closing" | "closed" = "open";
  private readonly sessions = new Set<SessionCore>();
  private readonly schemas = new SchemaCache();
  private readonly lifetime = new AbortController();
  private readonly tasks = new TaskQueue();
  private readonly host: SessionHost;
  private closing: Promise<void> | null = null;
  /** Sessions dropped without close() are closed when collected. */
  private readonly abandoned = new FinalizationRegistry(closeAbandoned);

  private constructor(private readonly transactions: TransactionManager) {
    this.host = {
      transactions,
      schemas: this.schemas,
      forget: (core) => {
        this.sessions.delete(core);
        this.abandoned.unregister(core);
      },
    };
  }

  /**
   * Opens a database in the origin's private file system. Use inside a
   * worker. Fails with DATABASE_IN_USE when another tab has it open in a
   * browser that allows only one tab per database (all but Chromium), and
   * with UNSUPPORTED_FORMAT for files of another format version.
   */
  static async open(name: string, options: OpenOptions = {}): Promise<Engine> {
    const files = await openFiles(name);
    return createEngine(files, {
      ...options,
      name,
      lockName: `opfsql:${name}`,
    });
  }

  /** Opens a session. */
  connect(): Session {
    return this.openSession({});
  }

  /**
   * Brings the main file as far forward as the snapshots open in every tab
   * allow, under the write lock, so it cannot race another tab's commit.
   */
  async checkpoint(options: CallOptions = {}): Promise<void> {
    await this.call(options, async (signal) =>
      this.transactions.checkpoint(signal),
    );
  }

  /**
   * Verifies the latest commit: every B-tree is well formed, every page used
   * once or free, every index matches its table. Returns the problems found;
   * an empty list means the database is consistent. Reads a snapshot, as a
   * query does: a writer's uncommitted pages are not in it.
   */
  async checkIntegrity(options: CallOptions = {}): Promise<string[]> {
    return this.call(options, async (signal) => {
      const pages = await this.transactions.beginRead(signal);
      try {
        return checkIntegrity(pages, (read) => this.schemas.load(read).schema);
      } finally {
        pages.end();
      }
    });
  }

  /**
   * Stops taking sessions and calls, closes every session (cancelling what
   * waits, rolling back open transactions, releasing the lock), then closes
   * the files. Calling it again returns the same promise.
   */
  async close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  /**
   * Closes the files whatever closing the sessions ran into, then reports
   * the first failure: a failed rollback must not keep them open.
   */
  private async shutdown(): Promise<void> {
    this.state = "closing";
    const closed = await Promise.allSettled(
      [...this.sessions].map(async (core) => core.close()),
    );
    this.lifetime.abort(new AbortError("the database was closed"));
    await this.tasks.idle();
    await this.transactions.close();
    this.state = "closed";
    const failure = closed.find((outcome) => outcome.status === "rejected");
    if (failure !== undefined) throw toOpfsqlError(failure.reason);
  }

  private assertOpen(): void {
    if (this.state !== "open") throw new MisuseError("the database is closed");
  }

  /** Runs engine-level work one at a time, cancelled by close or `options.signal`. */
  private async call<T>(
    options: CallOptions,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    this.assertOpen();
    return runInTurn(this.tasks, [options.signal, this.lifetime.signal], work);
  }

  private openSession(options: SessionOptions): Session {
    this.assertOpen();
    const core = new SessionCore(this.host, options);
    this.sessions.add(core);
    const session = new Session(core);
    this.abandoned.register(session, core, core);
    return session;
  }
}

/**
 * An engine on files of one's own — memory files, in tests. Engine.open is
 * the public way in. Takes the files: they are closed with the engine, or at
 * once if opening fails.
 */
export async function createEngine(
  files: StorageFiles,
  options: EngineOptions = {},
): Promise<Engine> {
  const lockName = options.lockName ?? `opfsql:private:${crypto.randomUUID()}`;
  const report = options.onCheckpointError ?? warnCheckpointError;
  try {
    const pager = await openPager(files, {
      name: options.name ?? "memory",
      lockName,
      ...(options.pageSize !== undefined && { pageSize: options.pageSize }),
      ...(options.checkpointThreshold !== undefined && {
        checkpointThreshold: options.checkpointThreshold,
      }),
    });
    const transactions = new TransactionManager(pager, {
      lockName,
      busyTimeout: options.busyTimeout ?? DEFAULT_BUSY_TIMEOUT,
      onCheckpointError: report,
    });
    return construct(transactions);
  } catch (error) {
    throw toOpfsqlError(error);
  }
}

/** A session with options the public API does not offer: for the worker host. */
export function connectSession(
  engine: Engine,
  options: SessionOptions,
): Session {
  return connectWith(engine, options);
}

/** The engine as the worker host sees it. */
export function engineContract(engine: Engine): EngineContract {
  return {
    connect: (options = {}) => connectSession(engine, options),
    close: async () => engine.close(),
  };
}

async function openFiles(name: string): Promise<StorageFiles> {
  try {
    return await openOpfsFiles(name);
  } catch (error) {
    throw toOpfsqlError(error);
  }
}

/** What the engine does with a session no one can reach any more. */
function closeAbandoned(core: SessionCore): void {
  void core.close();
}

function warnCheckpointError(error: OpfsqlError): void {
  console.warn(
    "[opfsql] checkpoint failed; it will be retried after the next commit",
    error,
  );
}

import {
  AbortError,
  DatabaseInUseError,
  MisuseError,
  type OpfsqlError,
} from "../errors.js";
import { OPFSSyncStorage } from "../store/backend/opfs-storage.js";
import type { SessionStore } from "../store/session-store.js";
import { Storage } from "../store/storage.js";
import type { SyncIPageStorage } from "../store/types.js";
import type { ISyncFileHandle } from "../store/wal/file-handle.js";
import { ReadMarks } from "../store/wal/read-marks.js";
import { WalStorage } from "../store/wal/wal-storage.js";
import type {
  CallOptions,
  EngineContract,
  OpenOptions,
  SessionOptions,
} from "./api/index.js";
import { checkIndexConsistency } from "./integrity.js";
import { SchemaLoader } from "./schema.js";
import { SessionCore, type SessionHost } from "./session-core.js";
import { Session } from "./session.js";
import { linkSignals } from "./signals.js";
import { TaskQueue } from "./task-queue.js";
import { toOpfsqlError } from "./translate-error.js";
import { WriteLock } from "./write-lock.js";

const DEFAULT_BUSY_TIMEOUT = 5000;

/** What an engine is made of. */
interface EngineParts {
  readonly storage: Storage;
  readonly lockName: string;
  readonly busyTimeout: number;
}

// Set by Engine's static block: the internal ways in, kept off its API.
let construct: (parts: EngineParts) => Engine;
let connectWith: (engine: Engine, options: SessionOptions) => Session;

/** Options of an engine on a storage backend of one's own. */
export interface EngineOptions {
  readonly busyTimeout?: number;
  readonly onCheckpointError?: (error: OpfsqlError) => void;
  /**
   * Prefix of the Web Locks through which engines sharing one database (one
   * per tab or worker) coordinate their writes. Engine.open derives it from
   * the database name; without one, the engine's lock is its own.
   */
  readonly lockName?: string;
}

/**
 * The database inside a worker. It makes sessions, owns the write lock they
 * share with every other tab, and closes them all when it closes.
 */
export class Engine implements AsyncDisposable {
  static {
    construct = (parts) => new Engine(parts);
    connectWith = (engine, options) => engine.openSession(options);
  }

  /**
   * Opens a database in the origin's private file system. Use inside a
   * worker. Fails with DATABASE_IN_USE when another tab has it open in a
   * browser that allows only one tab per database (all but Chromium).
   */
  static async open(name: string, options: OpenOptions = {}): Promise<Engine> {
    try {
      return await openFiles(name, options);
    } catch (error) {
      if (!isHeldByAnotherTab(error)) throw toOpfsqlError(error);
      throw new DatabaseInUseError(name, "one-tab-only", { cause: error });
    }
  }

  private state: "open" | "closing" | "closed" = "open";
  private readonly sessions = new Set<SessionCore>();
  private readonly lock: WriteLock;
  private readonly schemas = new SchemaLoader();
  private readonly lifetime = new AbortController();
  private readonly tasks = new TaskQueue();
  private readonly host: SessionHost;
  private closing: Promise<void> | null = null;
  /** Sessions dropped without close() are closed when collected. */
  private readonly abandoned = new FinalizationRegistry(closeAbandoned);

  private readonly storage: Storage;
  private readonly busyTimeout: number;

  private constructor({ storage, lockName, busyTimeout }: EngineParts) {
    this.storage = storage;
    this.busyTimeout = busyTimeout;
    this.lock = new WriteLock(`${lockName}:write`, () => {
      this.storage.catchUp();
    });
    this.host = this.sessionHost();
  }

  /** What this engine lends its sessions. */
  private sessionHost(): SessionHost {
    return {
      busyTimeout: this.busyTimeout,
      lock: this.lock,
      schemas: this.schemas,
      createStore: () => this.storage.createSession(),
      pin: async (store, signal) => this.pin(store, signal),
      checkpointIfNeeded: () => {
        this.storage.checkpointIfNeeded();
      },
      forget: (core) => {
        this.sessions.delete(core);
        this.abandoned.unregister(core);
      },
    };
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
    this.storage.close();
    this.state = "closed";
    const failure = closed.find((outcome) => outcome.status === "rejected");
    if (failure !== undefined) throw toOpfsqlError(failure.reason);
  }

  private assertOpen(): void {
    if (this.state !== "open") throw new MisuseError("the database is closed");
  }

  /**
   * Picks up other tabs' commits, unless this engine holds the lock (then
   * there are none); true when it found some.
   */
  private catchUp(): boolean {
    return !this.lock.isHeld && this.storage.catchUp();
  }

  /**
   * Pins the latest commit. A snapshot that another tab's checkpoint may
   * already have passed is unsafe: catch up and pin again, as long as
   * catching up finds commits. When it finds none, the checkpoint intent
   * that made the snapshot unsafe is stale; reset it and pin again.
   */
  private async pin(store: SessionStore, signal: AbortSignal): Promise<number> {
    this.catchUp();
    for (;;) {
      const position = this.storage.latestCommit();
      if (store.beginSnapshot()) return position;
      if (!this.catchUp()) await this.resetStaleIntent(signal);
    }
  }

  /** Under the write lock: no checkpoint runs, so no intent is current. */
  private async resetStaleIntent(signal: AbortSignal): Promise<void> {
    if (this.lock.isHeld) {
      this.storage.resetCheckpointIntent();
      return;
    }
    const lease = await this.lock.acquire({
      signal,
      timeout: this.busyTimeout,
    });
    try {
      this.storage.resetCheckpointIntent();
    } finally {
      lease.release();
    }
  }

  /** Runs engine-level work one at a time, cancelled by close or `options.signal`. */
  private async inTurn<T>(
    options: CallOptions,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    this.assertOpen();
    const linked = linkSignals([options.signal, this.lifetime.signal]);
    try {
      return await this.tasks.run(
        async () => work(linked.signal),
        linked.signal,
      );
    } catch (error) {
      throw toOpfsqlError(error);
    } finally {
      linked.dispose();
    }
  }

  private async underLock<T>(
    signal: AbortSignal,
    work: () => Promise<T> | T,
  ): Promise<T> {
    const lease = await this.lock.acquire({
      signal,
      timeout: this.busyTimeout,
    });
    try {
      return await work();
    } finally {
      lease.release();
    }
  }

  /** Checks the latest commit's B-trees, pages and indexes. */
  private async checkCommitted(signal: AbortSignal): Promise<string[]> {
    const store = this.storage.createSession();
    const position = await this.pin(store, signal);
    try {
      const { catalog } = this.schemas.load(store, position);
      const problems = this.storage.checkIntegrity(catalog);
      // Index lookups walk the trees, so they only mean something on sound ones.
      if (problems.length > 0) return problems;
      return checkIndexConsistency(store, catalog);
    } finally {
      store.endSnapshot();
    }
  }

  private openSession(options: SessionOptions): Session {
    this.assertOpen();
    const core = new SessionCore(this.host, options);
    this.sessions.add(core);
    const session = new Session(core);
    this.abandoned.register(session, core, core);
    return session;
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
    await this.inTurn(options, async (signal) => {
      if (this.lock.isHeld) {
        this.storage.checkpoint();
        return;
      }
      await this.underLock(signal, () => {
        this.storage.checkpoint();
      });
    });
  }

  /**
   * Verifies the latest commit: every B-tree is well-formed, every page used
   * once or free, every index matches its table. Returns the problems found;
   * an empty list means the database is consistent. Runs under the write
   * lock, so no writer's uncommitted pages count as leaked.
   */
  async checkIntegrity(options: CallOptions = {}): Promise<string[]> {
    return this.inTurn(options, async (signal) =>
      this.underLock(signal, async () => this.checkCommitted(signal)),
    );
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
}

/**
 * An engine on a storage backend of one's own — for tests. Engine.open is
 * the public way in: snapshot isolation needs a backend that keeps page
 * versions, as the WAL does.
 */
export async function createEngine(
  backend: SyncIPageStorage,
  options: EngineOptions = {},
): Promise<Engine> {
  const report = options.onCheckpointError ?? warnCheckpointError;
  const storage = new Storage(backend, {
    onCheckpointError: (error) => {
      report(toOpfsqlError(error));
    },
  });
  try {
    await storage.open();
  } catch (error) {
    throw toOpfsqlError(error);
  }
  const lockName = options.lockName ?? `opfsql:private:${crypto.randomUUID()}`;
  const busyTimeout = options.busyTimeout ?? DEFAULT_BUSY_TIMEOUT;
  return construct({ storage, lockName, busyTimeout });
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

async function openFiles(name: string, options: OpenOptions): Promise<Engine> {
  const root = await navigator.storage.getDirectory();
  const lockName = `opfsql:${name}`;
  const main = new OPFSSyncStorage(name, { pageSize: options.pageSize });
  const opened: { close(): void }[] = [main];
  try {
    const walHandle = await openShared(root, `${name}.opfsql-wal`);
    opened.push(walHandle);
    const shmHandle = await openShared(root, `${name}.opfsql-shm`);
    const marks = await ReadMarks.open(shmHandle, lockName);
    opened.push(marks);
    const wal = new WalStorage(main, walHandle, {
      checkpointThreshold: options.checkpointThreshold,
      marks,
      fileName: `${name}.opfsql-wal`,
    });
    return await createEngine(wal, { ...options, lockName });
  } catch (error) {
    for (const resource of opened.toReversed()) closeQuietly(resource);
    throw error;
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

/** Opens a file several tabs may hold at once, where the browser allows it. */
async function openShared(
  root: FileSystemDirectoryHandle,
  name: string,
): Promise<ISyncFileHandle> {
  const file = await root.getFileHandle(name, { create: true });
  return file.createSyncAccessHandle({ mode: "readwrite-unsafe" });
}

/**
 * How browsers without concurrent sync handles refuse a second one: Firefox
 * throws NoModificationAllowedError, WebKit InvalidStateError.
 */
function isHeldByAnotherTab(error: unknown): boolean {
  return (
    error instanceof DOMException &&
    (error.name === "NoModificationAllowedError" ||
      error.name === "InvalidStateError")
  );
}

/** Cleanup after a failed open must not hide the error that caused it. */
function closeQuietly(resource: { close(): void }): void {
  try {
    resource.close();
  } catch {
    // the open already failed; its error is the one worth reporting
  }
}

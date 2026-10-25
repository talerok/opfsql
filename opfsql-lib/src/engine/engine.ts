import { Parser } from "../parser/index.js";
import { OPFSSyncStorage } from "../store/backend/opfs-storage.js";
import { Catalog } from "../store/catalog.js";
import { DatabaseInUseError } from "../store/errors.js";
import { Storage } from "../store/storage.js";
import type { SyncIPageStorage } from "../store/types.js";
import { ReadMarks } from "../store/wal/read-marks.js";
import { WalStorage } from "../store/wal/wal-storage.js";
import { checkIndexConsistency } from "./integrity.js";
import { Session } from "./session.js";
import { EngineError } from "./types.js";

export interface EngineOptions {
  /**
   * Called when a background checkpoint fails. The commit that triggered it
   * has already succeeded; the checkpoint is retried after the next commit.
   * Defaults to console.warn.
   */
  onCheckpointError?: (err: unknown) => void;
  /**
   * Prefix of the Web Locks through which engines sharing one database (one
   * per tab or worker) coordinate their writes. Engine.open derives it from
   * the database name; set it when a custom backend is shared that way.
   */
  lockName?: string;
}

export interface OpenOptions extends Omit<EngineOptions, "lockName"> {
  /** Page size in bytes for a newly created database. Default 32 KB. */
  pageSize?: number;
  /** Number of WAL frames after which a commit triggers a checkpoint. Default 1000. */
  checkpointThreshold?: number;
}

// ---------------------------------------------------------------------------
// Engine — session factory + write lock + Web Lock
// ---------------------------------------------------------------------------

export class Engine {
  private catalog!: Catalog;
  private readonly parser = new Parser();
  private writeLockHolder: Session | null = null;
  private webLockRelease: (() => void) | null = null;

  private constructor(
    private readonly storage: Storage,
    private readonly lockName: string | null,
  ) {}

  /**
   * Open a named OPFS database with WAL. Use inside a worker. Throws
   * DatabaseInUseError when another tab has it open in a browser that allows
   * only one tab per database (all but Chromium).
   */
  static async open(
    dbName: string,
    options: OpenOptions = {},
  ): Promise<Engine> {
    const root = await navigator.storage.getDirectory();
    const lockName = `opfsql:${dbName}`;
    const main = new OPFSSyncStorage(dbName, { pageSize: options.pageSize });
    const opened: Array<{ close(): void }> = [main];
    try {
      const walHandle = await openShared(root, `${dbName}.opfsql-wal`);
      opened.push(walHandle);
      const shmHandle = await openShared(root, `${dbName}.opfsql-shm`);
      const marks = await ReadMarks.open(shmHandle, lockName);
      opened.push(marks);
      const wal = new WalStorage(
        main,
        walHandle,
        options.checkpointThreshold,
        marks,
      );
      return await Engine.create(wal, {
        onCheckpointError: options.onCheckpointError,
        lockName,
      });
    } catch (err) {
      for (const resource of opened.reverse()) closeQuietly(resource);
      throw isHeldByAnotherTab(err) ? new DatabaseInUseError(dbName, err) : err;
    }
  }

  /** Create engine with a custom storage backend. */
  static async create(
    backend: SyncIPageStorage,
    options: EngineOptions = {},
  ): Promise<Engine> {
    const storage = new Storage(backend, {
      onCheckpointError: options.onCheckpointError,
    });
    await storage.open();
    const engine = new Engine(storage, options.lockName ?? null);
    engine.catalog = engine.readCatalog();
    return engine;
  }

  // -------------------------------------------------------------------------
  // Session factory
  // -------------------------------------------------------------------------

  createSession(): Session {
    const session: Session = new Session(
      this.storage.createSession(),
      this.parser,
      {
        acquireWriteLock: async () => {
          if (this.lockName) await this.acquireWebLock();
          this.acquireWriteLock(session);
        },
        releaseWriteLock: () => {
          this.webLockRelease?.();
          this.webLockRelease = null;
          this.releaseWriteLock(session);
        },
        catchUp: () => this.catchUp(),
        getCatalog: () => this.catalog,
        onCatalogCommit: (data) => {
          this.catalog = Catalog.deserialize(data);
        },
        maintain: () => this.storage.checkpointIfNeeded(),
      },
    );
    return session;
  }

  catchUp(): void {
    if (this.writeLockHolder !== null) {
      return;
    }
    if (this.storage.catchUp()?.catalog) {
      this.catalog = this.readCatalog();
    }
  }

  private readCatalog(): Catalog {
    const data = this.storage.readCatalog();
    return data ? Catalog.deserialize(data) : new Catalog();
  }

  /**
   * Brings the main file as far forward as the snapshots open in every tab
   * allow. Holds the write lock, so restarting the WAL cannot race another
   * tab's commit.
   */
  async checkpoint(): Promise<void> {
    if (!this.lockName || this.webLockRelease) {
      this.storage.checkpoint();
      return;
    }
    await navigator.locks.request(`${this.lockName}:write`, () => {
      this.catchUp();
      this.storage.checkpoint();
    });
  }

  /**
   * Verifies the committed database: every B-tree is well-formed, every page
   * is used exactly once or free, and every index matches its table.
   * Returns the problems found; an empty list means the database is consistent.
   */
  checkIntegrity(): string[] {
    this.catchUp();
    const problems = this.storage.checkIntegrity(this.catalog);
    // Index lookups walk the trees, so they are only meaningful on sound ones.
    if (problems.length > 0) return problems;
    return checkIndexConsistency(this.storage.createSession(), this.catalog);
  }

  close(): void {
    this.storage.close();
  }

  // -------------------------------------------------------------------------
  // In-process write lock
  // -------------------------------------------------------------------------

  private acquireWriteLock(session: Session): void {
    if (this.writeLockHolder === session) return;
    if (this.writeLockHolder !== null) {
      throw new EngineError("database is locked");
    }
    this.writeLockHolder = session;
  }

  private releaseWriteLock(session: Session): void {
    if (this.writeLockHolder === session) {
      this.writeLockHolder = null;
    }
  }

  // -------------------------------------------------------------------------
  // Cross-tab Web Lock (only active when a lockName is set)
  // -------------------------------------------------------------------------

  private async acquireWebLock(): Promise<void> {
    if (this.webLockRelease) {
      return;
    }

    return new Promise<void>((acquired, failed) => {
      navigator.locks.request(`${this.lockName}:write`, () => {
        try {
          this.catchUp();
        } catch (err) {
          failed(err);
          return Promise.resolve();
        }

        acquired();
        return new Promise<void>((release) => {
          this.webLockRelease = release;
        });
      });
    });
  }
}

/** Opens a file several tabs may hold at once, where the browser allows it. */
async function openShared(root: FileSystemDirectoryHandle, name: string) {
  const file = await root.getFileHandle(name, { create: true });
  return file.createSyncAccessHandle({ mode: "readwrite-unsafe" });
}

/**
 * How browsers without concurrent sync handles refuse a second one: Firefox
 * throws NoModificationAllowedError, WebKit InvalidStateError.
 */
function isHeldByAnotherTab(err: unknown): boolean {
  return (
    err instanceof DOMException &&
    (err.name === "NoModificationAllowedError" ||
      err.name === "InvalidStateError")
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

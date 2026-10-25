import { Parser } from "../parser/index.js";
import { OPFSSyncStorage } from "../store/backend/opfs-storage.js";
import { Catalog } from "../store/catalog.js";
import { Storage } from "../store/storage.js";
import type { CatalogData, SyncIPageStorage } from "../store/types.js";
import { WalStorage } from "../store/wal/wal-storage.js";
import { Session } from "./session.js";
import { EngineError } from "./types.js";

export interface EngineOptions {
  /**
   * Called when a background checkpoint fails. The commit that triggered it
   * has already succeeded; the checkpoint is retried after the next commit.
   * Defaults to console.warn.
   */
  onCheckpointError?: (err: unknown) => void;
}

export interface OpenOptions extends EngineOptions {
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
  private dbName: string | null = null;
  private webLockRelease: (() => void) | null = null;

  private constructor(private readonly storage: Storage) {}

  /** Open a named OPFS database with WAL. Use inside a worker. */
  static async open(
    dbName: string,
    options: OpenOptions = {},
  ): Promise<Engine> {
    const mainStorage = new OPFSSyncStorage(dbName, {
      pageSize: options.pageSize,
    });
    const root = await navigator.storage.getDirectory();
    const walFh = await root.getFileHandle(`${dbName}.opfsql-wal`, {
      create: true,
    });
    const walHandle = await walFh.createSyncAccessHandle({
      mode: "readwrite-unsafe",
    });

    const wal = new WalStorage(
      mainStorage,
      walHandle,
      options.checkpointThreshold,
    );
    const engine = await Engine.create(wal, options);
    engine.dbName = dbName;
    return engine;
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
    const engine = new Engine(storage);
    engine.catalog = engine.readCatalog();
    return engine;
  }

  // -------------------------------------------------------------------------
  // Session factory
  // -------------------------------------------------------------------------

  createSession(): Session {
    const sessionStore = this.storage.createSession();

    const acquireLock = async () => {
      if (this.dbName) await this.acquireWebLock();
      this.acquireWriteLock(session);
    };

    const releaseLock = () => {
      this.webLockRelease?.();
      this.webLockRelease = null;
      this.releaseWriteLock(session);
    };

    const onCommit = (data: CatalogData) => {
      this.catalog = Catalog.deserialize(data);
    };

    const session: Session = new Session(
      sessionStore,
      this.parser,
      () => acquireLock(),
      () => releaseLock(),
      () => this.catchUp(),
      () => this.catalog,
      (d) => onCommit(d),
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

  checkpoint(): void {
    this.storage.checkpoint();
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
  // Cross-tab Web Lock (only active when opened via Engine.open)
  // -------------------------------------------------------------------------

  private async acquireWebLock(): Promise<void> {
    if (this.webLockRelease) {
      return;
    }

    return new Promise<void>((acquired, failed) => {
      const lockName = `opfsql:${this.dbName}:write`;
      navigator.locks.request(lockName, () => {
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

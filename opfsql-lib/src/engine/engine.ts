import { Parser } from "../parser/index.js";
import { OPFSSyncStorage } from "../store/backend/opfs-storage.js";
import { Catalog, initCatalog } from "../store/catalog.js";
import { SessionPageStore } from "../store/page-manager.js";
import { Storage } from "../store/storage.js";
import type { CatalogData, SyncIPageStorage } from "../store/types.js";
import { WalStorage } from "../store/wal/wal-storage.js";
import { Session } from "./session.js";
import { EngineError } from "./types.js";

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
  static async open(dbName: string): Promise<Engine> {
    const mainStorage = new OPFSSyncStorage(dbName);
    const root = await navigator.storage.getDirectory();
    const walFh = await root.getFileHandle(`${dbName}.opfsql-wal`, {
      create: true,
    });
    const walHandle = await walFh.createSyncAccessHandle({
      mode: "readwrite-unsafe",
    });

    const wal = new WalStorage(mainStorage, walHandle);
    const engine = await Engine.create(wal);
    engine.dbName = dbName;
    return engine;
  }

  /** Create engine with a custom storage backend. */
  static async create(backend: SyncIPageStorage): Promise<Engine> {
    const storage = new Storage(backend);
    await storage.open();
    const engine = new Engine(storage);
    engine.catalog = initCatalog(storage.pageStore);
    return engine;
  }

  // -------------------------------------------------------------------------
  // Session factory
  // -------------------------------------------------------------------------

  createSession(): Session {
    const ps = new SessionPageStore(this.storage.pageStore);

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
      ps,
      this.parser,
      acquireLock,
      releaseLock,
      () => this.catchUp(),
      () => this.catalog,
      (data) => onCommit(data),
    );
    return session;
  }

  catchUp(): void {
    if (this.writeLockHolder !== null) {
      return;
    }
    if (this.storage.catchUp()) {
      const pageStore = this.storage.pageStore;
      this.catalog = initCatalog(pageStore);
    }
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

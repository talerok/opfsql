import { Parser } from "../parser/index.js";
import { OPFSSyncStorage } from "../store/backend/opfs-storage.js";
import { Catalog, initCatalog } from "../store/catalog.js";
import { SessionPageStore } from "../store/page-manager.js";
import { Storage } from "../store/storage.js";
import type { SyncIPageStorage } from "../store/types.js";
import { WalStorage } from "../store/wal/wal-storage.js";
import { Session } from "./session.js";
import { EngineError } from "./types.js";

// ---------------------------------------------------------------------------
// Engine — session factory + write lock
// ---------------------------------------------------------------------------

export class Engine {
  private catalog!: Catalog;
  private readonly parser = new Parser();
  private writeLockHolder: Session | null = null;

  private constructor(private readonly storage: Storage) {}

  /** Open a named OPFS database with WAL. Use inside a worker. */
  static async open(dbName: string): Promise<Engine> {
    try {
      const mainStorage = new OPFSSyncStorage(dbName);
      const root = await navigator.storage.getDirectory();
      const walFh = await root.getFileHandle(`${dbName}.opfsql-wal`, {
        create: true,
      });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const walHandle = await (walFh as any).createSyncAccessHandle();
      return await Engine.create(new WalStorage(mainStorage, walHandle));
    } catch (err) {
      if (
        err instanceof DOMException &&
        (err.name === "NoModificationAllowedError" ||
          err.name === "InvalidStateError")
      ) {
        throw new EngineError("database is busy");
      }
      throw err;
    }
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
    const session: Session = new Session(
      ps,
      this.parser,
      () => this.acquireWriteLock(session),
      () => this.releaseWriteLock(session),
      () => this.catalog,
      (data) => {
        this.catalog = Catalog.deserialize(data);
      },
    );
    return session;
  }

  close(): void {
    this.storage.close();
  }

  // -------------------------------------------------------------------------
  // Write lock
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
}

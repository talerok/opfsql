import { Binder } from "../binder/index.js";
import { execute } from "../executor/executor.js";
import type { CatalogChange } from "../executor/types.js";
import { optimize } from "../optimizer/index.js";
import { Parser } from "../parser/index.js";
import {
  StatementType,
  TransactionType,
  type ExplainStatement,
  type Statement,
  type TransactionStatement,
} from "../parser/types.js";
import { OPFSSyncStorage } from "../store/backend/opfs-storage.js";
import { Catalog, initCatalog, writeCatalog } from "../store/catalog.js";
import { SyncIndexManager } from "../store/index-manager.js";
import { Storage } from "../store/storage.js";
import { SyncTableManager } from "../store/table-manager.js";
import type { CatalogData, SyncIPageStorage } from "../store/types.js";
import { WalStorage } from "../store/wal/wal-storage.js";
import type { Value } from "../types.js";
import { formatPlan } from "./explain.js";
import { EngineError, PreparedStatement, type Result } from "./types.js";

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

export class Engine {
  private catalog!: Catalog;
  private binder!: Binder;
  private readonly parser = new Parser();

  private inTransaction = false;
  private transactionAborted = false;
  private catalogSnapshot: CatalogData | null = null;
  private catalogDirty = false;

  private constructor(private readonly storage: Storage) {}

  /** Open a named OPFS database with WAL. Use inside a worker. */
  static async open(dbName: string): Promise<Engine> {
    const mainStorage = new OPFSSyncStorage(dbName);
    const root = await navigator.storage.getDirectory();
    const walFh = await root.getFileHandle(`${dbName}.opfsql-wal`, {
      create: true,
    });
    // createSyncAccessHandle is part of the OPFS private-file-system API and
    // is missing from older TypeScript DOM lib versions, so we cast to any.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const walHandle = await (walFh as any).createSyncAccessHandle();
    return Engine.create(new WalStorage(mainStorage, walHandle));
  }

  /** Create engine with a custom storage backend. */
  static async create(backend: SyncIPageStorage): Promise<Engine> {
    const storage = new Storage(backend);
    await storage.open();
    const engine = new Engine(storage);
    engine.catalog = initCatalog(storage.pageStore);

    storage.rowManager = new SyncTableManager(
      storage.pageStore,
      () => engine.catalog,
    );

    storage.indexManager = new SyncIndexManager(
      storage.pageStore,
      () => engine.catalog,
    );

    engine.binder = new Binder(engine.catalog);
    return engine;
  }

  // -------------------------------------------------------------------------
  // Public API — sync after open()
  // -------------------------------------------------------------------------

  execute(sql: string, params?: Value[]): Result[] {
    const statements = this.parser.parse(sql);
    return statements.map((stmt) => this.executeOne(stmt, params));
  }

  prepare(sql: string): PreparedStatement {
    const stmts = this.parser.parse(sql);
    if (stmts.length !== 1)
      throw new EngineError("prepare() requires exactly one statement");
    const stmt = stmts[0];
    return new PreparedStatement((params) => this.executeOne(stmt, params));
  }

  close(): void {
    this.storage.close();
  }

  getSchema(): CatalogData {
    return this.catalog.snapshot();
  }

  // -------------------------------------------------------------------------
  // Statement dispatch
  // -------------------------------------------------------------------------

  private executeOne(stmt: Statement, params?: Value[]): Result {
    if (stmt.type === StatementType.TRANSACTION_STATEMENT) {
      return this.executeTCL(stmt as TransactionStatement);
    }

    // EXPLAIN is allowed even inside an aborted transaction (useful for debugging).
    if (stmt.type === StatementType.EXPLAIN_STATEMENT) {
      const inner = (stmt as ExplainStatement).statement;
      const bound = this.binder.bindStatement(inner);
      const optimized = optimize(bound, this.catalog);
      return { type: "rows", rows: [{ plan: formatPlan(optimized) }] };
    }

    if (this.transactionAborted) {
      throw new EngineError(
        "current transaction is aborted, commands ignored until end of transaction block",
      );
    }

    const autocommit = !this.inTransaction;
    if (autocommit) this.catalogSnapshot = this.catalog.serialize();

    try {
      const result = this.runPipeline(stmt, params);

      if (autocommit) {
        if (this.catalogDirty) {
          this.writeCatalog();
          this.catalogDirty = false;
        }
        this.storage.pageStore.commit();
        this.catalogSnapshot = null;
      }

      return result;
    } catch (err) {
      this.storage.pageStore.rollback();
      this.catalogDirty = false;
      if (this.catalogSnapshot) {
        this.catalog = Catalog.deserialize(this.catalogSnapshot);
        this.binder = new Binder(this.catalog);
      }
      if (autocommit) {
        this.catalogSnapshot = null;
      } else {
        this.transactionAborted = true;
      }
      throw err;
    }
  }

  // -------------------------------------------------------------------------
  // TCL
  // -------------------------------------------------------------------------

  private executeTCL(stmt: TransactionStatement): Result {
    switch (stmt.transaction_type) {
      case TransactionType.BEGIN:
        return this.beginTransaction();
      case TransactionType.COMMIT:
        return this.commitTransaction();
      case TransactionType.ROLLBACK:
        return this.rollbackTransaction();
      default: {
        const unreachable: never = stmt.transaction_type;
        throw new EngineError(`Unknown transaction type: ${unreachable}`);
      }
    }
  }

  private beginTransaction(): Result {
    if (this.inTransaction) {
      throw new EngineError("already in a transaction");
    }
    this.catalogSnapshot = this.catalog.serialize();
    this.inTransaction = true;
    return { type: "ok" };
  }

  private commitTransaction(): Result {
    if (!this.inTransaction) {
      return { type: "ok" };
    }
    if (this.transactionAborted) {
      this.catalogSnapshot = null;
      this.inTransaction = false;
      this.transactionAborted = false;
      throw new EngineError(
        "current transaction is aborted, COMMIT treated as ROLLBACK",
      );
    }
    if (this.catalogDirty) {
      this.writeCatalog();
      this.catalogDirty = false;
    }
    this.storage.pageStore.commit();
    this.catalogSnapshot = null;
    this.inTransaction = false;
    return { type: "ok" };
  }

  private rollbackTransaction(): Result {
    if (!this.inTransaction) {
      return { type: "ok" };
    }
    if (!this.transactionAborted) {
      this.storage.pageStore.rollback();
      if (this.catalogSnapshot) {
        this.catalog = Catalog.deserialize(this.catalogSnapshot);
        this.binder = new Binder(this.catalog);
      }
    }
    this.catalogDirty = false;
    this.catalogSnapshot = null;
    this.inTransaction = false;
    this.transactionAborted = false;
    return { type: "ok" };
  }

  // -------------------------------------------------------------------------
  // Pipeline
  // -------------------------------------------------------------------------

  private runPipeline(stmt: Statement, params?: readonly Value[]): Result {
    const bound = this.binder.bindStatement(stmt);
    const optimized = optimize(bound, this.catalog);
    const result = execute(
      optimized,
      this.storage.rowManager,
      this.catalog,
      this.storage.indexManager,
      params,
    );

    const hasCatalogChanges = result.catalogChanges.length > 0;
    for (const change of result.catalogChanges) {
      this.applyCatalogChange(change);
    }
    if (hasCatalogChanges) {
      this.binder = new Binder(this.catalog);
    }
    if (hasCatalogChanges || result.catalogDirty) {
      this.catalogDirty = true;
    }

    if (stmt.type === StatementType.SELECT_STATEMENT) {
      return { type: "rows", rows: result.rows };
    }

    return { type: "ok", rowsAffected: result.rowsAffected };
  }

  // -------------------------------------------------------------------------
  // Catalog mutations
  // -------------------------------------------------------------------------

  private applyCatalogChange(change: CatalogChange): void {
    switch (change.type) {
      case "CREATE_TABLE":
        this.catalog.addTable(change.schema);
        break;
      case "DROP_TABLE":
        this.catalog.removeTable(change.name);
        break;
      case "ALTER_TABLE":
        this.catalog.updateTable(change.after);
        break;
      case "CREATE_INDEX":
        this.catalog.addIndex(change.index);
        break;
      case "DROP_INDEX":
        this.catalog.removeIndex(change.name);
        break;
    }
  }

  private writeCatalog(): void {
    writeCatalog(this.catalog, this.storage.pageStore);
  }
}

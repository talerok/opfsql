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
import { Catalog, writeCatalog } from "../store/catalog.js";
import { SyncIndexManager } from "../store/index-manager.js";
import { SyncTableManager } from "../store/table-manager.js";
import type {
  CatalogData,
  SyncIIndexManager,
  SyncIPageStore,
  SyncIRowManager,
} from "../store/types.js";
import type { Value } from "../types.js";
import { formatPlan } from "./explain.js";
import { EngineError, PreparedStatement, type Result } from "./types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const WRITE_STATEMENTS = new Set<StatementType>([
  StatementType.INSERT_STATEMENT,
  StatementType.UPDATE_STATEMENT,
  StatementType.DELETE_STATEMENT,
  StatementType.CREATE_TABLE_STATEMENT,
  StatementType.CREATE_INDEX_STATEMENT,
  StatementType.ALTER_TABLE_STATEMENT,
  StatementType.DROP_STATEMENT,
]);

function isWriteStatement(type: StatementType): boolean {
  return WRITE_STATEMENTS.has(type);
}

// ---------------------------------------------------------------------------
// Session — per-connection SQL execution
// ---------------------------------------------------------------------------

export class Session {
  private binder!: Binder;
  private readonly rowManager: SyncIRowManager;
  private readonly indexManager: SyncIIndexManager;

  private catalog!: Catalog;
  private inTransaction = false;
  private transactionAborted = false;
  private catalogSnapshot: CatalogData | null = null;
  private catalogDirty = false;
  private holdingWriteLock = false;

  constructor(
    private readonly pageStore: SyncIPageStore,
    private readonly parser: Parser,
    private readonly acquireWriteLock: () => void,
    private readonly releaseWriteLock: () => void,
    private readonly getCatalog: () => Catalog,
    private readonly onCatalogCommit: (data: CatalogData) => void,
  ) {
    this.refreshCatalog();
    this.rowManager = new SyncTableManager(pageStore, () => this.catalog);
    this.indexManager = new SyncIndexManager(pageStore, () => this.catalog);
  }

  /** Pull the latest committed catalog from the engine. */
  private refreshCatalog(): void {
    this.catalog = Catalog.deserialize(this.getCatalog().serialize());
    this.binder = new Binder(this.catalog);
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  execute(sql: string, params?: Value[]): Result[] {
    if (!this.inTransaction) {
      this.refreshCatalog();
    }
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

  getSchema(): CatalogData {
    return this.catalog.snapshot();
  }

  close(): void {
    if (this.inTransaction) {
      this.rollbackTransaction();
    }
    if (this.holdingWriteLock) {
      this.releaseWriteLock();
      this.holdingWriteLock = false;
    }
  }

  // -------------------------------------------------------------------------
  // Statement dispatch
  // -------------------------------------------------------------------------

  private executeOne(stmt: Statement, params?: Value[]): Result {
    if (stmt.type === StatementType.TRANSACTION_STATEMENT) {
      return this.executeTCL(stmt as TransactionStatement);
    }

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

    // Acquire write lock on first write statement
    if (isWriteStatement(stmt.type) && !this.holdingWriteLock) {
      this.acquireWriteLock();
      this.holdingWriteLock = true;
    }

    const autocommit = !this.inTransaction;
    if (autocommit) this.catalogSnapshot = this.catalog.serialize();

    try {
      const result = this.runPipeline(stmt, params);

      if (autocommit) {
        if (this.catalogDirty) {
          this.commitCatalog();
          this.catalogDirty = false;
        }
        this.pageStore.commit();
        this.catalogSnapshot = null;
        if (this.holdingWriteLock) {
          this.releaseWriteLock();
          this.holdingWriteLock = false;
        }
      }

      return result;
    } catch (err) {
      this.pageStore.rollback();
      this.catalogDirty = false;
      if (this.catalogSnapshot) {
        this.catalog = Catalog.deserialize(this.catalogSnapshot);
        this.binder = new Binder(this.catalog);
      }
      if (autocommit) {
        this.catalogSnapshot = null;
        if (this.holdingWriteLock) {
          this.releaseWriteLock();
          this.holdingWriteLock = false;
        }
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
      if (this.holdingWriteLock) {
        this.releaseWriteLock();
        this.holdingWriteLock = false;
      }
      throw new EngineError(
        "current transaction is aborted, COMMIT treated as ROLLBACK",
      );
    }
    if (this.catalogDirty) {
      this.commitCatalog();
      this.catalogDirty = false;
    }
    this.pageStore.commit();
    this.catalogSnapshot = null;
    this.inTransaction = false;
    if (this.holdingWriteLock) {
      this.releaseWriteLock();
      this.holdingWriteLock = false;
    }
    return { type: "ok" };
  }

  private rollbackTransaction(): Result {
    if (!this.inTransaction) {
      return { type: "ok" };
    }
    if (!this.transactionAborted) {
      this.pageStore.rollback();
      if (this.catalogSnapshot) {
        this.catalog = Catalog.deserialize(this.catalogSnapshot);
        this.binder = new Binder(this.catalog);
      }
    }
    this.catalogDirty = false;
    this.catalogSnapshot = null;
    this.inTransaction = false;
    this.transactionAborted = false;
    if (this.holdingWriteLock) {
      this.releaseWriteLock();
      this.holdingWriteLock = false;
    }
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
      this.rowManager,
      this.catalog,
      this.indexManager,
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

  private commitCatalog(): void {
    writeCatalog(this.catalog, this.pageStore);
    this.onCatalogCommit(this.catalog.serialize());
  }
}

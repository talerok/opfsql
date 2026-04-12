import { Binder } from '../binder/index.js';
import { execute } from '../executor/executor.js';
import type { CatalogChange } from '../executor/types.js';
import { optimize } from '../optimizer/index.js';
import { Parser } from '../parser/index.js';
import {
  StatementType,
  TransactionType,
  type Statement,
  type TransactionStatement,
} from '../parser/types.js';
import { Catalog, initCatalog, serializeCatalogEntry } from '../store/catalog.js';
import { Storage } from '../store/storage.js';
import type { CatalogData, Row, SyncIStorage } from '../store/types.js';

// ---------------------------------------------------------------------------

export interface Result {
  type: 'rows' | 'ok';
  rows?: Row[];
  rowsAffected?: number;
}

export class EngineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EngineError';
  }
}

export type ParamValue = string | number | boolean | null;

export class PreparedStatement {
  constructor(private readonly executeFn: (params: ParamValue[]) => Result) {}
  run(params: ParamValue[] = []): Result { return this.executeFn(params); }
}

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

  static async create(backend: SyncIStorage): Promise<Engine> {
    const storage = new Storage(backend);
    await storage.open();
    const engine = new Engine(storage);
    engine.catalog = initCatalog(storage.kv);
    engine.binder = new Binder(engine.catalog);
    return engine;
  }

  // -------------------------------------------------------------------------
  // Public API — sync after open()
  // -------------------------------------------------------------------------

  execute(sql: string, params?: ParamValue[]): Result[] {
    const statements = this.parser.parse(sql);
    return statements.map((stmt) => this.executeOne(stmt, params));
  }

  prepare(sql: string): PreparedStatement {
    const stmts = this.parser.parse(sql);
    if (stmts.length !== 1) throw new EngineError('prepare() requires exactly one statement');
    const stmt = stmts[0];
    return new PreparedStatement((params) => this.executeOne(stmt, params));
  }

  close(): void { this.storage.close(); }

  // -------------------------------------------------------------------------
  // Statement dispatch
  // -------------------------------------------------------------------------

  private executeOne(stmt: Statement, params?: ParamValue[]): Result {
    if (stmt.type === StatementType.TRANSACTION_STATEMENT) {
      return this.executeTCL(stmt as TransactionStatement);
    }

    if (this.transactionAborted) {
      throw new EngineError(
        'current transaction is aborted, commands ignored until end of transaction block',
      );
    }

    const autocommit = !this.inTransaction;
    if (autocommit) this.catalogSnapshot = this.catalog.serialize();

    try {
      const result = this.runPipeline(stmt, params);

      if (autocommit) {
        if (this.catalogDirty) { this.writeCatalog(); this.catalogDirty = false; }
        this.storage.kv.commit();
        this.catalogSnapshot = null;
      }

      return result;
    } catch (err) {
      if (autocommit) {
        this.storage.kv.rollback();
        this.catalogDirty = false;
        this.catalog = Catalog.deserialize(this.catalogSnapshot!);
        this.binder = new Binder(this.catalog);
        this.catalogSnapshot = null;
      } else {
        this.transactionAborted = true;
        this.catalogDirty = false;
        this.storage.kv.rollback();
        if (this.catalogSnapshot) {
          this.catalog = Catalog.deserialize(this.catalogSnapshot);
          this.binder = new Binder(this.catalog);
        }
      }
      throw err;
    }
  }

  // -------------------------------------------------------------------------
  // TCL
  // -------------------------------------------------------------------------

  private executeTCL(stmt: TransactionStatement): Result {
    const ok: Result = { type: 'ok' };

    switch (stmt.transaction_type) {
      case TransactionType.BEGIN:
        if (this.inTransaction) throw new EngineError('already in a transaction');
        this.catalogSnapshot = this.catalog.serialize();
        this.inTransaction = true;
        return ok;

      case TransactionType.COMMIT:
        if (!this.inTransaction) return ok;
        if (this.transactionAborted) {
          this.catalogSnapshot = null;
          this.inTransaction = false;
          this.transactionAborted = false;
          throw new EngineError('current transaction is aborted, COMMIT treated as ROLLBACK');
        }
        if (this.catalogDirty) { this.writeCatalog(); this.catalogDirty = false; }
        this.storage.kv.commit();
        this.catalogSnapshot = null;
        this.inTransaction = false;
        return ok;

      case TransactionType.ROLLBACK:
        if (!this.inTransaction) return ok;
        if (!this.transactionAborted) {
          this.storage.kv.rollback();
          if (this.catalogSnapshot) {
            this.catalog = Catalog.deserialize(this.catalogSnapshot);
            this.binder = new Binder(this.catalog);
          }
        }
        this.catalogDirty = false;
        this.catalogSnapshot = null;
        this.inTransaction = false;
        this.transactionAborted = false;
        return ok;
    }
  }

  // -------------------------------------------------------------------------
  // Pipeline
  // -------------------------------------------------------------------------

  private runPipeline(stmt: Statement, params?: readonly ParamValue[]): Result {
    const bound = this.binder.bindStatement(stmt);
    const optimized = optimize(bound, this.catalog);
    const result = execute(
      optimized,
      this.storage.rowManager,
      this.catalog,
      this.storage.indexManager,
      params,
    );

    for (const change of result.catalogChanges) this.applyCatalogChange(change);
    if (result.catalogChanges.length > 0) {
      this.catalogDirty = true;
      this.binder = new Binder(this.catalog);
    }

    if (stmt.type === StatementType.SELECT_STATEMENT) return { type: 'rows', rows: result.rows };
    return { type: 'ok', rowsAffected: result.rowsAffected };
  }

  // -------------------------------------------------------------------------
  // Catalog mutations
  // -------------------------------------------------------------------------

  private applyCatalogChange(change: CatalogChange): void {
    switch (change.type) {
      case 'CREATE_TABLE': this.catalog.addTable(change.schema); break;
      case 'DROP_TABLE':   this.catalog.removeTable(change.name); break;
      case 'ALTER_TABLE':  this.catalog.updateTable(change.after); break;
      case 'CREATE_INDEX': this.catalog.addIndex(change.index); break;
      case 'DROP_INDEX':   this.catalog.removeIndex(change.name); break;
    }
  }

  private writeCatalog(): void {
    const [key, data] = serializeCatalogEntry(this.catalog);
    this.storage.kv.writeKey(key, data);
  }
}

import { Storage } from '../store/storage.js';
import { Catalog, initCatalog, serializeCatalogEntry } from '../store/catalog.js';
import { Binder } from '../binder/index.js';
import { optimize } from '../optimizer/index.js';
import { execute } from '../executor/index.js';
import { Parser } from '../parser/index.js';
import {
  StatementType,
  TransactionType,
  type Statement,
  type TransactionStatement,
} from '../parser/types.js';
import type { CatalogData, IStorage, Row } from '../store/types.js';
import type { CatalogChange } from '../executor/types.js';

// ---------------------------------------------------------------------------
// Result type returned to the caller
// ---------------------------------------------------------------------------

export interface Result {
  type: 'rows' | 'ok';
  rows?: Row[];
  rowsAffected?: number;
}

// ---------------------------------------------------------------------------
// Engine error
// ---------------------------------------------------------------------------

export class EngineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EngineError';
  }
}

// ---------------------------------------------------------------------------
// Engine — coordinates the full SQL pipeline
// ---------------------------------------------------------------------------

export class Engine {
  private storage: Storage;
  private catalog!: Catalog;
  private binder!: Binder;
  private parser: Parser;

  private inTransaction = false;
  private catalogSnapshot: CatalogData | null = null;

  private constructor(storage: Storage) {
    this.storage = storage;
    this.parser = new Parser();
  }

  static async create(backend: IStorage): Promise<Engine> {
    const storage = new Storage(backend);
    await storage.open();

    const engine = new Engine(storage);
    engine.catalog = await initCatalog(storage.backend);
    engine.binder = new Binder(engine.catalog);

    storage.initAndVacuum(engine.catalog.getAllTables().map((t) => t.name));

    return engine;
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async execute(sql: string): Promise<Result[]> {
    const statements = this.parser.parse(sql);
    const results: Result[] = [];
    for (const stmt of statements) {
      results.push(await this.executeOne(stmt));
    }
    return results;
  }

  close(): void {
    this.storage.close();
  }

  // -------------------------------------------------------------------------
  // Statement dispatch
  // -------------------------------------------------------------------------

  private async executeOne(stmt: Statement): Promise<Result> {
    if (stmt.type === StatementType.TRANSACTION_STATEMENT) {
      return this.executeTCL(stmt as TransactionStatement);
    }

    const autocommit = !this.inTransaction;
    const stmtCatalogSnapshot = this.catalog.serialize();

    if (autocommit) {
      this.catalogSnapshot = stmtCatalogSnapshot;
    }

    if (!autocommit) {
      this.storage.pageManager.checkpoint();
    }

    try {
      const result = await this.runPipeline(stmt);

      if (autocommit) {
        this.writeCatalog();
        await this.storage.pageManager.commit();
        this.catalogSnapshot = null;
      }

      return result;
    } catch (err) {
      if (autocommit) {
        this.storage.pageManager.rollback();
        this.catalogSnapshot = null;
      } else {
        this.storage.pageManager.restoreCheckpoint();
      }

      this.catalog = Catalog.deserialize(stmtCatalogSnapshot);
      this.binder = new Binder(this.catalog);

      throw err;
    }
  }

  // -------------------------------------------------------------------------
  // TCL handling
  // -------------------------------------------------------------------------

  private async executeTCL(stmt: TransactionStatement): Promise<Result> {
    const ok: Result = { type: 'ok' };

    switch (stmt.transaction_type) {
      case TransactionType.BEGIN:
        if (this.inTransaction) {
          throw new EngineError('already in a transaction');
        }
        this.catalogSnapshot = this.catalog.serialize();
        this.inTransaction = true;
        return ok;

      case TransactionType.COMMIT:
        if (!this.inTransaction) {
          return ok;
        }
        this.writeCatalog();
        await this.storage.pageManager.commit();
        this.catalogSnapshot = null;
        this.inTransaction = false;
        return ok;

      case TransactionType.ROLLBACK:
        if (!this.inTransaction) {
          return ok;
        }
        this.storage.pageManager.rollback();
        if (this.catalogSnapshot) {
          this.catalog = Catalog.deserialize(this.catalogSnapshot);
          this.binder = new Binder(this.catalog);
          this.catalogSnapshot = null;
        }
        this.inTransaction = false;
        return ok;
    }
  }

  // -------------------------------------------------------------------------
  // Pipeline
  // -------------------------------------------------------------------------

  private isQuery(stmt: Statement): boolean {
    return stmt.type === StatementType.SELECT_STATEMENT;
  }

  private async runPipeline(stmt: Statement): Promise<Result> {
    const bound = this.binder.bindStatement(stmt);
    const optimized = optimize(bound);
    const result = await execute(
      optimized,
      this.storage.rowManager,
      this.storage.pageManager,
      this.catalog,
    );

    for (const change of result.catalogChanges) {
      this.applyCatalogChange(change);
    }
    if (result.catalogChanges.length > 0) {
      this.binder = new Binder(this.catalog);
    }

    if (this.isQuery(stmt)) {
      return { type: 'rows', rows: result.rows };
    }
    return { type: 'ok', rowsAffected: result.rowsAffected };
  }

  // -------------------------------------------------------------------------
  // Catalog mutations
  // -------------------------------------------------------------------------

  private applyCatalogChange(change: CatalogChange): void {
    switch (change.type) {
      case 'CREATE_TABLE':
        this.catalog.addTable(change.schema);
        break;
      case 'DROP_TABLE':
        this.catalog.removeTable(change.name);
        break;
      case 'ALTER_TABLE':
        this.catalog.updateTable(change.after);
        break;
      case 'CREATE_INDEX':
        this.catalog.addIndex(change.index);
        break;
      case 'DROP_INDEX':
        this.catalog.removeIndex(change.name);
        break;
    }
  }

  private writeCatalog(): void {
    const [key, data] = serializeCatalogEntry(this.catalog);
    this.storage.pageManager.writeKey(key, data);
  }
}

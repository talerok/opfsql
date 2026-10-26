import { InternalError } from "../errors.js";
import { runQuery, runWrite } from "../executor/index.js";
import type { Schema } from "../schema/index.js";
import type { ReadTransaction, WriteTransaction } from "../storage/index.js";
import {
  Catalog,
  type IndexKeys,
  Tables,
  WritableTables,
} from "../table/index.js";
import type { ParameterValues } from "../values/index.js";
import { emptyResult, type Result, type TransactionMode } from "./api/index.js";
import type { Database, Writer } from "./database.js";
import { runDdl } from "./ddl.js";
import { compileIndexKey } from "./index-keys.js";
import { changesResult, rowsResult } from "./results.js";
import { readSchema, type SchemaCache } from "./schemas.js";
import { columnsOf, type Planned } from "./statements.js";

/** What a transaction needs from its engine. */
export interface TransactionHost {
  readonly database: Database;
  readonly schemas: SchemaCache;
}

/**
 * A unit of work: one pinned snapshot, the schema stored in it, and once it
 * writes, the write lock and a write transaction on that snapshot. A
 * statement outside BEGIN runs in one of its own.
 *
 * Each statement that writes runs in a savepoint (docs/planning.md,
 * Execution): one that fails is rolled back to it, and the transaction goes
 * on, as in SQLite. Only a commit that fails ends the transaction.
 */
export class Transaction {
  private isOpen = true;

  /** The schema the next statement sees: the snapshot's, with this transaction's DDL. */
  private working: IndexKeys;

  private constructor(
    private readonly host: TransactionHost,
    /** What statements read: the snapshot, or once writing, the write transaction. */
    private pages: ReadTransaction,
    private writer: Writer | null,
    /** The schema as of the snapshot. */
    private readonly base: IndexKeys,
  ) {
    this.working = base;
  }

  /**
   * Starts a transaction. An immediate one takes the lock first, so the
   * snapshot it pins — and the schema read from it — is the latest commit,
   * and stays so.
   */
  static async begin(
    host: TransactionHost,
    mode: TransactionMode,
    signal: AbortSignal,
  ): Promise<Transaction> {
    if (mode === "immediate") {
      const writer = await host.database.beginWrite(signal);
      return this.over(host, writer.transaction, writer);
    }
    const read = await host.database.beginRead(signal);
    return this.over(host, read, null);
  }

  /** A transaction on pages already pinned; they are released if it cannot start. */
  private static over(
    host: TransactionHost,
    pages: ReadTransaction,
    writer: Writer | null,
  ): Transaction {
    try {
      return new Transaction(host, pages, writer, host.schemas.load(pages));
    } catch (error) {
      discard(host.database, pages, writer);
      throw error;
    }
  }

  get schema(): Schema {
    return this.working.schema;
  }

  /**
   * Takes the lock for a deferred transaction's first write. Writing on a
   * snapshot another commit has overtaken could lose that commit, so that
   * fails with SERIALIZATION_FAILURE instead, and the transaction goes on
   * reading.
   */
  async makeWritable(signal: AbortSignal): Promise<void> {
    this.ensureOpen();
    if (this.writer !== null) return;
    this.writer = await this.host.database.upgrade(this.pages, signal);
    this.pages = this.writer.transaction;
  }

  /** Runs a planned statement; one that writes needs makeWritable first. */
  run(planned: Planned, parameters: ParameterValues): Result {
    this.ensureOpen();
    switch (planned.kind) {
      case "query": {
        const tables = new Tables(this.pages, this.working.schema);
        const rows = runQuery(planned.statement, tables, parameters);
        return rowsResult(columnsOf(planned), rows);
      }
      case "explain": {
        return rowsResult(columnsOf(planned), [[planned.text]]);
      }
      case "write": {
        const { statement } = planned;
        return this.inSavepoint((transaction) => {
          const tables = new WritableTables(transaction, this.working);
          const outcome = runWrite(statement, tables, parameters);
          return changesResult(outcome.changes, outcome.lastInsertRowid);
        });
      }
      case "ddl": {
        const { statement } = planned;
        return this.inSavepoint((transaction) => {
          const { schema } = this.working;
          runDdl(
            statement,
            new Catalog(transaction, schema, compileIndexKey),
            schema,
          );
          this.readSchemaBack(transaction);
          return emptyResult;
        });
      }
    }
  }

  /** Commits; if that fails, nothing of the transaction remains. */
  commit(): void {
    this.ensureOpen();
    this.isOpen = false;
    const writer = this.writer;
    if (writer === null) {
      this.pages.end();
      return;
    }
    endWriter(this.host.database, writer, (transaction) => {
      transaction.commit();
    });
    // The schema just committed: kept for others, as the same object.
    if (this.working !== this.base) this.host.schemas.remember(this.working);
  }

  rollback(): void {
    if (!this.isOpen) return;
    this.isOpen = false;
    discard(this.host.database, this.pages, this.writer);
  }

  /**
   * Runs `change` in a savepoint: if it fails, what it wrote is rolled back
   * — the schema it changed too — and the error goes on to the caller.
   */
  private inSavepoint(
    change: (transaction: WriteTransaction) => Result,
  ): Result {
    const transaction = this.writing();
    const savepoint = transaction.savepoint();
    const working = this.working;
    try {
      const result = change(transaction);
      transaction.release(savepoint);
      return result;
    } catch (error) {
      transaction.rollbackTo(savepoint);
      transaction.release(savepoint);
      this.working = working;
      throw error;
    }
  }

  /**
   * After DDL, the schema is what the schema table now says. It stays this
   * transaction's own until it commits: its version is not taken yet.
   */
  private readSchemaBack(transaction: WriteTransaction): void {
    const hasChanged =
      transaction.schemaVersion !== this.working.schema.version;
    if (hasChanged) this.working = readSchema(transaction);
  }

  private writing(): WriteTransaction {
    if (this.writer === null) {
      throw new InternalError("a statement wrote before taking the lock");
    }
    return this.writer.transaction;
  }

  private ensureOpen(): void {
    if (!this.isOpen) throw new InternalError("the transaction has ended");
  }
}

/** Releases a transaction's pages, rolling back what it wrote. */
function discard(
  database: Database,
  pages: ReadTransaction,
  writer: Writer | null,
): void {
  if (writer === null) {
    pages.end();
    return;
  }
  endWriter(database, writer, (transaction) => {
    transaction.rollback();
  });
}

/** Ends a writer's transaction with `end`, then checkpoints if due and releases the lock. */
function endWriter(
  database: Database,
  writer: Writer,
  end: (transaction: WriteTransaction) => void,
): void {
  try {
    end(writer.transaction);
  } finally {
    database.endWrite(writer);
  }
}

import { column } from "../../../test/support/schema.js";
import type {
  Index,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import { constraintIndexName, Schema } from "../../schema/index.js";
import type {
  Pager,
  ReadTransaction,
  StoredSchemaEntry,
  WriteTransaction,
} from "../../storage/index.js";
import {
  checkStorage,
  openPager,
  readSchemaTable,
} from "../../storage/index.js";
import {
  beginWrite,
  memoryFiles,
  read,
  write,
} from "../../storage/tests/pager-support.js";
import type { Value } from "../../values/index.js";
import type { CompileExpression } from "../index.js";
import {
  Catalog,
  checkTables,
  keyedIndex,
  SEQUENCE_TABLE_NAME,
  Sequences,
  TableEditor,
  TableRows,
} from "../index.js";

export { column } from "../../../test/support/schema.js";

// A database for table-layer tests, its schema kept by hand as the engine
// would reload it, and a users table to work on.

export const noExpressions: CompileExpression = () => {
  throw new Error("these tests have no expression keys");
};

// users (id INTEGER PRIMARY KEY, nick TEXT, email TEXT UNIQUE)
export const usersShape: TableShape = {
  name: "users",
  columns: [
    column("id", "INTEGER"),
    column("nick", "TEXT"),
    column("email", "TEXT"),
  ],
  rowidColumn: 0,
  isAutoincrement: false,
};

export const uniqueEmail: IndexShape = {
  name: constraintIndexName("users", 1),
  table: "users",
  keys: [{ kind: "column", column: 2 }],
  isUnique: true,
  origin: "unique",
};

/** The database's schema, kept by hand as the engine would reload it. */
export class Database {
  tables: Table[] = [];

  indexes: Index[] = [];

  private constructor(
    readonly pager: Pager,
    private readonly compile: CompileExpression,
  ) {}

  /** `compile` builds expression keys; the default refuses them. */
  static async open(
    compile: CompileExpression = noExpressions,
  ): Promise<Database> {
    return new Database(
      await openPager(memoryFiles(), { name: "catalog" }),
      compile,
    );
  }

  get schema(): Schema {
    return new Schema(0, this.tables, this.indexes);
  }

  change<T>(work: (catalog: Catalog, transaction: WriteTransaction) => T): T {
    return write(this.pager, (transaction) =>
      work(new Catalog(transaction, this.schema, this.compile), transaction),
    );
  }

  /** What `query` returns, run in a read transaction. */
  reading<T>(query: (transaction: ReadTransaction) => T): T {
    return read(this.pager, query);
  }

  /**
   * What `change` returns, run in a write transaction: committed if it
   * succeeds, rolled back if it fails.
   */
  writing<T>(change: (transaction: WriteTransaction) => T): T {
    const transaction = beginWrite(this.pager);
    try {
      const result = change(transaction);
      transaction.commit();
      return result;
    } catch (error) {
      transaction.rollback();
      throw error;
    }
  }

  insert(table: Table, rows: Value[][]): void {
    write(this.pager, (transaction) => {
      const editor = this.editorIn(transaction, table);
      for (const row of rows) editor.insert(row);
    });
  }

  /** An editor of `table` within `transaction`, with all its indexes. */
  editorIn(transaction: WriteTransaction, table: Table): TableEditor {
    const indexes = this.schema
      .indexesOf(table.name)
      .map((index) => keyedIndex(index, table, this.compile));
    const sequences = this.sequencesIn(transaction);
    return new TableEditor(transaction, table, indexes, sequences);
  }

  /** checkTables within `transaction`, its uncommitted changes included. */
  problemsIn(transaction: WriteTransaction): string[] {
    return checkTables(transaction, this.schema, this.compile);
  }

  rows(table: Table): Value[][] {
    return read(this.pager, (transaction) =>
      [...new TableRows(transaction, table).scan()].map((row) => row.values()),
    );
  }

  problems(): string[] {
    return read(this.pager, (transaction) => checkStorage(transaction));
  }

  tableProblems(): string[] {
    return read(this.pager, (transaction) =>
      checkTables(transaction, this.schema, this.compile),
    );
  }

  /** Registers opfsql_sequence once a catalog change has made it. */
  findSequences(): void {
    const entry = this.storedEntries().find(
      ({ name }) => name === SEQUENCE_TABLE_NAME,
    );
    if (entry === undefined) return;
    this.tables.push({
      name: SEQUENCE_TABLE_NAME,
      columns: [column("name", "TEXT"), column("seq", "INTEGER")],
      rootPage: entry.rootPage,
      rowidColumn: null,
      isAutoincrement: false,
    });
  }

  storedEntries(): StoredSchemaEntry[] {
    return read(this.pager, (transaction) => readSchemaTable(transaction));
  }

  entries(): { name: string; sql: string | null }[] {
    return read(this.pager, (transaction) =>
      readSchemaTable(transaction).map(({ name, sql }) => ({ name, sql })),
    );
  }

  private sequencesIn(transaction: WriteTransaction): Sequences | null {
    const table = this.schema.table(SEQUENCE_TABLE_NAME);
    return table === null ? null : new Sequences(transaction, table.rootPage);
  }
}

/** A database with an empty users table and its unique email index. */
export async function withUsers(
  shape: TableShape = usersShape,
  constraints: readonly IndexShape[] = [uniqueEmail],
): Promise<{ database: Database; users: Table }> {
  const database = await Database.open();
  const { table, indexes } = database.change((catalog) =>
    catalog.createTable(shape, constraints),
  );
  database.tables.push(table);
  database.indexes.push(...indexes);
  database.findSequences();
  return { database, users: table };
}

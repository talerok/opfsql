import { describe, expect, it } from "vitest";
import { ConstraintError } from "../../errors.js";
import type {
  Column,
  Index,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import { constraintIndexName, Schema } from "../../schema/index.js";
import type {
  Pager,
  StoredSchemaEntry,
  WriteTransaction,
} from "../../storage/index.js";
import {
  checkStorage,
  openPager,
  readSchemaTable,
} from "../../storage/index.js";
import { memoryFiles, read, write } from "../../storage/tests/pager-support.js";
import type { Value } from "../../values/index.js";
import type { CompileExpression } from "../index.js";
import {
  Catalog,
  keyedIndex,
  SEQUENCE_TABLE_NAME,
  TableEditor,
  TableRows,
} from "../index.js";

const noExpressions: CompileExpression = () => {
  throw new Error("these tests have no expression keys");
};

function column(
  name: string,
  type: Column["type"],
  extra: Partial<Column> = {},
): Column {
  return { name, type, isNotNull: false, defaultValue: null, ...extra };
}

// users (id INTEGER PRIMARY KEY, nick TEXT, email TEXT UNIQUE)
const usersShape: TableShape = {
  name: "users",
  columns: [
    column("id", "INTEGER"),
    column("nick", "TEXT"),
    column("email", "TEXT"),
  ],
  rowidColumn: 0,
  isAutoincrement: false,
};

const uniqueEmail: IndexShape = {
  name: constraintIndexName("users", 1),
  table: "users",
  keys: [{ kind: "column", column: 2 }],
  isUnique: true,
  origin: "unique",
};

/** The database's schema, kept by hand as the engine would reload it. */
class Database {
  tables: Table[] = [];
  indexes: Index[] = [];

  private constructor(readonly pager: Pager) {}

  static async open(): Promise<Database> {
    return new Database(await openPager(memoryFiles(), { name: "catalog" }));
  }

  get schema(): Schema {
    return new Schema(0, this.tables, this.indexes);
  }

  change<T>(work: (catalog: Catalog, transaction: WriteTransaction) => T): T {
    return write(this.pager, (transaction) =>
      work(new Catalog(transaction, this.schema, noExpressions), transaction),
    );
  }

  insert(table: Table, rows: Value[][]): void {
    write(this.pager, (transaction) => {
      const indexes = this.schema
        .indexesOf(table.name)
        .map((index) => keyedIndex(index, table, noExpressions));
      const editor = new TableEditor(transaction, table, indexes, null);
      for (const row of rows) editor.insert(row);
    });
  }

  rows(table: Table): Value[][] {
    return read(this.pager, (transaction) =>
      [...new TableRows(transaction, table).scan()].map((row) => row.values()),
    );
  }

  problems(): string[] {
    return read(this.pager, (transaction) => checkStorage(transaction));
  }

  storedEntries(): StoredSchemaEntry[] {
    return read(this.pager, (transaction) => readSchemaTable(transaction));
  }

  entries(): { name: string; sql: string | null }[] {
    return read(this.pager, (transaction) =>
      readSchemaTable(transaction).map(({ name, sql }) => ({ name, sql })),
    );
  }
}

async function withUsers(): Promise<{ database: Database; users: Table }> {
  const database = await Database.open();
  const { table, indexes } = database.change((catalog) =>
    catalog.createTable(usersShape, [uniqueEmail]),
  );
  database.tables.push(table);
  database.indexes.push(...indexes);
  return { database, users: table };
}

describe("Catalog", () => {
  it("creates a table and its constraint's index, with statements from the model", async () => {
    const { database } = await withUsers();
    expect(database.entries()).toEqual([
      {
        name: "users",
        sql: 'CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "nick" TEXT, "email" TEXT UNIQUE)',
      },
      { name: "opfsql_autoindex_users_1", sql: null },
    ]);
    expect(database.problems()).toEqual([]);
  });

  it("fills a new index from the rows, and refuses a unique one they break", async () => {
    const { database, users } = await withUsers();
    database.insert(users, [
      [null, "ann", "a@x"],
      [null, "ann", "b@x"],
    ]);
    const byNick: IndexShape = {
      name: "users_nick",
      table: "users",
      keys: [{ kind: "column", column: 1 }],
      isUnique: false,
      origin: "statement",
    };
    database.change((catalog) => catalog.createIndex(byNick, users));
    expect(database.entries().at(-1)?.sql).toBe(
      'CREATE INDEX "users_nick" ON "users" ("nick")',
    );
    expect(database.problems()).toEqual([]);

    const uniqueNick = { ...byNick, name: "users_nick_unique", isUnique: true };
    expect(() =>
      database.change((catalog) => catalog.createIndex(uniqueNick, users)),
    ).toThrow(ConstraintError);
  });

  it("drops a table with its indexes and leaves no page behind", async () => {
    const { database, users } = await withUsers();
    // A nick long enough for overflow pages, which must go too.
    database.insert(users, [[null, "x".repeat(20_000), "a@x"]]);
    database.change((catalog) => {
      catalog.dropTable(users);
    });
    expect(database.entries()).toEqual([]);
    expect(database.problems()).toEqual([]);
  });

  it("adds a column that old rows read as its default", async () => {
    const { database, users } = await withUsers();
    database.insert(users, [[null, "ann", "a@x"]]);
    const age = column("age", "INTEGER", { defaultValue: 18 });
    const changed = database.change((catalog) => catalog.addColumn(users, age));
    expect(database.rows(changed)).toEqual([[1, "ann", "a@x", 18]]);
    expect(database.entries()[0]?.sql).toContain('"age" INTEGER DEFAULT 18');
  });

  it("drops a column from every row, and renumbers the constraints after it", async () => {
    const { database, users } = await withUsers();
    database.insert(users, [
      [null, "ann", "a@x"],
      [null, "bob", "b@x"],
    ]);
    const changed = database.change((catalog) => catalog.dropColumn(users, 1));
    expect(database.rows(changed)).toEqual([
      [1, "a@x"],
      [2, "b@x"],
    ]);
    expect(database.entries()[0]?.sql).toBe(
      'CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "email" TEXT UNIQUE)',
    );
    expect(database.problems()).toEqual([]);
  });

  it("creates opfsql_sequence for the first AUTOINCREMENT table only", async () => {
    const database = await Database.open();
    const counted = { ...usersShape, isAutoincrement: true };
    database.change((catalog) => catalog.createTable(counted, []));
    const sequence = database
      .storedEntries()
      .find((entry) => entry.name === SEQUENCE_TABLE_NAME);
    expect(sequence?.sql).toBe(
      'CREATE TABLE "opfsql_sequence" ("name" TEXT, "seq" INTEGER)',
    );

    // Once the schema shows it, as the engine's reload would, it is reused.
    database.tables.push({
      ...counted,
      name: SEQUENCE_TABLE_NAME,
      rootPage: sequence?.rootPage ?? 0,
    });
    database.change((catalog) =>
      catalog.createTable({ ...counted, name: "more" }, []),
    );
    const names = database.entries().map((entry) => entry.name);
    expect(names.filter((name) => name === SEQUENCE_TABLE_NAME)).toHaveLength(
      1,
    );
  });
});

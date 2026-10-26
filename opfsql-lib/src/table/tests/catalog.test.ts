import { describe, expect, it } from "vitest";
import { ConstraintError, InternalError, SemanticError } from "../../errors.js";
import { type IndexShape, SEQUENCE_TABLE_NAME } from "../../schema/index.js";
import type { Catalog, CompileExpression } from "../index.js";
import { column, Database, usersShape, withUsers } from "./database.js";

/** `lower(column)`, compiled as the engine would: by name, failing for a column the table lacks. */
const compileLower: CompileExpression = (sql, table) => {
  const name = sql.startsWith("lower(") ? sql.slice(6, -1) : sql;
  const position = table.columns.findIndex((each) => each.name === name);
  if (position === -1) throw new SemanticError(`no such column in ${sql}`);
  return (row) => {
    const value = row[position];
    return typeof value === "string" ? value.toLowerCase() : null;
  };
};

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

    const before = database.entries();
    const uniqueNick = { ...byNick, name: "users_nick_unique", isUnique: true };
    expect(() =>
      database.change((catalog) => catalog.createIndex(uniqueNick, users)),
    ).toThrow(ConstraintError);
    // The refused statement left nothing: no entry, no page.
    expect(database.entries()).toEqual(before);
    expect(database.problems()).toEqual([]);
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

  it("refuses to drop a column an expression key reads, whatever the binder checked", async () => {
    const database = await Database.open(compileLower);
    const { table: users } = database.change((catalog) =>
      catalog.createTable(usersShape, []),
    );
    database.tables.push(users);
    const byNick = database.change((catalog) =>
      catalog.createIndex(
        {
          name: "users_lower_nick",
          table: "users",
          keys: [{ kind: "expression", sql: "lower(nick)", type: "TEXT" }],
          isUnique: false,
          origin: "statement",
        },
        users,
      ),
    );
    database.indexes.push(byNick);

    expect(() =>
      database.change((catalog) => catalog.dropColumn(users, 1)),
    ).toThrow(
      new InternalError(
        "column 1 of users is in use by index users_lower_nick",
      ),
    );
    // lower(nick) still compiles without email.
    const changed = database.change((catalog) => catalog.dropColumn(users, 2));
    expect(changed.columns.map(({ name }) => name)).toEqual(["id", "nick"]);
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
    database.findSequences();
    database.change((catalog) =>
      catalog.createTable({ ...counted, name: "more" }, []),
    );
    const names = database.entries().map((entry) => entry.name);
    expect(names.filter((name) => name === SEQUENCE_TABLE_NAME)).toHaveLength(
      1,
    );
  });

  it("refuses changes that would leave the database inconsistent", async () => {
    const { database, users } = await withUsers();
    const [uniqueIndex] = database.indexes;
    if (uniqueIndex === undefined)
      throw new Error("users has its unique index");
    const refusals: [string, (catalog: Catalog) => unknown][] = [
      ["drop the rowid column", (catalog) => catalog.dropColumn(users, 0)],
      ["drop an indexed column", (catalog) => catalog.dropColumn(users, 2)],
      [
        "drop a constraint's index",
        (catalog) => {
          catalog.dropIndex(uniqueIndex);
        },
      ],
      [
        "add NOT NULL without a default",
        (catalog) =>
          catalog.addColumn(
            users,
            column("age", "INTEGER", { isNotNull: true }),
          ),
      ],
      [
        "add a default of another type",
        (catalog) =>
          catalog.addColumn(
            users,
            column("age", "INTEGER", { defaultValue: "42" }),
          ),
      ],
    ];
    for (const [what, change] of refusals) {
      expect(() => database.change(change), what).toThrow(InternalError);
    }
  });
});

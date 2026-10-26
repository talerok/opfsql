import { describe, expect, it } from "vitest";
import { column } from "../../../test/support/schema.js";
import { InternalError } from "../../errors.js";
import type { IndexShape, TableShape } from "../index.js";
import {
  constraintIndexName,
  createIndexSql,
  createTableSql,
  defaultSql,
  quoteName,
} from "../index.js";

const users: TableShape = {
  name: "users",
  columns: [
    column("id", "INTEGER"),
    column("email", "TEXT", { isNotNull: true }),
    column("settings", "JSON", { defaultValue: { theme: "dark" } }),
    column("score", "REAL", { defaultValue: -1.5 }),
  ],
  rowidColumn: 0,
  isAutoincrement: true,
};

const uniqueEmail: IndexShape = {
  name: constraintIndexName("users", 1),
  table: "users",
  keys: [{ kind: "column", column: 1 }],
  isUnique: true,
  origin: "unique",
};

describe("schema statements", () => {
  it("write a table with its rowid, constraints and defaults", () => {
    expect(createTableSql(users, [uniqueEmail])).toBe(
      'CREATE TABLE "users" (' +
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT, ' +
        '"email" TEXT UNIQUE NOT NULL, ' +
        `"settings" JSON DEFAULT '{"theme":"dark"}', ` +
        '"score" REAL DEFAULT -1.5)',
    );
  });

  it("write a composite primary key after the columns", () => {
    const pairs: TableShape = {
      name: "pairs",
      columns: [
        column("a", "TEXT", { isNotNull: true }),
        column("b", "TEXT", { isNotNull: true }),
      ],
      rowidColumn: null,
      isAutoincrement: false,
    };
    const key: IndexShape = {
      name: constraintIndexName("pairs", 1),
      table: "pairs",
      keys: [
        { kind: "column", column: 0 },
        { kind: "column", column: 1 },
      ],
      isUnique: true,
      origin: "primary-key",
    };
    expect(createTableSql(pairs, [key])).toBe(
      'CREATE TABLE "pairs" ("a" TEXT NOT NULL, "b" TEXT NOT NULL, PRIMARY KEY ("a", "b"))',
    );
  });

  it("write an index with column and expression keys", () => {
    const index: IndexShape = {
      name: "users_lower_email",
      table: "users",
      keys: [
        { kind: "expression", sql: "lower(email)", type: "TEXT" },
        { kind: "column", column: 3 },
      ],
      isUnique: true,
      origin: "statement",
    };
    expect(createIndexSql(index, users)).toBe(
      'CREATE UNIQUE INDEX "users_lower_email" ON "users" (lower(email), "score")',
    );
  });

  it("quote any name, and write defaults as literals that read back the same", () => {
    expect(quoteName('say "hi"')).toBe('"say ""hi"""');
    expect(defaultSql(Infinity, "REAL")).toBe("9e999");
    // A JSON column parses its default as JSON text, so a string is quoted twice.
    expect(defaultSql("abc", "JSON")).toBe(`'"abc"'`);
    expect(defaultSql("abc", "TEXT")).toBe("'abc'");
  });

  it("write PRIMARY KEY and UNIQUE on one column, each with its index", () => {
    const codes: TableShape = {
      name: "codes",
      columns: [column("code", "TEXT", { isNotNull: true })],
      rowidColumn: null,
      isAutoincrement: false,
    };
    const key = { ...codeConstraint(1), origin: "primary-key" as const };
    const unique = codeConstraint(2);
    expect(createTableSql(codes, [key, unique])).toBe(
      'CREATE TABLE "codes" ("code" TEXT PRIMARY KEY UNIQUE NOT NULL)',
    );
  });

  it.each<[string, TableShape, IndexShape[]]>([
    ["a constraint named out of order", users, [{ ...uniqueEmail, name: "x" }]],
    [
      "an index no constraint made",
      users,
      [{ ...uniqueEmail, origin: "statement" }],
    ],
    [
      "a UNIQUE over two columns",
      users,
      [
        {
          ...uniqueEmail,
          keys: [
            { kind: "column", column: 1 },
            { kind: "column", column: 2 },
          ],
        },
      ],
    ],
    ["AUTOINCREMENT without a rowid", { ...users, rowidColumn: null }, []],
  ])("refuse to write %s instead of losing it", (_, table, constraints) => {
    expect(() => createTableSql(table, constraints)).toThrow(InternalError);
  });
});

/** A UNIQUE constraint on codes.code, named for the `ordinal`-th constraint. */
function codeConstraint(ordinal: number): IndexShape {
  return {
    name: constraintIndexName("codes", ordinal),
    table: "codes",
    keys: [{ kind: "column", column: 0 }],
    isUnique: true,
    origin: "unique",
  };
}

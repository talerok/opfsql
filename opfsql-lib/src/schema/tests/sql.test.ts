import { describe, expect, it } from "vitest";
import type { Column, IndexShape, TableShape } from "../index.js";
import {
  constraintIndexName,
  createIndexSql,
  createTableSql,
  defaultSql,
  literalSql,
  quoteName,
} from "../index.js";

function column(
  name: string,
  type: Column["type"],
  extra: Partial<Column> = {},
): Column {
  return { name, type, isNotNull: false, defaultValue: null, ...extra };
}

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
        { kind: "expression", sql: "lower(email)" },
        { kind: "column", column: 3 },
      ],
      isUnique: true,
      origin: "statement",
    };
    expect(createIndexSql(index, users)).toBe(
      'CREATE UNIQUE INDEX "users_lower_email" ON "users" (lower(email), "score")',
    );
  });

  it("quote any name, and write values as literals that read back the same", () => {
    expect(quoteName('say "hi"')).toBe('"say ""hi"""');
    expect(
      [null, true, 42, Infinity, "it's", Uint8Array.from([0xde, 0xad])].map(
        (value) => literalSql(value),
      ),
    ).toEqual(["NULL", "TRUE", "42", "9e999", "'it''s'", "X'dead'"]);
    // A JSON column parses its default as JSON text, so a string is quoted twice.
    expect(defaultSql("abc", "JSON")).toBe(`'"abc"'`);
    expect(defaultSql("abc", "TEXT")).toBe("'abc'");
  });
});

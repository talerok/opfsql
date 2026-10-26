import { describe, expect, it } from "vitest";
import type { Column, Index, Table } from "../index.js";
import { isReservedName, isSameName, Schema } from "../index.js";

function column(name: string, type: Column["type"] = "TEXT"): Column {
  return { name, type, isNotNull: false, defaultValue: null };
}

function table(
  name: string,
  columns: Column[],
  rowidColumn: number | null = null,
): Table {
  return { name, columns, rootPage: 3, rowidColumn, isAutoincrement: false };
}

function index(name: string, tableName: string, keyColumn: number): Index {
  return {
    name,
    table: tableName,
    keys: [{ kind: "column", column: keyColumn }],
    isUnique: false,
    rootPage: 4,
    origin: "statement",
  };
}

describe("names", () => {
  it("ignore the case of ASCII letters only", () => {
    expect(isSameName("Users", "USERS")).toBe(true);
    expect(isSameName("Été", "été")).toBe(false);
    expect(isReservedName("OPFSQL_schema")).toBe(true);
    expect(isReservedName("opfsqlish")).toBe(false);
  });
});

describe("Schema", () => {
  const users = table("Users", [column("id", "INTEGER"), column("name")], 0);
  const byName = index("users_name", "users", 1);
  const schema = new Schema(7, [users], [byName]);

  it("finds tables and indexes by name in any case", () => {
    expect(schema.table("USERS")).toBe(users);
    expect(schema.index("Users_Name")).toBe(byName);
    expect(schema.indexesOf("users")).toEqual([byName]);
    expect(schema.table("orders")).toBeNull();
  });

  it("is consistent when names, tables and columns match", () => {
    expect(schema.problems()).toEqual([]);
    expect(Schema.empty.problems()).toEqual([]);
  });

  it("reports what does not match", () => {
    const broken = new Schema(
      1,
      [
        users,
        table("t", [column("a"), column("A")]),
        table("u", [column("a")], 0),
        { ...table("v", [column("n", "INTEGER")]), isAutoincrement: true },
        table("w", [{ ...column("n", "INTEGER"), defaultValue: "42" }]),
      ],
      [
        index("USERS", "users", 0),
        index("i", "nowhere", 0),
        index("j", "t", 5),
      ],
    );
    expect(broken.problems()).toEqual([
      "the name USERS is used twice",
      "table t: the name A is used twice",
      "table u: no INTEGER rowid column 0",
      "table v: AUTOINCREMENT without a rowid column",
      "table w: the default of n is not a INTEGER",
      "index i is on a missing table nowhere",
      "index j keys missing column 5",
    ]);
  });
});

import { describe, expect, it } from "vitest";
import { DatatypeMismatchError } from "../../../errors.js";
import type { logical, Statement } from "../../plan/index.js";
import { bind, notSupported, semanticError, thrown } from "./helpers.js";

type Bound = Statement<logical.Plan>;

function createTable(sql: string): Extract<Bound, { kind: "create-table" }> {
  const bound = bind(sql);
  if (bound.kind !== "create-table") throw new TypeError(sql);
  return bound;
}

function createIndex(sql: string): Extract<Bound, { kind: "create-index" }> {
  const bound = bind(sql);
  if (bound.kind !== "create-index") throw new TypeError(sql);
  return bound;
}

describe("CREATE TABLE", () => {
  it("types columns, converts defaults to them and makes a sole INTEGER key the rowid", () => {
    const { table, constraints } = createTable(
      `CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, a TEXT UNIQUE NOT NULL DEFAULT 'x', b REAL DEFAULT 1, c BOOLEAN DEFAULT 1, d JSON DEFAULT '{"k": [1, 2]}', e BLOB DEFAULT x'00ff', f varchar(9) NULL, g)`,
    );
    expect(table).toEqual({
      name: "t",
      columns: [
        { name: "id", type: "INTEGER", isNotNull: false, defaultValue: null },
        { name: "a", type: "TEXT", isNotNull: true, defaultValue: "x" },
        { name: "b", type: "REAL", isNotNull: false, defaultValue: 1 },
        { name: "c", type: "BOOLEAN", isNotNull: false, defaultValue: true },
        {
          name: "d",
          type: "JSON",
          isNotNull: false,
          defaultValue: { k: [1, 2] },
        },
        {
          name: "e",
          type: "BLOB",
          isNotNull: false,
          defaultValue: Uint8Array.from([0x00, 0xff]),
        },
        { name: "f", type: "TEXT", isNotNull: false, defaultValue: null },
        { name: "g", type: "ANY", isNotNull: false, defaultValue: null },
      ],
      rowidColumn: 0,
      isAutoincrement: true,
    });
    expect(constraints).toEqual([
      {
        name: "opfsql_autoindex_t_1",
        table: "t",
        keys: [{ kind: "column", column: 1 }],
        isUnique: true,
        origin: "unique",
      },
    ]);
  });

  it("names constraint indexes per column, PRIMARY KEY before UNIQUE, then a table key", () => {
    const shapes = (sql: string): string[] =>
      createTable(sql).constraints.map(
        (index) =>
          `${index.name}: ${index.origin} ${index.keys.map((key) => (key.kind === "column" ? String(key.column) : key.sql)).join(",")}`,
      );
    expect(
      shapes(
        "CREATE TABLE t (a TEXT, b INT, PRIMARY KEY (b, a), c TEXT UNIQUE)",
      ),
    ).toEqual([
      "opfsql_autoindex_t_1: unique 2",
      "opfsql_autoindex_t_2: primary-key 1,0",
    ]);
    expect(
      shapes("CREATE TABLE t (a TEXT UNIQUE PRIMARY KEY, b TEXT UNIQUE)"),
    ).toEqual([
      "opfsql_autoindex_t_1: primary-key 0",
      "opfsql_autoindex_t_2: unique 0",
      "opfsql_autoindex_t_3: unique 1",
    ]);
    // A one-column table PRIMARY KEY is its column's, as the statement writes it.
    expect(
      shapes("CREATE TABLE t (a TEXT UNIQUE, b TEXT UNIQUE, PRIMARY KEY (b))"),
    ).toEqual([
      "opfsql_autoindex_t_1: unique 0",
      "opfsql_autoindex_t_2: primary-key 1",
      "opfsql_autoindex_t_3: unique 1",
    ]);
    expect(shapes("CREATE TABLE t (a INTEGER PRIMARY KEY UNIQUE)")).toEqual([
      "opfsql_autoindex_t_1: unique 0",
    ]);
  });

  it("makes primary key columns NOT NULL, except the rowid", () => {
    const { table } = createTable(
      "CREATE TABLE t (a TEXT, b INT, PRIMARY KEY (a, b))",
    );
    expect(table.columns.map((column) => column.isNotNull)).toEqual([
      true,
      true,
    ]);
    expect(table.rowidColumn).toBeNull();
    const rowid = createTable("CREATE TABLE t (a BIGINT, PRIMARY KEY (a))");
    expect(rowid.table.rowidColumn).toBe(0);
    expect(rowid.table.columns[0]?.isNotNull).toBe(false);
    expect(rowid.constraints).toEqual([]);
  });

  it("refuses AUTOINCREMENT off the rowid and a second primary key", () => {
    expect(semanticError("CREATE TABLE t (a INTEGER AUTOINCREMENT)")).toBe(
      "AUTOINCREMENT on column a: it is allowed only on an INTEGER PRIMARY KEY",
    );
    expect(
      semanticError("CREATE TABLE t (a TEXT PRIMARY KEY AUTOINCREMENT)"),
    ).toBe(
      "AUTOINCREMENT on column a: it is allowed only on an INTEGER PRIMARY KEY",
    );
    for (const sql of [
      "CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)",
      "CREATE TABLE t (a INT PRIMARY KEY, PRIMARY KEY (a))",
      "CREATE TABLE t (a INT PRIMARY KEY PRIMARY KEY)",
    ]) {
      expect(semanticError(sql)).toBe(
        'table "t" has more than one primary key',
      );
    }
    expect(semanticError("CREATE TABLE t (a INT, PRIMARY KEY (a, b))")).toBe(
      "table t has no column named b",
    );
    expect(semanticError("CREATE TABLE t (a INT, PRIMARY KEY (a, A))")).toBe(
      "a column is named twice in the primary key of t",
    );
  });

  it("refuses duplicate columns, unknown types and defaults that are no constant", () => {
    expect(semanticError("CREATE TABLE t (a INT, A TEXT)")).toBe(
      "duplicate column name in t: A",
    );
    expect(semanticError("CREATE TABLE t (a NUMBER)")).toBe(
      "unknown type: NUMBER",
    );
    expect(semanticError("CREATE TABLE t (a INTEGER DEFAULT ?)")).toBe(
      "the DEFAULT of column a is not constant",
    );
    expect(notSupported("CREATE TABLE t (a INTEGER DEFAULT (1 + 2))")).toBe(
      "a DEFAULT other than a literal",
    );
  });

  it("refuses a default its column cannot store, when it is declared", () => {
    const error = thrown("CREATE TABLE t (a INTEGER DEFAULT 'abc')");
    expect(error).toBeInstanceOf(DatatypeMismatchError);
    expect(error.message).toBe(
      'the DEFAULT of column a: cannot store "abc" as INTEGER',
    );
  });

  it("refuses reserved names, and names in use unless IF NOT EXISTS names a table", () => {
    expect(semanticError("CREATE TABLE opfsql_x (a INT)")).toBe(
      "opfsql_x is reserved: names starting with opfsql_ belong to opfsql",
    );
    expect(semanticError("CREATE TABLE USERS (a INT)")).toBe(
      "there is already a table named USERS",
    );
    expect(
      createTable("CREATE TABLE IF NOT EXISTS users (a INT)").isIfNotExists,
    ).toBe(true);
    expect(
      semanticError("CREATE TABLE IF NOT EXISTS orders_user (a INT)"),
    ).toBe("there is already an index named orders_user");
  });

  it("does not support keys over BLOB or JSON columns", () => {
    expect(notSupported("CREATE TABLE t (a BLOB PRIMARY KEY)")).toBe(
      "a PRIMARY KEY on a BLOB column",
    );
    expect(notSupported("CREATE TABLE t (a JSON UNIQUE)")).toBe(
      "UNIQUE on a JSON column",
    );
  });
});

describe("CREATE INDEX", () => {
  it("keys columns by position and expressions by canonical SQL and type", () => {
    expect(
      createIndex(
        "CREATE UNIQUE INDEX i ON users (age, users.email, LOWER( name ), data.city, CAST(age AS REAL)/2.0)",
      ).index,
    ).toEqual({
      name: "i",
      table: "users",
      keys: [
        { kind: "column", column: 2 },
        { kind: "column", column: 3 },
        { kind: "expression", sql: "lower(name)", type: "TEXT" },
        { kind: "expression", sql: "data.city", type: "JSON" },
        { kind: "expression", sql: "CAST(age AS REAL) / 2.0", type: "REAL" },
      ],
      isUnique: true,
      origin: "statement",
    });
  });

  it("refuses what does not give a row one value: subqueries, parameters, aggregates, volatile calls", () => {
    expect(semanticError("CREATE INDEX i ON users ((SELECT 1))")).toBe(
      "subqueries are not allowed in an index",
    );
    expect(semanticError("CREATE INDEX i ON users (age + ?)")).toBe(
      "parameters are not allowed in an index",
    );
    expect(semanticError("CREATE INDEX i ON users (count(*))")).toBe(
      "aggregate functions are not allowed in an index",
    );
    expect(semanticError("CREATE INDEX i ON users (random())")).toBe(
      "random() is not allowed in an index: it gives another value each call",
    );
    expect(semanticError("CREATE INDEX i ON users (age + random())")).toBe(
      "random() is not allowed in an index: it gives another value each call",
    );
  });

  it("refuses names in use, reserved names and missing tables or columns", () => {
    expect(semanticError("CREATE INDEX users ON orders (id)")).toBe(
      "there is already a table named users",
    );
    expect(
      createIndex("CREATE INDEX IF NOT EXISTS orders_user ON orders (id)")
        .isIfNotExists,
    ).toBe(true);
    expect(semanticError("CREATE INDEX opfsql_i ON orders (id)")).toBe(
      "opfsql_i is reserved: names starting with opfsql_ belong to opfsql",
    );
    expect(semanticError("CREATE INDEX i ON nowhere (id)")).toBe(
      "no such table: nowhere",
    );
    expect(semanticError("CREATE INDEX i ON users (nope)")).toBe(
      "no such column: nope",
    );
  });

  it("does not support keys of BLOB or a whole JSON column", () => {
    expect(notSupported("CREATE INDEX i ON users (data)")).toBe(
      "an index key on a JSON column",
    );
    expect(notSupported("CREATE INDEX i ON users (x'00')")).toBe(
      "an index key of type BLOB",
    );
  });
});

describe("ALTER TABLE", () => {
  it("adds a column with its default converted", () => {
    expect(
      bind("ALTER TABLE users ADD COLUMN score REAL DEFAULT 0"),
    ).toMatchObject({
      kind: "alter-table",
      table: { name: "users" },
      action: {
        kind: "add-column",
        column: {
          name: "score",
          type: "REAL",
          isNotNull: false,
          defaultValue: 0,
        },
      },
    });
    expect(
      bind("ALTER TABLE users ADD k TEXT NOT NULL DEFAULT 'a'"),
    ).toMatchObject({
      action: { column: { isNotNull: true, defaultValue: "a" } },
    });
  });

  it("refuses an added key, a duplicate, and NOT NULL without a default", () => {
    expect(semanticError("ALTER TABLE users ADD COLUMN NAME TEXT")).toBe(
      "duplicate column name: NAME",
    );
    expect(
      semanticError("ALTER TABLE users ADD COLUMN k INTEGER PRIMARY KEY"),
    ).toBe("cannot add a PRIMARY KEY column");
    expect(semanticError("ALTER TABLE users ADD COLUMN k TEXT UNIQUE")).toBe(
      "cannot add a UNIQUE column",
    );
    expect(semanticError("ALTER TABLE users ADD COLUMN k TEXT NOT NULL")).toBe(
      "cannot add a NOT NULL column with default value NULL",
    );
  });

  it("drops a column no key needs", () => {
    expect(bind("ALTER TABLE users DROP COLUMN age")).toMatchObject({
      kind: "alter-table",
      action: { kind: "drop-column", column: 2 },
    });
  });

  it("refuses to drop the rowid, an indexed column or the last one", () => {
    const refusal = (column: string, table: string, why: string): string =>
      `cannot drop column ${column} of ${table}: ${why}`;
    expect(semanticError("ALTER TABLE users DROP COLUMN id")).toBe(
      refusal("id", "users", "it is the table's INTEGER PRIMARY KEY"),
    );
    expect(semanticError("ALTER TABLE users DROP COLUMN email")).toBe(
      refusal("email", "users", "index opfsql_autoindex_users_1 uses it"),
    );
    expect(semanticError("ALTER TABLE users DROP COLUMN name")).toBe(
      refusal("name", "users", "index users_lower_name uses it"),
    );
    expect(semanticError("ALTER TABLE items DROP COLUMN sku")).toBe(
      refusal("sku", "items", "index opfsql_autoindex_items_1 uses it"),
    );
    expect(semanticError("ALTER TABLE users DROP COLUMN nope")).toBe(
      "table users has no column named nope",
    );
  });
});

describe("DROP", () => {
  it("names what exists as the schema has it", () => {
    expect(bind("DROP TABLE Users")).toEqual({
      kind: "drop",
      object: "table",
      name: "users",
      isIfExists: false,
    });
    expect(bind("DROP INDEX USERS_LOWER_NAME")).toEqual({
      kind: "drop",
      object: "index",
      name: "users_lower_name",
      isIfExists: false,
    });
  });

  it("refuses what does not exist, unless IF EXISTS", () => {
    expect(semanticError("DROP TABLE nope")).toBe("no such table: nope");
    expect(semanticError("DROP TABLE orders_user")).toBe(
      "no such table: orders_user",
    );
    expect(semanticError("DROP INDEX users")).toBe("no such index: users");
    expect(bind("DROP TABLE IF EXISTS nope")).toMatchObject({
      name: "nope",
      isIfExists: true,
    });
  });

  it("never drops a constraint's index", () => {
    expect(semanticError("DROP INDEX opfsql_autoindex_users_1")).toBe(
      "index opfsql_autoindex_users_1 belongs to a PRIMARY KEY or UNIQUE constraint of users and cannot be dropped",
    );
  });
});

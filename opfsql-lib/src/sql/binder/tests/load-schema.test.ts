import { describe, expect, it } from "vitest";
import { CorruptError } from "../../../errors.js";
import {
  constraintIndexName,
  createIndexSql,
  createTableSql,
  Schema,
  type Column,
  type Index,
  type IndexShape,
  type Table,
  type TableShape,
} from "../../../schema/index.js";
import { formatPlan } from "../../plan/index.js";
import {
  bindIndexKey,
  bindStatement,
  loadSchema,
  type SchemaRow,
} from "../index.js";
import { bind, lines, only, semanticError } from "./helpers.js";

// ---------------------------------------------------------------------------
// The schema table's statements are generated from the model; loading them
// must give back that model exactly: model → createTableSql/createIndexSql
// → loadSchema → the same model.
// ---------------------------------------------------------------------------

function column(
  name: string,
  type: Column["type"],
  extra: Partial<Column> = {},
): Column {
  return { name, type, isNotNull: false, defaultValue: null, ...extra };
}

/** A table and its constraint indexes, as the catalog stores them. */
interface Stored {
  readonly table: Table;
  readonly constraints: readonly Index[];
}

let nextPage = 10;

function stored(
  shape: TableShape,
  constraints: readonly IndexShape[] = [],
): Stored {
  const table = { ...shape, rootPage: nextPage++ };
  const indexes = constraints.map((index) => ({
    ...index,
    rootPage: nextPage++,
  }));
  return { table, constraints: indexes };
}

function constraint(
  table: string,
  ordinal: number,
  origin: "primary-key" | "unique",
  columns: readonly number[],
): IndexShape {
  return {
    name: constraintIndexName(table, ordinal),
    table,
    keys: columns.map((position) => ({ kind: "column", column: position })),
    isUnique: true,
    origin,
  };
}

/** The schema table's rows for `tables` and the statement `indexes`. */
function rowsOf(
  tables: readonly Stored[],
  indexes: readonly Index[],
): SchemaRow[] {
  const tableRows = tables.flatMap(({ table, constraints }): SchemaRow[] => [
    {
      type: "table",
      name: table.name,
      tableName: table.name,
      rootPage: table.rootPage,
      sql: createTableSql(table, constraints),
    },
    ...constraints.map((index): SchemaRow => ({
      type: "index",
      name: index.name,
      tableName: index.table,
      rootPage: index.rootPage,
      sql: null,
    })),
  ]);
  const indexRows = indexes.map((index): SchemaRow => {
    const table = tables.find((item) => item.table.name === index.table);
    if (table === undefined) throw new TypeError(`no table ${index.table}`);
    return {
      type: "index",
      name: index.name,
      tableName: index.table,
      rootPage: index.rootPage,
      sql: createIndexSql(index, table.table),
    };
  });
  return [...tableRows, ...indexRows];
}

const everyDefault = stored(
  {
    name: "defaults",
    columns: [
      column("id", "INTEGER"),
      column("i", "INTEGER", {
        defaultValue: -9_007_199_254_740_991,
        isNotNull: true,
      }),
      column("r", "REAL", { defaultValue: 2 }),
      column("fraction", "REAL", { defaultValue: -0.125 }),
      column("huge", "REAL", { defaultValue: Infinity }),
      column("tiny", "REAL", { defaultValue: -Infinity }),
      column("t", "TEXT", { defaultValue: `it's "Été" 🙂` }),
      column("empty", "TEXT", { defaultValue: "" }),
      column("b", "BLOB", {
        defaultValue: Uint8Array.from([0, 1, 0xfe, 0xff]),
      }),
      column("yes", "BOOLEAN", { defaultValue: true }),
      column("no", "BOOLEAN", { defaultValue: false }),
      column("object", "JSON", {
        defaultValue: { b: [1, "x", null], a: true },
      }),
      column("array", "JSON", { defaultValue: [1, [2], { c: 3 }] }),
      column("number", "JSON", { defaultValue: 1.5 }),
      column("flag", "JSON", { defaultValue: false }),
      column("anything", "ANY", { defaultValue: "a" }),
      column("number_any", "ANY", { defaultValue: 7 }),
    ],
    rowidColumn: 0,
    isAutoincrement: true,
  },
  [],
);

const compositeKey = stored(
  {
    name: "lines",
    columns: [
      column("order", "INTEGER", { isNotNull: true }),
      column("sku", "TEXT", { isNotNull: true }),
      column("note", "TEXT"),
      column("code", "TEXT"),
    ],
    rowidColumn: null,
    isAutoincrement: false,
  },
  [
    constraint("lines", 1, "unique", [1]),
    constraint("lines", 2, "unique", [2]),
    constraint("lines", 3, "unique", [3]),
    constraint("lines", 4, "primary-key", [0, 1]),
  ],
);

const keyedByText = stored(
  {
    name: "Odd Names",
    columns: [
      column("Key", "TEXT", { isNotNull: true }),
      column('say "hi"', "INTEGER"),
      column("select", "ANY"),
      column("Été", "REAL", { isNotNull: true, defaultValue: 0 }),
    ],
    rowidColumn: null,
    isAutoincrement: false,
  },
  [
    constraint("Odd Names", 1, "primary-key", [0]),
    constraint("Odd Names", 2, "unique", [0]),
    constraint("Odd Names", 3, "unique", [1]),
  ],
);

const rowidUnique = stored(
  {
    name: "plain",
    columns: [
      column("id", "INTEGER"),
      column("data", "JSON"),
      column("x", "ANY"),
    ],
    rowidColumn: 0,
    isAutoincrement: false,
  },
  [constraint("plain", 1, "unique", [0])],
);

const noKey = stored({
  name: "loose",
  columns: [column("a", "ANY"), column("b", "BOOLEAN")],
  rowidColumn: null,
  isAutoincrement: false,
});

/** opfsql's own counters: a reserved name the loader accepts. */
const sequence = stored({
  name: "opfsql_sequence",
  columns: [column("name", "TEXT"), column("seq", "INTEGER")],
  rowidColumn: null,
  isAutoincrement: false,
});

const TABLES = [
  everyDefault,
  compositeKey,
  keyedByText,
  rowidUnique,
  noKey,
  sequence,
];

/** Statement indexes, bound from CREATE INDEX so their keys are canonical. */
function statementIndexes(schema: Schema): Index[] {
  return [
    "CREATE INDEX by_text ON defaults (t, lower(t), length(t) * 2)",
    "CREATE UNIQUE INDEX by_json ON plain (data.city, data.tags[0], json_extract(data, '$.a'))",
    `CREATE INDEX odd ON "Odd Names" ("Été" / 2.0, -"say ""hi""", CASE WHEN "select" IS NULL THEN 'none' ELSE 'some' END)`,
    "CREATE INDEX casts ON loose (CAST(a AS INTEGER), b, NOT b, a || 'x', coalesce(a, 1))",
  ].map((sql) => {
    const bound = bindStatement(only(sql), schema);
    if (bound.kind !== "create-index") throw new TypeError(sql);
    return { ...bound.index, rootPage: nextPage++ };
  });
}

describe("loadSchema", () => {
  it("gives back the model the schema table's statements were generated from", () => {
    const tables = new Schema(
      1,
      TABLES.map((item) => item.table),
      TABLES.flatMap((item) => item.constraints),
    );
    const indexes = statementIndexes(tables);
    const loaded = loadSchema(7, rowsOf(TABLES, indexes));
    expect(loaded.version).toBe(7);
    expect(loaded.tables).toEqual(TABLES.map((item) => item.table));
    expect(loaded.indexes).toEqual([
      ...TABLES.flatMap((item) => item.constraints),
      ...indexes,
    ]);
    expect(loaded.problems()).toEqual([]);
  });

  it("gives back a JSON column's string default", () => {
    const text = stored({
      name: "texts",
      columns: [column("j", "JSON", { defaultValue: "it's text" })],
      rowidColumn: null,
      isAutoincrement: false,
    });
    expect(loadSchema(1, rowsOf([text], [])).tables).toEqual([text.table]);
  });

  it("gives back what CREATE TABLE bound", () => {
    const sql = [
      "CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, x TEXT UNIQUE NOT NULL DEFAULT 'x')",
      "CREATE TABLE b (p TEXT UNIQUE, q BIGINT, r BLOB DEFAULT x'beef', PRIMARY KEY (q, p))",
      "CREATE TABLE c (k TEXT UNIQUE PRIMARY KEY, j JSON DEFAULT '[1, {\"z\": 2}]', f REAL DEFAULT 1)",
      "CREATE TABLE d (u INTEGER UNIQUE, v INTEGER, PRIMARY KEY (v))",
    ];
    const bound = sql.map((statement) => {
      const result = bindStatement(only(statement), Schema.empty);
      if (result.kind !== "create-table") throw new TypeError(statement);
      return stored(result.table, result.constraints);
    });
    const loaded = loadSchema(1, rowsOf(bound, []));
    expect(loaded.tables).toEqual(bound.map((item) => item.table));
    expect(loaded.indexes).toEqual(bound.flatMap((item) => item.constraints));
  });

  it("reports a row that does not load as corruption naming it", () => {
    const { table } = noKey;
    const tableRow: SchemaRow = {
      type: "table",
      name: table.name,
      tableName: table.name,
      rootPage: table.rootPage,
      sql: createTableSql(table, []),
    };
    const corrupt = (rows: readonly SchemaRow[]): string => {
      try {
        loadSchema(1, rows);
      } catch (error) {
        if (error instanceof CorruptError) return error.message;
        throw error;
      }
      throw new Error("loaded");
    };
    expect(
      corrupt([{ ...tableRow, sql: "CREATE TABLE loose (a NUMBER)" }]),
    ).toBe("the schema of table loose does not load: unknown type: NUMBER");
    expect(corrupt([{ ...tableRow, sql: "CREATE TABLE (" }])).toMatch(
      /^the schema of table loose does not load: expected a table name/,
    );
    expect(corrupt([{ ...tableRow, sql: null }])).toBe(
      "the schema of table loose does not load: it has no statement",
    );
    expect(corrupt([{ ...tableRow, sql: "CREATE TABLE other (a ANY)" }])).toBe(
      "the schema of table loose does not load: its statement names other",
    );
    expect(corrupt([{ ...tableRow, sql: "SELECT 1" }])).toBe(
      "the schema of table loose does not load: its statement is not one CREATE_TABLE_STATEMENT: SELECT 1",
    );
    const indexRow: SchemaRow = {
      type: "index",
      name: "i",
      tableName: "loose",
      rootPage: 99,
      sql: 'CREATE INDEX "i" ON "loose" (nosuchfn(a))',
    };
    expect(corrupt([tableRow, indexRow])).toBe(
      "the schema of index i does not load: no such function: nosuchfn",
    );
    expect(corrupt([tableRow, { ...indexRow, sql: null }])).toBe(
      "the schema of index i does not load: it has no statement and no constraint of loose is named so",
    );
    expect(corrupt([tableRow, { ...indexRow, tableName: "gone" }])).toBe(
      "the schema of index i does not load: its table gone has no row",
    );
    const keyed = stored(
      { ...table, name: "keyed", columns: [column("a", "TEXT")] },
      [constraint("keyed", 1, "unique", [0])],
    );
    const [keyedRow] = rowsOf([keyed], []);
    expect(corrupt(keyedRow === undefined ? [] : [keyedRow])).toBe(
      "table keyed has a constraint index opfsql_autoindex_keyed_1 with no row in opfsql_schema",
    );
  });

  it("accepts reserved names, which user statements may not touch", () => {
    const loaded = loadSchema(1, rowsOf([sequence], []));
    expect(loaded.table("opfsql_sequence")).toEqual(sequence.table);
    expect(semanticError("DROP TABLE opfsql_sequence", loaded)).toBe(
      "table opfsql_sequence may not be dropped",
    );
    expect(semanticError("ALTER TABLE opfsql_sequence ADD x INT", loaded)).toBe(
      "table opfsql_sequence may not be altered",
    );
    expect(
      semanticError("CREATE INDEX i ON opfsql_sequence (seq)", loaded),
    ).toBe("table opfsql_sequence may not be indexed");
    for (const sql of [
      "INSERT INTO opfsql_sequence VALUES ('t', 0)",
      "UPDATE opfsql_sequence SET seq = 0",
      "DELETE FROM opfsql_sequence",
    ]) {
      expect(semanticError(sql, loaded)).toBe(
        "table opfsql_sequence may not be modified",
      );
    }
    expect(bind("SELECT name, seq FROM opfsql_sequence", loaded).kind).toBe(
      "query",
    );
  });
});

describe("bindIndexKey", () => {
  it("binds a stored key over the table's columns, bound to the index given", () => {
    const { table } = everyDefault;
    expect(
      formatPlan({
        kind: "project",
        tableIndex: 1,
        child: { kind: "values", tableIndex: 0, rows: [[]], types: [] },
        expressions: [bindIndexKey("lower(t) || -i", table, 7)],
      }),
    ).toBe(lines("project #1 (lower(t#7.6) || (-i#7.1))", "  values #0 ()"));
  });

  it("reports a key that does not parse or bind as corruption naming it", () => {
    const { table } = everyDefault;
    expect(() => bindIndexKey("lower(", table, 0)).toThrow(CorruptError);
    expect(() => bindIndexKey("random()", table, 0)).toThrow(
      "the index key random() of table defaults does not bind: random() is not allowed in an index: it gives another value each call",
    );
    expect(() => bindIndexKey("nope + 1", table, 0)).toThrow(
      "the index key nope + 1 of table defaults does not bind: no such column: nope",
    );
  });
});

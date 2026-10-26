import type { Random } from "./random.js";

// ---------------------------------------------------------------------------
// Random schemas: a few tables of every column type, sometimes an INTEGER
// PRIMARY KEY or a UNIQUE column, and random indexes — on columns, several
// columns, and expressions — which only the indexed copy of a database gets.
// ---------------------------------------------------------------------------

export type ColumnType =
  "INTEGER" | "REAL" | "TEXT" | "BOOLEAN" | "ANY" | "JSON";

export const COLUMN_TYPES: readonly ColumnType[] = [
  "INTEGER",
  "REAL",
  "TEXT",
  "BOOLEAN",
  "ANY",
  "JSON",
];

/** The keys the JSON documents use, so that paths find something. */
export const JSON_KEYS = ["k", "s"] as const;
/** The key path indexes are on. */
export const JSON_PATH_KEY = "k";

export interface ColumnSpec {
  readonly name: string;
  readonly type: ColumnType;
  readonly constraint: "primary key" | "unique" | "not null" | null;
}

export interface TableSpec {
  readonly name: string;
  readonly columns: readonly ColumnSpec[];
  /** Rows to generate; UNIQUE may refuse some of them. */
  readonly rowCount: number;
}

/** A key of an index: a column, or an expression of one that queries also write. */
export interface IndexKey {
  readonly column: ColumnSpec;
  readonly form: "column" | "lower" | "abs" | "path";
}

export interface IndexSpec {
  readonly name: string;
  readonly table: string;
  readonly keys: readonly IndexKey[];
  /** Created before the rows are inserted, so that inserting maintains it. */
  readonly isEarly: boolean;
}

export interface DatabaseSpec {
  readonly tables: readonly TableSpec[];
  /** The indexed copy's own indexes. */
  readonly indexes: readonly IndexSpec[];
}

const MAX_TABLES = 3;
const MAX_INDEXES_PER_TABLE = 3;
/** NOT NULL lets the optimizer assume what a LEFT JOIN can still undo. */
const NOT_NULL_CHANCE = 0.15;
/** Mostly small tables; now and then one large enough to split pages. */
const LARGE_TABLE_CHANCE = 0.1;

export function generateDatabase(random: Random): DatabaseSpec {
  const tableCount = random.between(1, MAX_TABLES);
  const tables = Array.from({ length: tableCount }, (_, index) =>
    generateTable(random, `t${String(index)}`),
  );
  const indexes = tables.flatMap((table) => generateIndexes(random, table));
  return { tables, indexes };
}

function generateTable(random: Random, name: string): TableSpec {
  const columns: ColumnSpec[] = [];
  if (random.chance(0.5)) {
    columns.push({ name: "id", type: "INTEGER", constraint: "primary key" });
  }
  // Types may repeat, so that two columns of a table can be compared.
  const columnCount = random.between(3, 6);
  for (let index = 0; index < columnCount; index++) {
    const type = random.pick(COLUMN_TYPES);
    const constraint = random.chance(NOT_NULL_CHANCE) ? "not null" : null;
    columns.push({ name: `c${String(index)}`, type, constraint });
  }
  const unique = random.chance(0.3) ? uniqueCandidate(random, columns) : null;
  const withUnique = columns.map((column) =>
    column === unique ? { ...column, constraint: "unique" as const } : column,
  );
  const isLarge = random.chance(LARGE_TABLE_CHANCE);
  const rowCount = isLarge ? random.between(200, 400) : random.between(0, 40);
  return { name, columns: withUnique, rowCount };
}

/** A column that may be UNIQUE: not the primary key, and not JSON. */
function uniqueCandidate(
  random: Random,
  columns: readonly ColumnSpec[],
): ColumnSpec | null {
  const candidates = columns.filter(
    (column) => column.constraint === null && column.type !== "JSON",
  );
  return candidates.length === 0 ? null : random.pick(candidates);
}

function generateIndexes(random: Random, table: TableSpec): IndexSpec[] {
  const count = random.below(MAX_INDEXES_PER_TABLE + 1);
  return Array.from({ length: count }, (_, index) => ({
    name: `${table.name}_i${String(index)}`,
    table: table.name,
    keys: indexKeys(random, table),
    isEarly: random.chance(0.5),
  }));
}

function indexKeys(random: Random, table: TableSpec): IndexKey[] {
  const keyCount = random.chance(0.3) ? 2 : 1;
  return Array.from({ length: keyCount }, () => indexKey(random, table));
}

function indexKey(random: Random, table: TableSpec): IndexKey {
  const column = random.pick(table.columns);
  const isExpression = random.chance(0.4);
  return { column, form: isExpression ? expressionForm(column) : "column" };
}

function expressionForm(column: ColumnSpec): IndexKey["form"] {
  switch (column.type) {
    case "TEXT": {
      return "lower";
    }
    case "JSON": {
      return "path";
    }
    case "INTEGER": {
      return "abs";
    }
    case "REAL":
    case "BOOLEAN":
    case "ANY": {
      return "column";
    }
  }
}

/** A key as SQL, its column read through `alias` when there is one. */
export function keySql(key: IndexKey, alias?: string): string {
  const column =
    alias === undefined ? key.column.name : `${alias}.${key.column.name}`;
  switch (key.form) {
    case "column": {
      return column;
    }
    case "lower": {
      return `lower(${column})`;
    }
    case "abs": {
      return `abs(${column})`;
    }
    case "path": {
      return `${column}.${JSON_PATH_KEY}`;
    }
  }
}

export function createTableSql(table: TableSpec): string {
  const columns = table.columns.map((column) => {
    const constraint = column.constraint?.toUpperCase();
    const parts = [column.name, column.type, constraint];
    return parts.filter((part) => part !== undefined).join(" ");
  });
  return `CREATE TABLE ${table.name} (${columns.join(", ")})`;
}

export function createIndexSql(index: IndexSpec): string {
  const keys = index.keys.map((key) => keySql(key));
  return `CREATE INDEX ${index.name} ON ${index.table} (${keys.join(", ")})`;
}

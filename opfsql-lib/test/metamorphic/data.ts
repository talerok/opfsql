import type { Random } from "./random.js";
import type { ColumnSpec, ColumnType, TableSpec } from "./schema.js";

// ---------------------------------------------------------------------------
// Values, as SQL literals. They come from small pools, so that values repeat
// and conditions and joins match some rows and not others; and a fair share
// are NULL. The query generator draws its constants from the same pools.
// ---------------------------------------------------------------------------

/** One row as SQL literals, in column order. */
export type Row = readonly string[];

export const INTEGERS = [
  "-7",
  "-1",
  "0",
  "1",
  "2",
  "3",
  "5",
  "8",
  "13",
  "42",
  "100",
  "1000",
];
/** Fractions, and integral values that equal INTEGER ones. */
export const REALS = ["-2.5", "-0.25", "0.5", "1.5", "2.75", "13.5", "0", "2"];
export const TEXTS = [
  "''",
  "'a'",
  "'A'",
  "'ab'",
  "'Ab'",
  "'abc'",
  "'b'",
  "'B'",
  "'ba'",
  "'a b'",
  "' a'",
  "'é'",
  "'É'",
  "'x_y'",
  "'10'",
  "'2'",
];
export const BOOLEANS = ["TRUE", "FALSE"];
/** LIKE patterns over the text pool. */
export const PATTERNS = ["'a%'", "'%b'", "'_'", "'%'", "'A_'", "'%a%'", "''"];

const NULL_SHARE = 0.2;

export function generateRows(random: Random, table: TableSpec): Row[] {
  const rows: Row[] = [];
  for (let index = 0; index < table.rowCount; index++) {
    rows.push(table.columns.map((column) => valueFor(random, column, index)));
  }
  return rows;
}

function valueFor(random: Random, column: ColumnSpec, index: number): string {
  if (column.constraint === "primary key") return keyFor(random, index);
  if (random.chance(NULL_SHARE)) return "NULL";
  return literalOf(random, column.type);
}

/** Ascending keys with gaps, and now and then NULL, which assigns one. */
function keyFor(random: Random, index: number): string {
  if (random.chance(0.1)) return "NULL";
  return String(index * 3 + random.below(3));
}

/** A value of `type`, never NULL. */
export function literalOf(random: Random, type: ColumnType): string {
  switch (type) {
    case "INTEGER": {
      return random.pick(INTEGERS);
    }
    case "REAL": {
      return random.pick(REALS);
    }
    case "TEXT": {
      return random.pick(TEXTS);
    }
    case "BOOLEAN": {
      return random.pick(BOOLEANS);
    }
    case "ANY": {
      return random.pick([...INTEGERS, ...REALS, ...TEXTS, ...BOOLEANS]);
    }
    case "JSON": {
      return jsonLiteral(random);
    }
  }
}

/** A document: mostly an object with some of the keys, else an array or a scalar. */
function jsonLiteral(random: Random): string {
  const choice = random.below(10);
  if (choice === 0) return `'[${random.pick(INTEGERS)}, 2]'`;
  if (choice === 1) return `'${random.pick(INTEGERS)}'`;
  const fields: string[] = [];
  if (random.chance(0.8)) fields.push(`"k": ${jsonScalar(random)}`);
  if (random.chance(0.6)) fields.push(`"s": ${jsonScalar(random)}`);
  return `'{${fields.join(", ")}}'`;
}

function jsonScalar(random: Random): string {
  switch (random.below(4)) {
    case 0: {
      return random.pick(INTEGERS);
    }
    case 1: {
      return JSON.stringify(random.pick(["a", "ab", "B", "é", "2"]));
    }
    case 2: {
      return random.pick(["true", "false", "null"]);
    }
    default: {
      return random.pick(REALS);
    }
  }
}

export function insertSql(table: TableSpec, row: Row): string {
  return `INSERT INTO ${table.name} VALUES (${row.join(", ")})`;
}

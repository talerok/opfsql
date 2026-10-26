import { print } from "./expression.js";
import { ExpressionGenerator } from "./generate-expression.js";
import type { Random } from "./random.js";
import type { DatabaseSpec, TableSpec } from "./schema.js";
import { AliasSource, tableLeaves } from "./scope.js";

// ---------------------------------------------------------------------------
// What a FROM item reads: mostly a table, now and then a derived table over
// one — with a WHERE, DISTINCT, a UNION ALL or an ORDER BY … LIMIT, which
// decide what a condition outside may be pushed into it.
// ---------------------------------------------------------------------------

export interface Source {
  /** The FROM item without its alias: a table's name, or a query in parentheses. */
  readonly sql: string;
  /** Its columns, as a table's. */
  readonly table: TableSpec;
  /** Whether it is a table, whose keys an access path can seek. */
  readonly isTable: boolean;
  /** The most rows it can have. */
  readonly maxRows: number;
}

const DERIVED_CHANCE = 0.2;

/** The source at `position` in the FROM clause. */
export function generateSource(
  random: Random,
  database: DatabaseSpec,
  position: number,
): Source {
  const table = random.pick(database.tables);
  if (!random.chance(DERIVED_CHANCE)) {
    return { sql: table.name, table, isTable: true, maxRows: table.rowCount };
  }
  return derivedOver(random, database, table, `x${String(position)}`);
}

/** `(SELECT [DISTINCT] columns FROM table [WHERE …] [UNION ALL …] [ORDER BY … LIMIT n])`. */
function derivedOver(
  random: Random,
  database: DatabaseSpec,
  table: TableSpec,
  alias: string,
): Source {
  const columns = random.some(
    table.columns,
    random.between(1, table.columns.length),
  );
  const list = columns.map(
    (column) => `${alias}.${column.name} AS ${column.name}`,
  );
  const select = (keyword: string): string =>
    `SELECT ${keyword}${list.join(", ")} FROM ${table.name} AS ${alias}${where(random, database, table, alias)}`;
  const first = select(random.chance(0.2) ? "DISTINCT " : "");
  const isUnion = random.chance(0.15);
  const union = isUnion ? ` UNION ALL ${select("")}` : "";
  const positions = columns.map((_, index) => String(index + 1));
  const limit = random.chance(0.2)
    ? ` ORDER BY ${positions.join(", ")} LIMIT ${String(random.between(0, 10))}`
    : "";
  const derived = {
    ...table,
    columns: columns.map((column) => ({ ...column, constraint: null })),
  };
  const maxRows = isUnion ? table.rowCount * 2 : table.rowCount;
  return {
    sql: `(${first}${union}${limit})`,
    table: derived,
    isTable: false,
    maxRows,
  };
}

function where(
  random: Random,
  database: DatabaseSpec,
  table: TableSpec,
  alias: string,
): string {
  if (random.chance(0.5)) return "";
  const scope = { leaves: tableLeaves(table, alias), database };
  const generator = new ExpressionGenerator(random, scope, new AliasSource());
  return ` WHERE ${print(generator.condition(2))}`;
}

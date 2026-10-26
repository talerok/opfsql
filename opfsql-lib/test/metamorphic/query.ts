import type { Value } from "../../src/values/index.js";
import {
  areComparable,
  type Expression,
  type Leaf,
  leaf,
  node,
  parameterized,
  print,
} from "./expression.js";
import { AliasSource, ExpressionGenerator } from "./generate-expression.js";
import type { Random } from "./random.js";
import { seekableCondition } from "./seekable.js";
import { keyLeaf, seekKeys, tableLeaves } from "./scope.js";
import type { DatabaseSpec, TableSpec } from "./schema.js";

// ---------------------------------------------------------------------------
// One case: a FROM clause — a table, or a chain of up to three joins — and
// the expressions the oracles build their queries from.
// ---------------------------------------------------------------------------

export interface FromClause {
  readonly sql: string;
  readonly leaves: readonly Leaf[];
  /** The keys of its tables an access path can seek on, as queries write them. */
  readonly keys: readonly Leaf[];
}

export interface QueryCase {
  readonly from: FromClause;
  /** The condition TLP partitions on and NoREC counts. */
  readonly condition: Expression;
  /** The select list of the queries that return rows. */
  readonly outputs: readonly Expression[];
  readonly groupKey: Expression;
  /** What MIN, MAX and count take. */
  readonly measured: Expression;
  /** What SUM takes: an INTEGER, so that sums are exact in any order. */
  readonly summed: Expression;
  /** The condition TLP partitions groups on, over the group key and aggregates. */
  readonly groupCondition: Expression;
  /** `condition` with some of its constants as parameters, and their values. */
  readonly parameterized: {
    readonly expression: Expression;
    readonly values: Value[];
  };
  readonly limit: number;
  readonly offset: number;
}

const TABLE_ALIASES = ["a", "b", "c"];
/**
 * The most rows a join may make, every pair matching: every form reads the
 * FROM, so this bounds a case's time.
 */
const MAX_JOINED_ROWS = 20_000;
const CONDITION_DEPTH = 3;
const PARAMETER_CHANCE = 0.6;
const VALUE_DEPTH = 2;

export function generateCase(
  random: Random,
  database: DatabaseSpec,
): QueryCase {
  const aliases = new AliasSource();
  const from = generateFrom(random, database);
  const generator = new ExpressionGenerator(
    random,
    { leaves: from.leaves, tables: database.tables },
    aliases,
  );
  // Grouping reads no subquery: GROUP BY and the aggregates stay simple.
  const plain = new ExpressionGenerator(
    random,
    { leaves: from.leaves, tables: [] },
    aliases,
  );
  const outputCount = random.between(1, 3);
  const groupKey = generateGroupKey(random, from);
  const measured = plain.anyValue(1);
  const summed = plain.value("integer", 1);
  const condition = generateCondition(random, generator, from);
  const values: Value[] = [];
  const isParameter = (): boolean => random.chance(PARAMETER_CHANCE);
  return {
    from,
    condition,
    outputs: Array.from({ length: outputCount }, () =>
      generator.anyValue(VALUE_DEPTH),
    ),
    groupKey,
    measured,
    summed,
    groupCondition: generateGroupCondition(random, {
      groupKey,
      measured,
      summed,
    }),
    parameterized: {
      expression: parameterized(condition, isParameter, values),
      values,
    },
    limit: random.between(0, 8),
    offset: random.chance(0.5) ? 0 : random.between(1, 5),
  };
}

/**
 * A condition: random, or one an access path can use — keys against
 * constants — alone or with a random one, so that the optimizer seeks
 * indexes the indexed copy has and the plain one does not.
 */
function generateCondition(
  random: Random,
  generator: ExpressionGenerator,
  from: FromClause,
): Expression {
  const free = generator.condition(CONDITION_DEPTH);
  const seekable = seekableCondition(random, from.keys);
  if (seekable === null) return free;
  switch (random.below(4)) {
    case 0: {
      return seekable;
    }
    case 1: {
      return node("boolean", seekable, " AND ", free);
    }
    case 2: {
      return node("boolean", seekable, " OR ", free);
    }
    default: {
      return free;
    }
  }
}

/** A column of the FROM, now and then in an expression an index may be on. */
function generateGroupKey(random: Random, from: FromClause): Expression {
  const column = random.pick(from.leaves);
  if (!random.chance(0.3)) return column;
  if (column.type === "text") return node("text", "lower(", column, ")");
  if (column.type === "integer") return node("integer", "abs(", column, ")");
  return column;
}

/** A condition over what a group has: its key and aggregates. */
function generateGroupCondition(
  random: Random,
  parts: Pick<QueryCase, "groupKey" | "measured" | "summed">,
): Expression {
  const leaves = [
    leaf(parts.groupKey.type, print(parts.groupKey)),
    leaf("integer", "count(*)"),
    leaf("integer", `sum(${print(parts.summed)})`),
    leaf(parts.measured.type, `min(${print(parts.measured)})`),
    leaf(parts.measured.type, `max(${print(parts.measured)})`),
  ];
  const generator = new ExpressionGenerator(
    random,
    { leaves, tables: [] },
    new AliasSource(),
  );
  return generator.condition(2);
}

// --- FROM ------------------------------------------------------------------------

function generateFrom(random: Random, database: DatabaseSpec): FromClause {
  const tableCount = random.pick([1, 1, 2, 2, 3]);
  const first = random.pick(database.tables);
  const keysOf = (table: TableSpec, alias: string) =>
    seekKeys(database, table, false).map((key) => keyLeaf(key, alias));
  let sql = `${first.name} AS a`;
  let leaves = tableLeaves(first, "a");
  let keys = keysOf(first, "a");
  let pairs = first.rowCount + 1;
  for (const alias of TABLE_ALIASES.slice(1, tableCount)) {
    const table = random.pick(database.tables);
    pairs *= table.rowCount + 1;
    if (pairs > MAX_JOINED_ROWS) break;
    const joined = joinOf(random, { database, table, alias, leaves });
    sql += ` ${joined.sql}`;
    leaves = [...leaves, ...tableLeaves(table, alias)];
    keys = [...keys, ...keysOf(table, alias)];
  }
  return { sql, leaves, keys };
}

interface JoinSite {
  readonly database: DatabaseSpec;
  readonly table: TableSpec;
  readonly alias: string;
  /** What the tables already joined offer. */
  readonly leaves: readonly Leaf[];
}

/**
 * `JOIN`, `LEFT JOIN` or `CROSS JOIN` of the next table. An ON condition
 * equates a key of the new table — often one an index or the rowid is on,
 * so that a lookup join can seek it — with a comparable value of the
 * tables before, and sometimes adds a condition of its own.
 */
function joinOf(random: Random, site: JoinSite): { sql: string } {
  const target = `${site.table.name} AS ${site.alias}`;
  if (random.chance(0.1)) return { sql: `CROSS JOIN ${target}` };
  const kind = random.chance(0.5) ? "JOIN" : "LEFT JOIN";
  const key = joinKey(random, site);
  const partners = site.leaves.filter((each) =>
    areComparable(each.type, key.type),
  );
  const partner =
    partners.length > 0 ? random.pick(partners) : leaf(key.type, "NULL");
  let on = `${print(partner)} = ${print(key)}`;
  if (random.chance(0.3)) {
    const leaves = [...site.leaves, ...tableLeaves(site.table, site.alias)];
    const generator = new ExpressionGenerator(
      random,
      { leaves, tables: [] },
      new AliasSource(),
    );
    on += ` AND ${print(generator.condition(1))}`;
  }
  return { sql: `${kind} ${target} ON ${on}` };
}

/** A key of the new table: often one a seek can use, else any column. */
function joinKey(random: Random, site: JoinSite): Expression {
  const keys = seekKeys(site.database, site.table, true);
  const column = {
    column: random.pick(site.table.columns),
    form: "column" as const,
  };
  const isSeekable = keys.length > 0 && random.chance(0.7);
  return keyLeaf(isSeekable ? random.pick(keys) : column, site.alias);
}

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
import { ExpressionGenerator } from "./generate-expression.js";
import type { Random } from "./random.js";
import { anotherLiteral } from "./data.js";
import { seekableCondition } from "./seekable.js";
import { generateSource, type Source } from "./sources.js";
import { AliasSource, keyLeaf, seekKeys, tableLeaves } from "./scope.js";
import type { DatabaseSpec } from "./schema.js";

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
  /**
   * `condition` with some of its constants as parameters: their values,
   * and others of the same kinds to run the prepared statement with next.
   */
  readonly parameterized: {
    readonly expression: Expression;
    readonly values: readonly Value[];
    readonly others: readonly string[];
  };
  /** What ORDER BY puts first — often a key, so that an index can deliver the order — or null. */
  readonly orderKey: string | null;
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
    { leaves: from.leaves, database },
    aliases,
  );
  // Grouping reads no subquery: GROUP BY and the aggregates stay simple.
  const plain = new ExpressionGenerator(
    random,
    { leaves: from.leaves, database: null },
    aliases,
  );
  const outputCount = random.between(1, 3);
  const groupKey = generateGroupKey(random, from);
  // Often a key, so that MIN and MAX can read one end of an index.
  const measured = keyOrElse(random, from, () => plain.anyValue(1));
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
      others: values.map((value) => anotherLiteral(random, value)),
    },
    orderKey: generateOrderKey(random, from),
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
  return keyOrElse(random, from, () => random.pick(from.leaves));
}

/** Half the time a key an access path can seek, when there is one; else `otherwise`. */
function keyOrElse(
  random: Random,
  from: FromClause,
  otherwise: () => Expression,
): Expression {
  const isKey = from.keys.length > 0 && random.chance(0.5);
  return isKey ? random.pick(from.keys) : otherwise();
}

/** A key, in either direction and with NULLs placed either way, or null. */
function generateOrderKey(random: Random, from: FromClause): string | null {
  if (from.keys.length === 0 || random.chance(0.4)) return null;
  const direction = random.pick(["", " ASC", " DESC"]);
  const nulls = random.pick(["", " NULLS FIRST", " NULLS LAST"]);
  return `${print(random.pick(from.keys))}${direction}${nulls}`;
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
    { leaves, database: null },
    new AliasSource(),
  );
  return generator.condition(2);
}

// --- FROM ------------------------------------------------------------------------

function generateFrom(random: Random, database: DatabaseSpec): FromClause {
  const sourceCount = random.pick([1, 1, 2, 2, 3]);
  const keysOf = (source: Source, alias: string) =>
    source.isTable
      ? seekKeys(database, source.table, false).map((key) =>
          keyLeaf(key, alias),
        )
      : [];
  const first = generateSource(random, database, 0);
  let sql = `${first.sql} AS a`;
  let leaves = tableLeaves(first.table, "a");
  let keys = keysOf(first, "a");
  let maxJoinedRows = first.maxRows + 1;
  for (const [position, alias] of TABLE_ALIASES.slice(
    1,
    sourceCount,
  ).entries()) {
    const source = generateSource(random, database, position + 1);
    maxJoinedRows *= source.maxRows + 1;
    if (maxJoinedRows > MAX_JOINED_ROWS) break;
    sql += ` ${joinOf(random, { database, source, alias, leaves })}`;
    leaves = [...leaves, ...tableLeaves(source.table, alias)];
    keys = [...keys, ...keysOf(source, alias)];
  }
  return { sql, leaves, keys };
}

interface JoinSite {
  readonly database: DatabaseSpec;
  readonly source: Source;
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
function joinOf(random: Random, site: JoinSite): string {
  const target = `${site.source.sql} AS ${site.alias}`;
  if (random.chance(0.1)) return `CROSS JOIN ${target}`;
  const kind = random.chance(0.5) ? "JOIN" : "LEFT JOIN";
  const key = joinKey(random, site);
  const partners = site.leaves.filter((each) =>
    areComparable(each.type, key.type),
  );
  const partner =
    partners.length > 0 ? random.pick(partners) : leaf(key.type, "NULL");
  let on = `${print(partner)} = ${print(key)}`;
  if (random.chance(0.3)) {
    const leaves = [
      ...site.leaves,
      ...tableLeaves(site.source.table, site.alias),
    ];
    const generator = new ExpressionGenerator(
      random,
      { leaves, database: null },
      new AliasSource(),
    );
    on += ` AND ${print(generator.condition(1))}`;
  }
  return `${kind} ${target} ON ${on}`;
}

/** A key of the new table: often one a seek can use, else any column. */
function joinKey(random: Random, site: JoinSite): Expression {
  const { source } = site;
  const keys = source.isTable
    ? seekKeys(site.database, source.table, true)
    : [];
  const column = {
    column: random.pick(source.table.columns),
    form: "column" as const,
  };
  const isSeekable = keys.length > 0 && random.chance(0.7);
  return keyLeaf(isSeekable ? random.pick(keys) : column, site.alias);
}

import type { Table } from "../../schema/index.js";
import type { AggregateCall, logical, physical } from "../plan/index.js";
import type { IndexKeys, KeyPart } from "./key-parts.js";
import {
  equalTestOn,
  type KeyTest,
  keyTestOf,
  type SeekValue,
} from "./key-tests.js";
import { lowerExpressions } from "./lower-expression.js";
import { tableReadOf } from "./plan-tree.js";
import type { Planner } from "./planner.js";
import { sameExpression } from "./same-expression.js";

// ---------------------------------------------------------------------------
// Pass 6, MIN and MAX from one end of an index. `SELECT MIN(x) FROM t WHERE
// a = 1` reads one entry when an index starts with (a, x): the first entry
// after the prefix whose x is not NULL. Eligible only when the aggregate has
// no GROUP BY and exactly that one call, over the next key part, and every
// filter condition is an `=` on one of the leading parts — a seek finds
// exactly the entries `=` holds for (key-tests.ts). index-extreme has no
// residual (docs/planning.md, Physical plans), so any other condition, a
// range included, rules it out. It returns the decoded key, so never for a
// part that may hold a JSON object or array (holds).
// ---------------------------------------------------------------------------

type Extreme = physical.IndexExtreme["extreme"];

/** The aggregate's one MIN or MAX call, and where its value goes. */
interface Wanted {
  readonly aggregateIndex: number;
  readonly call: AggregateCall<logical.Plan>;
  readonly argument: logical.Expression;
  readonly extreme: Extreme;
}

export function indexExtreme(
  plan: logical.Aggregate,
  planner: Planner,
): physical.IndexExtreme | null {
  const wanted = extremeWanted(plan);
  const source = tableReadOf(plan.child);
  if (wanted === null || source === null) return null;
  const tests = asKeyTests(lowerExpressions(source.conditions, planner));
  if (tests === null) return null;
  for (const keys of planner.indexes.indexesOf(source.get)) {
    const extreme = extremeOn(keys, tests, wanted, source.get.table);
    if (extreme !== null) return extreme;
  }
  return null;
}

/** The one MIN or MAX call of an aggregate without groups, or null. */
function extremeWanted(plan: logical.Aggregate): Wanted | null {
  const [call, ...others] = plan.aggregates;
  if (call === undefined || others.length > 0 || plan.groups.length > 0) {
    return null;
  }
  const [argument, ...rest] = call.arguments;
  const extreme = extremeOf(call.function.name);
  if (extreme === null || argument === undefined || rest.length > 0) {
    return null;
  }
  return { aggregateIndex: plan.aggregateIndex, call, argument, extreme };
}

function extremeOf(name: string): Extreme | null {
  if (name === "min") return "MIN";
  return name === "max" ? "MAX" : null;
}

/** Every condition as a key test, or null if one is not. */
function asKeyTests(
  conditions: readonly physical.Expression[],
): KeyTest[] | null {
  const tests: KeyTest[] = [];
  for (const condition of conditions) {
    const test = keyTestOf(condition);
    if (test === null) return null;
    tests.push(test);
  }
  return tests;
}

/**
 * The extreme read from this index: the tests set its leading parts equal,
 * one test each, and the wanted argument is the part after them.
 */
function extremeOn(
  keys: IndexKeys,
  tests: readonly KeyTest[],
  wanted: Wanted,
  table: Table,
): physical.IndexExtreme | null {
  const equal = equalPrefixOf(keys, tests);
  const part = keys.parts[tests.length];
  const isArgument = part !== undefined && holds(part, wanted.argument);
  if (equal === null || !isArgument) return null;
  return {
    kind: "index-extreme",
    aggregateIndex: wanted.aggregateIndex,
    table,
    index: keys.index,
    equal,
    extreme: wanted.extreme,
    type: wanted.call.type,
  };
}

/**
 * The seek values of the index's leading parts, one `=` test each, every
 * test used once; null if the tests are not exactly that.
 */
function equalPrefixOf(
  keys: IndexKeys,
  tests: readonly KeyTest[],
): SeekValue[] | null {
  let unused = tests;
  const equal: SeekValue[] = [];
  for (const part of keys.parts.slice(0, tests.length)) {
    const found = equalTestOn(part, unused);
    if (found === null) return null;
    unused = unused.filter((keyTest) => keyTest !== found);
    equal.push(found.test.value);
  }
  return unused.length === 0 ? equal : null;
}

/**
 * True if the aggregate's argument is what the part holds, and the part's
 * decoded key is that value. A JSON object or array is keyed by its
 * canonical text, which loses the stored object's key order (physical.ts,
 * IndexExtreme); a JSON part holds them, and so may an ANY part, which
 * stores any value as given.
 */
function holds(part: KeyPart, argument: logical.Expression): boolean {
  const isDecodedExactly = part.type !== "JSON" && part.type !== "ANY";
  return isDecodedExactly && sameExpression(argument, part.expression);
}

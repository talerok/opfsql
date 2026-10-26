import type { AggregateCall, logical, physical } from "../plan/index.js";
import { type IndexKeys, indexesOf, type KeyPart } from "./key-parts.js";
import {
  fitsPart,
  isExact,
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
// filter condition is an exact `=` (key-tests.ts, isExact) on one of the
// leading parts. index-extreme has no residual (docs/planning.md, Physical
// plans), so a condition it would not enforce rules it out: a parameter, a
// range, anything else.
// ---------------------------------------------------------------------------

type Extreme = physical.IndexExtreme["extreme"];

export function indexExtreme(
  plan: logical.Aggregate,
  planner: Planner,
): physical.IndexExtreme | null {
  const wanted = extremeWanted(plan);
  const source = tableReadOf(plan.child);
  if (wanted === null || source === null) return null;
  const tests = keyTestsOf(lowerExpressions(source.conditions, planner));
  if (tests === null) return null;
  for (const keys of indexesOf(source.get, planner)) {
    const equal = exactPrefix(keys, tests);
    const part = keys.parts[tests.length];
    if (equal !== null && part !== undefined && holds(part, wanted.argument)) {
      return {
        kind: "index-extreme",
        aggregateIndex: plan.aggregateIndex,
        table: source.get.table,
        index: keys.index,
        equal,
        extreme: wanted.extreme,
        type: wanted.call.type,
      };
    }
  }
  return null;
}

interface Wanted {
  readonly call: AggregateCall<logical.Plan>;
  readonly argument: logical.Expression;
  readonly extreme: Extreme;
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
  return { call, argument, extreme };
}

function extremeOf(name: string): Extreme | null {
  if (name === "min") return "MIN";
  return name === "max" ? "MAX" : null;
}

/** Every condition as a key test, or null if one is not. */
function keyTestsOf(
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
 * The seek values of the index's leading parts, one exact `=` test each,
 * every test used once; null if the tests are not exactly that.
 */
function exactPrefix(
  keys: IndexKeys,
  tests: readonly KeyTest[],
): SeekValue[] | null {
  const used = new Set<KeyTest>();
  const equal: SeekValue[] = [];
  for (const part of keys.parts.slice(0, tests.length)) {
    const found = exactEqualOn(part, tests, used);
    if (found === null) return null;
    used.add(found.keyTest);
    equal.push(found.value);
  }
  return used.size === tests.length ? equal : null;
}

function exactEqualOn(
  part: KeyPart,
  tests: readonly KeyTest[],
  used: ReadonlySet<KeyTest>,
): { readonly keyTest: KeyTest; readonly value: SeekValue } | null {
  for (const keyTest of tests) {
    const { test } = keyTest;
    if (test.kind !== "equal" || used.has(keyTest)) continue;
    if (fitsPart(keyTest, part) && isExact(keyTest, part)) {
      return { keyTest, value: test.value };
    }
  }
  return null;
}

/** True if the aggregate's argument is what the part holds. */
function holds(part: KeyPart, argument: logical.Expression): boolean {
  return (
    argument.type === part.type && sameExpression(argument, part.expression)
  );
}

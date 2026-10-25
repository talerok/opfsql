import type { StaticType } from "../functions/index.js";
import type { InList, physical } from "../sql/plan/index.js";
import {
  foldTruth,
  membership,
  type Truth,
  type Value,
} from "../values/index.js";
import {
  comparisonConversion,
  compareTyped,
  shareAffinity,
} from "./comparison.js";
import { fixedValue } from "./parameter.js";
import type { CompiledExpression, Compiler, Run, Tuple } from "./scope.js";

// ---------------------------------------------------------------------------
// `x IN (list)`: true if some item equals x, as `=` compares — comparison
// affinity included, so the answer does not depend on whether the optimizer
// rewrote a short list into `=`s. A list of constants and parameters is
// hashed once; each row then costs one lookup.
// ---------------------------------------------------------------------------

/** A test of the needle's value for a row. */
type Membership = (row: Tuple, needle: Value) => Truth;

export function compileInList(
  expression: InList<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const needle = compiler.compile(expression.needle);
  const isIn =
    compileFixedList(expression, compiler) ??
    compileItems(expression, compiler);
  return (row) => isIn(row, needle(row));
}

/** Each item compared with x by `=`, the answers combined by OR. */
function compileItems(
  expression: InList<physical.Plan>,
  compiler: Compiler,
): Membership {
  const needleType = expression.needle.type;
  const tests = expression.list.map((item) => ({
    value: compiler.compile(item),
    equals: compareTyped("EQUAL", needleType, item.type),
  }));
  function* comparisons(row: Tuple, needle: Value): Generator<Truth> {
    for (const test of tests) yield test.equals(needle, test.value(row));
  }
  return (row, needle) => foldTruth("or", comparisons(row, needle));
}

/**
 * A list of constants and parameters, hashed once — or null for any other
 * list. An x of unknown type is converted to the items' affinity, so they
 * must all share one.
 */
function compileFixedList(
  expression: InList<physical.Plan>,
  compiler: Compiler,
): Membership | null {
  const { needle, list } = expression;
  const [first] = list;
  const types = list.map((item) => item.type);
  const isConvertible = needle.type !== "ANY" || shareAffinity(types);
  if (first === undefined || !isConvertible) return null;
  const items = fixedItems(list, needle.type, compiler.scope.run);
  if (items === null) return null;
  const contains = membership(items);
  const convertNeedle = comparisonConversion(needle.type, first.type);
  return (_row, value) => contains(convertNeedle(value));
}

/**
 * The list's values, each converted as `=` with a needle of `needleType`
 * converts it; null if one is not the same for every row.
 */
function fixedItems(
  list: readonly physical.Expression[],
  needleType: StaticType,
  run: Run,
): Value[] | null {
  const items: Value[] = [];
  for (const item of list) {
    const value = fixedValue(item, run);
    if (value === undefined) return null;
    items.push(comparisonConversion(item.type, needleType)(value));
  }
  return items;
}

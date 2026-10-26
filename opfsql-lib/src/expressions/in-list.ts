import type { InList, physical } from "../sql/plan/index.js";
import {
  foldTruth,
  membership,
  type Truth,
  type Value,
} from "../values/index.js";
import { comparison } from "./comparison.js";
import { fixedValue } from "./parameter.js";
import type { CompiledExpression, Compiler, Run, Tuple } from "./scope.js";

// ---------------------------------------------------------------------------
// `x IN (list)`: true if some item equals x, as `=` compares — so the answer
// does not depend on whether the optimizer rewrote a short list into `=`s.
// A list of constants and parameters is hashed once; each row then costs one
// lookup.
// ---------------------------------------------------------------------------

/** A test of the needle's value for a row. */
type Membership = (row: Tuple, needle: Value) => Truth;

const equals = comparison("EQUAL");

export function compileInList(
  expression: InList<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const needle = compiler.compile(expression.needle);
  const isIn =
    compileFixedList(expression.list, compiler.scope.run) ??
    compileItems(expression.list, compiler);
  return (row) => isIn(row, needle(row));
}

/** Each item compared with x by `=`, the answers combined by OR. */
function compileItems(
  list: readonly physical.Expression[],
  compiler: Compiler,
): Membership {
  const items = list.map((item) => compiler.compile(item));
  function* comparisons(row: Tuple, needle: Value): Generator<Truth> {
    for (const item of items) yield equals(needle, item(row));
  }
  return (row, needle) => foldTruth("or", comparisons(row, needle));
}

/** A list of constants and parameters, hashed once — or null for any other. */
function compileFixedList(
  list: readonly physical.Expression[],
  run: Run,
): Membership | null {
  const items: Value[] = [];
  for (const item of list) {
    const value = fixedValue(item, run);
    if (value === undefined) return null;
    items.push(value);
  }
  const contains = membership(items);
  return (_row, needle) => contains(needle);
}

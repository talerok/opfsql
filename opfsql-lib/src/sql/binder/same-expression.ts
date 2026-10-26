import {
  type AggregateCall,
  isSameExpression as isSamePlanExpression,
  type logical,
  isSameColumnAnyLevel,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// Structural equality as the binder needs it: how a select item is matched
// to a GROUP BY expression, an ORDER BY key to an output column, and an
// aggregate call to one met before. It is sql/plan's, but for one rule: a
// column is the same column by its binding alone, so a column met inside a
// subquery, a level further in, still matches the one it reads out there.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

export function isSameExpression(a: Expression, b: Expression): boolean {
  return isSamePlanExpression(a, b, isSameColumnAnyLevel);
}

export function isSameAggregate(
  a: AggregateCall<logical.Plan>,
  b: AggregateCall<logical.Plan>,
): boolean {
  return (
    a.function === b.function &&
    a.isDistinct === b.isDistinct &&
    a.arguments.length === b.arguments.length &&
    a.arguments.every((argument, index) => {
      const other = b.arguments[index];
      return other !== undefined && isSameExpression(argument, other);
    })
  );
}

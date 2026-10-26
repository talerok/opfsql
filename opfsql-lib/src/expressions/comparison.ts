import type { ComparisonOperator } from "../sql/plan/index.js";
import { compareSql, type Truth, type Value } from "../values/index.js";

// ---------------------------------------------------------------------------
// Comparisons: the order from values (compareSql, NULL as unknown), applied
// to the values as they are (docs/semantics.md, Comparisons). The one
// conversion a comparison involves, a parameter's affinity, is applied by
// the parameter itself, so nothing here depends on static types.
// ---------------------------------------------------------------------------

/** `order` (as compareSql returns it) satisfies `operator`. */
function comparisonHolds(
  operator: ComparisonOperator,
): (order: number) => boolean {
  switch (operator) {
    case "EQUAL": {
      return (order) => order === 0;
    }
    case "NOT_EQUAL": {
      return (order) => order !== 0;
    }
    case "LESS": {
      return (order) => order < 0;
    }
    case "LESS_EQUAL": {
      return (order) => order <= 0;
    }
    case "GREATER": {
      return (order) => order > 0;
    }
    case "GREATER_EQUAL": {
      return (order) => order >= 0;
    }
  }
}

/** `left <operator> right`: NULL when either side is NULL. */
export function comparison(
  operator: ComparisonOperator,
): (left: Value, right: Value) => Truth {
  const holds = comparisonHolds(operator);
  return (left, right) => {
    const order = compareSql(left, right);
    return order === null ? null : holds(order);
  };
}

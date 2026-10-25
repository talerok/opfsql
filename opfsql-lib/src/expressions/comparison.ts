import type { StaticType } from "../functions/index.js";
import type { ComparisonOperator } from "../sql/plan/index.js";
import {
  type Affinity,
  applyAffinity,
  compareSql,
  type Truth,
  type Value,
} from "../values/index.js";

// ---------------------------------------------------------------------------
// Comparisons: the order from values (compareSql, NULL as unknown), and
// comparison affinity (docs/semantics.md, Comparisons). An operand whose type
// is known only at run time (ANY: a parameter, a mixed CASE) is converted to
// the type of the side it is compared with — `id = $1` with '5' finds id 5,
// `name = $1` with 5 finds '5'. Typed operands of different kinds are a bind
// error, and JSON values compare as they are.
// ---------------------------------------------------------------------------

/** A conversion of one side's values before comparing them. */
export type Conversion = (value: Value) => Value;

const unchanged: Conversion = (value) => value;

/** The affinity a static type gives the other side of a comparison. */
function affinityOf(type: StaticType): Affinity | null {
  switch (type) {
    case "INTEGER":
    case "REAL": {
      return "numeric";
    }
    case "TEXT": {
      return "text";
    }
    case "BLOB":
    case "BOOLEAN":
    case "JSON":
    case "NULL":
    case "ANY": {
      return null;
    }
  }
}

/**
 * How to convert a side of type `type` compared with a side of type
 * `other` — for comparisons, and for hash keys that must agree with them.
 */
export function comparisonConversion(
  type: StaticType,
  other: StaticType,
): Conversion {
  const affinity = type === "ANY" ? affinityOf(other) : null;
  if (affinity === null) return unchanged;
  return (value) => applyAffinity(value, affinity);
}

/** True if values of these types all convert the same way, compared. */
export function shareAffinity(types: readonly StaticType[]): boolean {
  return new Set(types.map((type) => affinityOf(type))).size <= 1;
}

/** `order` (as compareValues returns it) satisfies `operator`. */
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

/**
 * `left <operator> right` for operands of static types `left` and `right`,
 * comparison affinity included: NULL when either side is NULL.
 */
export function compareTyped(
  operator: ComparisonOperator,
  left: StaticType,
  right: StaticType,
): (left: Value, right: Value) => Truth {
  const holds = comparisonHolds(operator);
  const convertLeft = comparisonConversion(left, right);
  const convertRight = comparisonConversion(right, left);
  return (a, b) => {
    const order = compareSql(convertLeft(a), convertRight(b));
    return order === null ? null : holds(order);
  };
}

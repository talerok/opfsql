import type * as BT from "../types.js";
import type { BoundCaseExpression } from "../types.js";
import { areEqual } from "../../../values/index.js";
import { BoundExpressionClass } from "../types.js";

export function sameExpression(
  a: BT.BoundExpression,
  b: BT.BoundExpression,
): boolean {
  if (a.expressionClass !== b.expressionClass) {
    return false;
  }

  switch (a.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF: {
      const ar = a as BT.BoundColumnRefExpression;
      const br = b as BT.BoundColumnRefExpression;
      return (
        ar.binding.tableIndex === br.binding.tableIndex &&
        ar.binding.columnIndex === br.binding.columnIndex
      );
    }
    case BoundExpressionClass.BOUND_CONSTANT: {
      const ac = a as BT.BoundConstantExpression;
      const bc = b as BT.BoundConstantExpression;
      return areEqual(ac.value, bc.value) && ac.returnType === bc.returnType;
    }
    case BoundExpressionClass.BOUND_COMPARISON: {
      const ac = a as BT.BoundComparisonExpression;
      const bc = b as BT.BoundComparisonExpression;
      return (
        ac.comparisonType === bc.comparisonType &&
        sameExpression(ac.left, bc.left) &&
        sameExpression(ac.right, bc.right)
      );
    }
    case BoundExpressionClass.BOUND_CONJUNCTION:
      return sameTaggedChildren(
        a as BT.BoundConjunctionExpression,
        b as BT.BoundConjunctionExpression,
        "conjunctionType",
      );
    case BoundExpressionClass.BOUND_OPERATOR:
      return sameTaggedChildren(
        a as BT.BoundOperatorExpression,
        b as BT.BoundOperatorExpression,
        "operatorType",
      );
    case BoundExpressionClass.BOUND_FUNCTION:
      return sameTaggedChildren(
        a as BT.BoundFunctionExpression,
        b as BT.BoundFunctionExpression,
        "functionName",
      );
    case BoundExpressionClass.BOUND_AGGREGATE:
      return sameAggregate(
        a as BT.BoundAggregateExpression,
        b as BT.BoundAggregateExpression,
      );
    case BoundExpressionClass.BOUND_BETWEEN: {
      const ab = a as BT.BoundBetweenExpression;
      const bb = b as BT.BoundBetweenExpression;
      return (
        sameExpression(ab.input, bb.input) &&
        sameExpression(ab.lower, bb.lower) &&
        sameExpression(ab.upper, bb.upper)
      );
    }
    case BoundExpressionClass.BOUND_CAST: {
      const ac = a as BT.BoundCastExpression;
      const bc = b as BT.BoundCastExpression;
      return ac.castType === bc.castType && sameExpression(ac.child, bc.child);
    }
    case BoundExpressionClass.BOUND_CASE: {
      return sameCase(a as BoundCaseExpression, b as BoundCaseExpression);
    }
    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      const aj = a as BT.BoundJsonAccessExpression;
      const bj = b as BT.BoundJsonAccessExpression;
      if (!sameExpression(aj.child, bj.child)) return false;
      if (aj.path.length !== bj.path.length) return false;
      return aj.path.every((seg, i) => {
        const other = bj.path[i];
        if (seg.type !== other.type) return false;
        return seg.type === "field"
          ? seg.name === (other as typeof seg).name
          : seg.value === (other as { type: "index"; value: number }).value;
      });
    }
    default:
      return false;
  }
}

/** Compare two expressions that share a tag field + children array pattern. */
function sameTaggedChildren<T extends { children: BT.BoundExpression[] }>(
  a: T,
  b: T,
  tagKey: keyof T,
): boolean {
  if (a[tagKey] !== b[tagKey]) return false;
  if (a.children.length !== b.children.length) return false;
  return a.children.every((c, i) => sameExpression(c, b.children[i]));
}

function sameCase(a: BoundCaseExpression, b: BoundCaseExpression): boolean {
  if (a.caseChecks.length !== b.caseChecks.length) return false;
  const areChecksSame = a.caseChecks.every((check, index) => {
    const other = b.caseChecks[index];
    return (
      sameExpression(check.when, other.when) &&
      sameExpression(check.result, other.result)
    );
  });
  return (
    areChecksSame &&
    sameNullable(a.operand, b.operand) &&
    sameNullable(a.elseExpr, b.elseExpr)
  );
}

/** Both absent, or both present and the same. */
function sameNullable(
  a: BT.BoundExpression | null,
  b: BT.BoundExpression | null,
): boolean {
  if (a === null || b === null) return a === b;
  return sameExpression(a, b);
}

export function sameAggregate(
  a: BT.BoundAggregateExpression,
  b: BT.BoundAggregateExpression,
): boolean {
  return (
    a.functionName === b.functionName &&
    a.isStar === b.isStar &&
    a.distinct === b.distinct &&
    a.children.length === b.children.length &&
    a.children.every((c, i) => sameExpression(c, b.children[i]))
  );
}

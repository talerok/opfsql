import type {
  BoundComparisonExpression,
  BoundExpression,
  BoundSubqueryExpression,
  JoinCondition,
  LogicalComparisonJoin,
  LogicalFilter,
  LogicalOperator,
} from "../binder/types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../binder/types.js";
import {
  flattenConjunction,
  getExpressionTables,
  getOperatorTables,
  makeConjunction,
} from "./utils/index.js";

// ============================================================================
// Decorrelate EXISTS / NOT EXISTS subqueries into SEMI / ANTI joins.
//
// Transforms:
//   FILTER(EXISTS(subplan), outerRel)
// Into:
//   SEMI_JOIN(outerRel, innerRel, correlatedConditions)
//
// This converts O(n×m) correlated execution into O(n+m) hash join.
// ============================================================================

export function decorrelateExists(plan: LogicalOperator): LogicalOperator {
  // Recurse into children first
  for (let i = 0; i < plan.children.length; i++) {
    plan.children[i] = decorrelateExists(plan.children[i]);
  }

  if (plan.type !== LogicalOperatorType.LOGICAL_FILTER) {
    return plan;
  }

  const filter = plan as LogicalFilter;
  const condition = filter.expressions[0];
  if (!condition) return plan;

  const outerTables = getOperatorTables(filter.children[0]);
  const parts = flattenConjunction(condition);

  // Find EXISTS/NOT_EXISTS subquery expressions in the AND-flattened conditions
  const existsParts: Array<{ index: number; expr: BoundSubqueryExpression }> =
    [];
  for (let i = 0; i < parts.length; i++) {
    if (parts[i].expressionClass === BoundExpressionClass.BOUND_SUBQUERY) {
      const sq = parts[i] as BoundSubqueryExpression;
      if (sq.subqueryType === "EXISTS" || sq.subqueryType === "NOT_EXISTS") {
        existsParts.push({ index: i, expr: sq });
      }
    }
  }

  if (existsParts.length === 0) return plan;

  // Process one EXISTS at a time (from last to first to preserve indices)
  let result: LogicalOperator = filter.children[0];
  const remainingParts = [...parts];

  for (let k = existsParts.length - 1; k >= 0; k--) {
    const { index, expr: sq } = existsParts[k];
    const decorrelated = tryDecorrelate(sq, result, outerTables);
    if (decorrelated) {
      result = decorrelated;
      remainingParts.splice(index, 1);
    }
  }

  // If there are remaining non-EXISTS conditions, wrap in a filter
  if (remainingParts.length > 0) {
    const remaining = makeConjunction(remainingParts);
    if (remaining) {
      result = {
        type: LogicalOperatorType.LOGICAL_FILTER,
        children: [result],
        expressions: [remaining],
        types: result.types,
        estimatedCardinality: 0,
        columnBindings: result.columnBindings,
      } satisfies LogicalFilter;
    }
  }

  return result;
}

// ============================================================================
// Try to decorrelate a single EXISTS/NOT EXISTS subquery
// ============================================================================

function tryDecorrelate(
  sq: BoundSubqueryExpression,
  outer: LogicalOperator,
  outerTables: Set<number>,
): LogicalComparisonJoin | null {
  // Walk the subplan to find the filter with correlated predicates.
  // The subplan is typically: PROJECTION → FILTER → GET (or similar).
  const innerFilter = findFilter(sq.subplan);
  if (!innerFilter) return null;

  const innerTables = getOperatorTables(sq.subplan);
  const predicates = flattenConjunction(innerFilter.expressions[0]);

  // Split predicates into correlated (refs outer+inner) vs uncorrelated
  const correlated: BoundComparisonExpression[] = [];
  const uncorrelated: BoundExpression[] = [];

  for (const pred of predicates) {
    const tables = getExpressionTables(pred);
    const touchesOuter = setOverlaps(tables, outerTables);
    const touchesInner = setOverlaps(tables, innerTables);

    if (touchesOuter && touchesInner) {
      // Must be an equality comparison for hash join
      if (pred.expressionClass !== BoundExpressionClass.BOUND_COMPARISON)
        return null;
      const cmp = pred as BoundComparisonExpression;
      if (cmp.comparisonType !== "EQUAL") return null;
      correlated.push(cmp);
    } else {
      uncorrelated.push(pred);
    }
  }

  // Need at least one correlated equality predicate for the join condition
  if (correlated.length === 0) return null;

  // Build join conditions: left = outer ref, right = inner ref
  const conditions: JoinCondition[] = [];
  for (const cmp of correlated) {
    const leftTables = getExpressionTables(cmp.left);
    const rightTables = getExpressionTables(cmp.right);
    const leftIsOuter = setOverlaps(leftTables, outerTables);
    const rightIsOuter = setOverlaps(rightTables, outerTables);

    if (leftIsOuter && !rightIsOuter) {
      conditions.push({
        left: cmp.left,
        right: cmp.right,
        comparisonType: "EQUAL",
      });
    } else if (rightIsOuter && !leftIsOuter) {
      conditions.push({
        left: cmp.right,
        right: cmp.left,
        comparisonType: "EQUAL",
      });
    } else {
      // Both sides reference same scope — bail out
      return null;
    }
  }

  // Build the inner relation: strip correlated predicates from the subplan filter,
  // keep uncorrelated predicates, and extract the scan underneath.
  let inner: LogicalOperator = innerFilter.children[0];
  if (uncorrelated.length > 0) {
    const uncorrCond = makeConjunction(uncorrelated)!;
    inner = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [inner],
      expressions: [uncorrCond],
      types: inner.types,
      estimatedCardinality: 0,
      columnBindings: inner.columnBindings,
    } satisfies LogicalFilter;
  }

  // Build SEMI or ANTI join
  const joinType = sq.subqueryType === "EXISTS" ? "SEMI" : "ANTI";
  const join: LogicalComparisonJoin = {
    type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    joinType,
    children: [outer, inner],
    conditions,
    expressions: [],
    // SEMI/ANTI only output left (outer) side columns
    types: outer.types,
    estimatedCardinality: 0,
    columnBindings: outer.columnBindings,
  };

  return join;
}

// ============================================================================
// Helpers
// ============================================================================

/** Find the first LOGICAL_FILTER in a plan (depth-first, skipping projections). */
function findFilter(plan: LogicalOperator): LogicalFilter | null {
  if (plan.type === LogicalOperatorType.LOGICAL_FILTER) {
    return plan as LogicalFilter;
  }
  // Walk through projections (subplan is typically PROJ → FILTER → GET)
  if (plan.type === LogicalOperatorType.LOGICAL_PROJECTION) {
    return findFilter(plan.children[0]);
  }
  return null;
}

function setOverlaps(a: Set<number>, b: Set<number>): boolean {
  for (const item of a) {
    if (b.has(item)) return true;
  }
  return false;
}

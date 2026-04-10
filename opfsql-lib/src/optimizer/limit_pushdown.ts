import type {
  LogicalOperator,
  LogicalLimit,
  LogicalOrderBy,
  LogicalProjection,
} from '../binder/types.js';
import { LogicalOperatorType } from '../binder/types.js';

// ============================================================================
// Limit Pushdown — pushes LIMIT below PROJECTION for early termination,
// and annotates ORDER BY with topN for heap-based top-K sorting.
//
// Based on DuckDB's limit_pushdown.cpp:
// Pattern 1: LIMIT → PROJECTION → X  →  PROJECTION → LIMIT → X
// Pattern 2: LIMIT → ORDER BY  →  set ORDER BY.topN = limit + offset
// Pattern 3: LIMIT → PROJECTION → ORDER BY  →  same topN annotation
// Only applied when limit < 8192 (small result sets benefit most).
// ============================================================================

const MAX_PUSHDOWN_LIMIT = 8192;

export function pushdownLimit(plan: LogicalOperator): LogicalOperator {
  // Recurse into children first
  for (let i = 0; i < plan.children.length; i++) {
    plan.children[i] = pushdownLimit(plan.children[i]);
  }

  if (plan.type !== LogicalOperatorType.LOGICAL_LIMIT) return plan;

  const limit = plan as LogicalLimit;
  if (limit.limitVal === null || limit.limitVal >= MAX_PUSHDOWN_LIMIT) return plan;

  const topN = limit.limitVal + limit.offsetVal;
  const child = limit.children[0];

  // Pattern: LIMIT → ORDER BY — annotate with topN
  if (child.type === LogicalOperatorType.LOGICAL_ORDER_BY) {
    (child as LogicalOrderBy).topN = topN;
    return plan;
  }

  // Pattern: LIMIT → PROJECTION
  if (child.type === LogicalOperatorType.LOGICAL_PROJECTION) {
    const projection = child as LogicalProjection;
    const grandchild = projection.children[0];

    // Pattern: LIMIT → PROJECTION → ORDER BY — annotate with topN
    if (grandchild.type === LogicalOperatorType.LOGICAL_ORDER_BY) {
      (grandchild as LogicalOrderBy).topN = topN;
    }

    // Create new limit below projection
    const newLimit: LogicalLimit = {
      type: LogicalOperatorType.LOGICAL_LIMIT,
      children: [projection.children[0]],
      limitVal: limit.limitVal,
      offsetVal: limit.offsetVal,
      expressions: [],
      types: projection.children[0].types,
      estimatedCardinality: limit.limitVal + limit.offsetVal,
      getColumnBindings: () => newLimit.children[0].getColumnBindings(),
    };

    // Put projection on top
    projection.children = [newLimit] as [LogicalOperator];

    // Keep original limit on top (now: LIMIT → PROJECTION → LIMIT → X)
    // The outer LIMIT is still needed for correctness with offset
    limit.children = [projection] as [LogicalOperator];
    return limit;
  }

  return plan;
}

import type {
  LogicalOperator,
  LogicalLimit,
  LogicalProjection,
} from '../binder/types.js';
import { LogicalOperatorType } from '../binder/types.js';

// ============================================================================
// Limit Pushdown — pushes LIMIT below PROJECTION for early termination
//
// Based on DuckDB's limit_pushdown.cpp:
// Pattern: LIMIT → PROJECTION → X  →  PROJECTION → LIMIT → X
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

  const child = limit.children[0];

  // Pattern: LIMIT → PROJECTION
  if (child.type === LogicalOperatorType.LOGICAL_PROJECTION) {
    const projection = child as LogicalProjection;

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

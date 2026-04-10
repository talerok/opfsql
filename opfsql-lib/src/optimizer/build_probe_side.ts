import type {
  LogicalOperator,
  LogicalComparisonJoin,
} from '../binder/types.js';
import { LogicalOperatorType } from '../binder/types.js';
import { flipComparison } from './utils/index.js';

// ============================================================================
// Build/Probe Side Optimizer — selects which side of a hash join builds
// the hash table vs probes it
//
// Based on DuckDB's build_probe_side_optimizer.cpp:
// The build side (hash table) should be the smaller relation.
// If the right side has smaller cardinality, swap children and flip conditions.
// ============================================================================

function estimateRowSize(types: readonly string[]): number {
  let size = 0;
  for (const t of types) {
    switch (t) {
      case 'TEXT':
      case 'BLOB':
        size += 32; // variable-length, estimate average
        break;
      case 'REAL':
        size += 8;
        break;
      default:
        size += 4; // INTEGER, BOOLEAN, etc.
    }
  }
  return Math.max(size, 1);
}

function estimateBuildCost(op: LogicalOperator): number {
  return op.estimatedCardinality * estimateRowSize(op.types);
}

export function optimizeBuildProbeSide(plan: LogicalOperator): LogicalOperator {
  // Recurse into children first
  for (let i = 0; i < plan.children.length; i++) {
    plan.children[i] = optimizeBuildProbeSide(plan.children[i]);
  }

  if (plan.type !== LogicalOperatorType.LOGICAL_COMPARISON_JOIN) {
    return plan;
  }

  const join = plan as LogicalComparisonJoin;

  // LEFT JOIN: can't swap sides (left must remain the probe side)
  if (join.joinType === 'LEFT') return plan;

  // Build side cost = cardinality * estimated row size (from types)
  const leftCost = estimateBuildCost(join.children[0]);
  const rightCost = estimateBuildCost(join.children[1]);

  // Convention: right side is build side (hash table).
  // If left is cheaper to build, swap so left becomes right (build side).
  if (leftCost < rightCost) {
    // Swap children
    const temp = join.children[0];
    join.children[0] = join.children[1];
    join.children[1] = temp;

    // Flip all join conditions
    for (const cond of join.conditions) {
      const tempExpr = cond.left;
      cond.left = cond.right;
      cond.right = tempExpr;
      cond.comparisonType = flipComparison(cond.comparisonType);
    }

    // Update types and bindings
    const leftTypes = join.children[0].types;
    const rightTypes = join.children[1].types;
    join.types = [...leftTypes, ...rightTypes];
    join.getColumnBindings = () => {
      return [
        ...join.children[0].getColumnBindings(),
        ...join.children[1].getColumnBindings(),
      ];
    };
  }

  return plan;
}

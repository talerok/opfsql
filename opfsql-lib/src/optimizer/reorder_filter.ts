import type {
  LogicalOperator,
  LogicalFilter,
  BoundExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundOperatorExpression,
  BoundFunctionExpression,
  BoundCastExpression,
  BoundColumnRefExpression,
} from '../binder/types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../binder/types.js';

// ============================================================================
// Reorder Filter — sorts filter conditions by estimated evaluation cost
//
// Based on DuckDB's expression_heuristics.cpp:
// Cheap conditions are evaluated first so expensive ones can be short-circuited.
// ============================================================================

export function reorderFilters(plan: LogicalOperator): LogicalOperator {
  // Recurse into children first
  for (let i = 0; i < plan.children.length; i++) {
    plan.children[i] = reorderFilters(plan.children[i]);
  }

  if (plan.type !== LogicalOperatorType.LOGICAL_FILTER) return plan;

  const filter = plan as LogicalFilter;
  if (filter.expressions.length <= 1) return plan;

  // If any expression can throw (e.g. division), don't reorder —
  // reordering could evaluate a throwing expression before a guard condition.
  if (filter.expressions.some(canThrow)) return plan;

  // Sort expressions by cost (ascending — cheap first)
  filter.expressions.sort((a, b) => estimateCost(a) - estimateCost(b));

  return plan;
}

// ============================================================================
// Cost estimation for expressions
// ============================================================================

function estimateCost(expr: BoundExpression): number {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_CONSTANT:
      return 1;

    case BoundExpressionClass.BOUND_COLUMN_REF: {
      const ref = expr as BoundColumnRefExpression;
      return 8 * typeMultiplier(ref.returnType);
    }

    case BoundExpressionClass.BOUND_COMPARISON: {
      const cmp = expr as BoundComparisonExpression;
      return 5 + estimateCost(cmp.left) + estimateCost(cmp.right);
    }

    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const conj = expr as BoundConjunctionExpression;
      let cost = 5;
      for (const child of conj.children) {
        cost += estimateCost(child);
      }
      return cost;
    }

    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BoundOperatorExpression;
      let baseCost: number;
      switch (op.operatorType) {
        case 'ADD':
        case 'SUBTRACT':
          baseCost = 5;
          break;
        case 'MULTIPLY':
          baseCost = 10;
          break;
        case 'DIVIDE':
        case 'MOD':
          baseCost = 15;
          break;
        case 'NOT':
        case 'IS_NULL':
        case 'IS_NOT_NULL':
          baseCost = 3;
          break;
        case 'IN':
        case 'NOT_IN':
          baseCost = (op.children.length - 1) * 100;
          break;
        case 'NEGATE':
          baseCost = 3;
          break;
        default:
          baseCost = 10;
      }
      for (const child of op.children) {
        baseCost += estimateCost(child);
      }
      return baseCost;
    }

    case BoundExpressionClass.BOUND_FUNCTION: {
      const func = expr as BoundFunctionExpression;
      let baseCost = functionCost(func.functionName);
      for (const child of func.children) {
        baseCost += estimateCost(child);
      }
      return baseCost;
    }

    case BoundExpressionClass.BOUND_CAST: {
      const cast = expr as BoundCastExpression;
      const castCost = cast.castType === 'TEXT' || cast.castType === 'BLOB' ? 200 : 5;
      return castCost + estimateCost(cast.child);
    }

    case BoundExpressionClass.BOUND_SUBQUERY:
      return 10000; // Subqueries are very expensive

    case BoundExpressionClass.BOUND_CASE:
      return 50;

    case BoundExpressionClass.BOUND_BETWEEN:
      return 15;

    case BoundExpressionClass.BOUND_AGGREGATE:
      return 100;

    default:
      return 10;
  }
}

function typeMultiplier(type: string): number {
  switch (type) {
    case 'TEXT':
    case 'BLOB':
      return 5;
    case 'REAL':
      return 2;
    default:
      return 1;
  }
}

// ============================================================================
// CanThrow — checks if an expression could throw at runtime (e.g. division by zero)
// ============================================================================

function canThrow(expr: BoundExpression): boolean {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BoundOperatorExpression;
      if (op.operatorType === 'DIVIDE' || op.operatorType === 'MOD') return true;
      return op.children.some(canThrow);
    }
    case BoundExpressionClass.BOUND_COMPARISON: {
      const cmp = expr as BoundComparisonExpression;
      return canThrow(cmp.left) || canThrow(cmp.right);
    }
    case BoundExpressionClass.BOUND_CONJUNCTION:
      return (expr as BoundConjunctionExpression).children.some(canThrow);
    case BoundExpressionClass.BOUND_FUNCTION:
      return (expr as BoundFunctionExpression).children.some(canThrow);
    case BoundExpressionClass.BOUND_CAST:
      return canThrow((expr as BoundCastExpression).child);
    default:
      return false;
  }
}

function functionCost(name: string): number {
  const upper = name.toUpperCase();
  switch (upper) {
    case 'ABS':
    case 'SIGN':
      return 5;
    case 'UPPER':
    case 'LOWER':
    case 'TRIM':
    case 'LTRIM':
    case 'RTRIM':
      return 200;
    case 'LENGTH':
    case 'TYPEOF':
      return 10;
    case 'SUBSTR':
    case 'SUBSTRING':
    case 'REPLACE':
    case 'INSTR':
      return 200;
    case 'COALESCE':
    case 'IFNULL':
    case 'NULLIF':
      return 5;
    case 'HEX':
    case 'UNHEX':
    case 'QUOTE':
      return 200;
    default:
      return 100;
  }
}

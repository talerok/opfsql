import type {
  BoundColumnRefExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundJsonAccessExpression,
  BoundParameterExpression,
  ComparisonType,
  TableFilter,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import type { Resolver } from '../resolve.js';
import type { Tuple, Value } from '../types.js';
import { evaluateExpression } from './index.js';
import type { SyncEvalContext } from './context.js';
import { traverseJsonPath } from './json-access.js';

// ---------------------------------------------------------------------------
// CompiledFilter — pre-compiled for scan-time hot path.
// Expression tree is walked once at operator creation; at runtime only
// closures execute — no tree walking, no switch dispatch per row.
// ---------------------------------------------------------------------------

export interface CompiledFilter {
  getValue: (tuple: Tuple) => Value;
  comparisonType: ComparisonType;
  getConstant: (params?: readonly Value[]) => Value;
}

// ---------------------------------------------------------------------------
// compileExpression — walks the expression tree once, returns a closure
// that captures resolved tuple positions.
// ---------------------------------------------------------------------------

export function compileExpression(
  expr: BoundExpression,
  resolver: Resolver,
  ctx: SyncEvalContext,
): (tuple: Tuple) => Value {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF: {
      const pos = resolver((expr as BoundColumnRefExpression).binding);
      if (pos !== undefined) {
        return (tuple) => tuple[pos] ?? null;
      }
      break;
    }

    case BoundExpressionClass.BOUND_CONSTANT:
      return () => (expr as BoundConstantExpression).value;

    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      const ja = expr as BoundJsonAccessExpression;
      const childPos = resolver(ja.child.binding);
      if (childPos !== undefined) {
        const path = ja.path;
        return (tuple) => traverseJsonPath(tuple[childPos], path);
      }
      break;
    }
  }

  // Fallback: use the general evaluator
  return (tuple) => evaluateExpression(expr, tuple, resolver, ctx);
}

// ---------------------------------------------------------------------------
// compileFilter — pre-compiles a TableFilter into a CompiledFilter
// ---------------------------------------------------------------------------

export function compileFilter(
  filter: TableFilter,
  resolver: Resolver,
  ctx: SyncEvalContext,
): CompiledFilter {
  const getValue = compileExpression(filter.expression, resolver, ctx);
  const { comparisonType } = filter;

  let getConstant: (params?: readonly Value[]) => Value;
  if (filter.constant.expressionClass === BoundExpressionClass.BOUND_CONSTANT) {
    const val = (filter.constant as BoundConstantExpression).value;
    getConstant = () => val;
  } else {
    const paramIndex = (filter.constant as BoundParameterExpression).index;
    getConstant = (params) => params?.[paramIndex] ?? null;
  }

  return { getValue, comparisonType, getConstant };
}


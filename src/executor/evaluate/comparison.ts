import type {
  BoundExpression,
  BoundComparisonExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { evaluateExpression } from './index.js';
import { applyComparison } from './helpers.js';

export async function evalComparison(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const cmp = expr as BoundComparisonExpression;
  const left = await evaluateExpression(cmp.left, tuple, resolver, ctx);
  const right = await evaluateExpression(cmp.right, tuple, resolver, ctx);
  return applyComparison(left, right, cmp.comparisonType);
}

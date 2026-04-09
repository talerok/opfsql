import type {
  BoundExpression,
  BoundBetweenExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { evaluateExpression } from './index.js';
import { compareValues } from './helpers.js';

export async function evalBetween(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const bt = expr as BoundBetweenExpression;
  const input = await evaluateExpression(bt.input, tuple, resolver, ctx);
  const lower = await evaluateExpression(bt.lower, tuple, resolver, ctx);
  const upper = await evaluateExpression(bt.upper, tuple, resolver, ctx);

  if (input === null || lower === null || upper === null) return null;
  return compareValues(input, lower) >= 0 && compareValues(input, upper) <= 0;
}

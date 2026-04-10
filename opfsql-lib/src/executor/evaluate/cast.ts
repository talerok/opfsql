import type {
  BoundExpression,
  BoundCastExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { evaluateExpression } from './index.js';
import { castValue } from './helpers.js';

export async function evalCast(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const cast = expr as BoundCastExpression;
  const val = await evaluateExpression(cast.child, tuple, resolver, ctx);
  return castValue(val, cast.castType);
}

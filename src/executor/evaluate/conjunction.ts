import type {
  BoundExpression,
  BoundConjunctionExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { evaluateExpression } from './index.js';

export async function evalConjunction(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const conj = expr as BoundConjunctionExpression;

  if (conj.conjunctionType === 'AND') {
    let hasNull = false;
    for (const child of conj.children) {
      const val = await evaluateExpression(child, tuple, resolver, ctx);
      if (val === false) return false;
      if (val === null) hasNull = true;
    }
    return hasNull ? null : true;
  }

  // OR
  let hasNull = false;
  for (const child of conj.children) {
    const val = await evaluateExpression(child, tuple, resolver, ctx);
    if (val === true) return true;
    if (val === null) hasNull = true;
  }
  return hasNull ? null : false;
}

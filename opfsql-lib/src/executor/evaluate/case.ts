import type { BoundExpression, BoundCaseExpression } from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { SyncEvalContext } from './context.js';
import { evaluateExpression } from './index.js';

export function evalCase(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const cs = expr as BoundCaseExpression;

  for (const check of cs.caseChecks) {
    const when = evaluateExpression(check.when, tuple, resolver, ctx);
    if (when === true) {
      return evaluateExpression(check.then, tuple, resolver, ctx);
    }
  }

  if (cs.elseExpr) {
    return evaluateExpression(cs.elseExpr, tuple, resolver, ctx);
  }
  return null;
}

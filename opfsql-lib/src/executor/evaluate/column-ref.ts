import type {
  BoundExpression,
  BoundColumnRefExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { ExecutorError } from '../errors.js';

export function evalColumnRef(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Value {
  const ref = expr as BoundColumnRefExpression;
  const pos = resolver(ref.binding);
  if (pos !== undefined) return tuple[pos] ?? null;

  // Correlated subquery: fall back to outer context
  if (ctx.outerTuple && ctx.outerResolver) {
    const outerPos = ctx.outerResolver(ref.binding);
    if (outerPos !== undefined) return ctx.outerTuple[outerPos] ?? null;
  }

  throw new ExecutorError(
    `Column binding {tableIndex:${ref.binding.tableIndex}, columnIndex:${ref.binding.columnIndex}} not found in layout`,
  );
}

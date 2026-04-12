import type { LogicalOperator } from '../../binder/types.js';
import type { Tuple, Value } from '../types.js';
import type { Resolver } from '../resolve.js';

/**
 * Context passed to expression evaluator for subquery execution.
 * Breaks the circular dependency: evaluate → executor → evaluate
 * by using an interface instead of direct import.
 */
export interface EvalContext {
  /** Execute a logical plan and return result tuples.
   *  Pass limit to stop early (e.g. limit=1 for EXISTS). */
  executeSubplan(
    plan: LogicalOperator,
    outerTuple?: Tuple,
    outerResolver?: Resolver,
    limit?: number,
  ): Promise<Tuple[]>;

  /** Outer tuple for correlated subquery resolution */
  outerTuple?: Tuple;
  /** Outer resolver for correlated subquery resolution */
  outerResolver?: Resolver;
  /** Bound parameter values for $1, $2, ... placeholders (0-based). */
  params?: readonly Value[];
}

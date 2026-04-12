import type { LogicalOperator } from '../../binder/types.js';
import type { Tuple, Value } from '../types.js';
import type { Resolver } from '../resolve.js';

export interface SyncEvalContext {
  executeSubplan(
    plan: LogicalOperator,
    outerTuple?: Tuple,
    outerResolver?: Resolver,
    limit?: number,
  ): Tuple[];

  outerTuple?: Tuple;
  outerResolver?: Resolver;
  params?: readonly Value[];
}

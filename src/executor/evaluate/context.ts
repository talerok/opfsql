import type { LogicalOperator } from '../../binder/types.js';
import type { Tuple } from '../types.js';

/**
 * Context passed to expression evaluator for subquery execution.
 * Breaks the circular dependency: evaluate → executor → evaluate
 * by using an interface instead of direct import.
 */
export interface EvalContext {
  /** Execute a logical plan and return all result tuples */
  executeSubplan(plan: LogicalOperator): Promise<Tuple[]>;
}

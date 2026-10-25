import type { LogicalOperator } from "../../binder/types.js";
import type {
  SyncIIndexManager,
  SyncIRowManager,
} from "../../../store/types.js";
import type { Value } from "../../../values/index.js";
import type { ExecutionContext, OuterRow } from "../evaluate/index.js";
import { drainOperator, rowsOf, takeBatch } from "../operators/shared.js";
import { createPhysicalPlan } from "../planner/index.js";
import type { CTECacheEntry, ExecuteResult, Tuple } from "../types.js";

/**
 * The context a statement's expressions run in: its parameters, and how
 * its subqueries run — each for one outer row, on a fresh physical plan.
 */
export function createExecutionContext(
  rowManager: SyncIRowManager,
  parameters: readonly Value[],
  outer: OuterRow | null = null,
): ExecutionContext {
  return {
    parameters,
    outer,
    runSubquery: (plan, outerRow, limit) =>
      runSubplan(
        plan,
        rowManager,
        createExecutionContext(rowManager, parameters, outerRow),
        limit,
      ),
  };
}

export function executeSelect(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  context: ExecutionContext,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const cteCache = new Map<number, CTECacheEntry>();
  const physical = createPhysicalPlan(
    plan,
    rowManager,
    cteCache,
    context,
    indexManager,
  );
  const rows = drainOperator(physical);
  return { rows, rowsAffected: 0, catalogChanges: [] };
}

/** Runs a subquery's plan, stopping after `limit` rows. */
function runSubplan(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  context: ExecutionContext,
  limit?: number,
): Tuple[] {
  const physical = createPhysicalPlan(plan, rowManager, new Map(), context);
  if (limit === undefined) return drainOperator(physical);
  return takeBatch(rowsOf(physical), limit) ?? [];
}

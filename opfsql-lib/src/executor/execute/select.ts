import type { LogicalOperator } from "../../sql/binder/types.js";
import type {
  ICatalog,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { drainOperator } from "../operators/utils.js";
import { createPhysicalPlan } from "../planner/index.js";
import type { Resolver } from "../resolve.js";
import type { CTECacheEntry, ExecuteResult, Tuple, Value } from "../types.js";

export function createEvalContext(
  rowManager: SyncIRowManager,
  catalog: ICatalog,
  params?: readonly Value[],
  outerTuple?: Tuple,
  outerResolver?: Resolver,
): SyncEvalContext {
  return {
    executeSubplan: (sub, ot, or_, lim) =>
      executeSubplan(sub, rowManager, catalog, ot, or_, lim, params),
    outerTuple,
    outerResolver,
    params,
  };
}

export function executeSelect(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const cteCache = new Map<number, CTECacheEntry>();
  const physical = createPhysicalPlan(
    plan,
    rowManager,
    cteCache,
    ctx,
    indexManager,
  );
  const rows = drainOperator(physical);
  return { rows, rowsAffected: 0, catalogChanges: [] };
}

export function executeSubplan(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  catalog: ICatalog,
  outerTuple?: Tuple,
  outerResolver?: Resolver,
  limit?: number,
  params?: readonly Value[],
): Tuple[] {
  const ctx = createEvalContext(
    rowManager,
    catalog,
    params,
    outerTuple,
    outerResolver,
  );
  const cteCache = new Map<number, CTECacheEntry>();
  const physical = createPhysicalPlan(plan, rowManager, cteCache, ctx);

  if (limit === undefined) {
    return drainOperator(physical);
  }

  const result: Tuple[] = [];
  while (result.length < limit) {
    const batch = physical.next();
    if (!batch) break;
    for (const tuple of batch) {
      result.push(tuple);
      if (result.length >= limit) break;
    }
  }
  return result;
}

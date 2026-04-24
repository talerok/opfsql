import type { LogicalOperator } from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import type { SyncIIndexManager, SyncIRowManager } from "../../store/types.js";
import { ExecutorError } from "../errors.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import type { CTECacheEntry, SyncPhysicalOperator } from "../types.js";
import { planAggregate } from "./plan-aggregate.js";
import { planCTERef, planMaterializedCTE, planRecursiveCTE } from "./plan-cte.js";
import { planFilter } from "./plan-filter.js";
import { planGet } from "./plan-get.js";
import { planComparisonJoin, planCrossProduct } from "./plan-join.js";
import { planLimit } from "./plan-limit.js";
import { planOrderBy } from "./plan-order-by.js";
import { planProjection } from "./plan-projection.js";
import { planDistinct, planUnion } from "./plan-set.js";
import type { PlanHandler, PlannerContext } from "./types.js";

const handlers = new Map<string, PlanHandler>([
  [LogicalOperatorType.LOGICAL_GET, planGet],
  [LogicalOperatorType.LOGICAL_FILTER, planFilter],
  [LogicalOperatorType.LOGICAL_PROJECTION, planProjection],
  [LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY, planAggregate],
  [LogicalOperatorType.LOGICAL_COMPARISON_JOIN, planComparisonJoin],
  [LogicalOperatorType.LOGICAL_CROSS_PRODUCT, planCrossProduct],
  [LogicalOperatorType.LOGICAL_ORDER_BY, planOrderBy],
  [LogicalOperatorType.LOGICAL_LIMIT, planLimit],
  [LogicalOperatorType.LOGICAL_DISTINCT, planDistinct],
  [LogicalOperatorType.LOGICAL_UNION, planUnion],
  [LogicalOperatorType.LOGICAL_MATERIALIZED_CTE, planMaterializedCTE],
  [LogicalOperatorType.LOGICAL_RECURSIVE_CTE, planRecursiveCTE],
  [LogicalOperatorType.LOGICAL_CTE_REF, planCTERef],
]);

export function createPhysicalPlan(
  node: LogicalOperator,
  rowManager: SyncIRowManager,
  cteCache: Map<number, CTECacheEntry>,
  ctx: SyncEvalContext,
  indexManager?: SyncIIndexManager,
): SyncPhysicalOperator {
  const pc: PlannerContext = {
    rowManager,
    cteCache,
    ctx,
    indexManager,
    plan: (child) => createPhysicalPlan(child, rowManager, cteCache, ctx, indexManager),
  };

  const handler = handlers.get(node.type);
  if (!handler) {
    throw new ExecutorError(
      `Cannot create physical plan for ${node.type} — use executor directly for DML/DDL`,
    );
  }
  return handler(node, pc);
}

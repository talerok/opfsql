import type {
  LogicalOperator,
  LogicalGet,
  LogicalFilter,
  LogicalProjection,
  LogicalAggregate,
  LogicalComparisonJoin,
  LogicalOrderBy,
  LogicalLimit,
  LogicalUnion,
  LogicalCTERef,
  LogicalMaterializedCTE,
} from '../binder/types.js';
import { LogicalOperatorType } from '../binder/types.js';
import type { IRowManager } from '../store/types.js';
import type { IIndexManager } from '../store/index-manager.js';
import type { PhysicalOperator, CTECacheEntry } from './types.js';
import type { EvalContext } from './evaluate/context.js';
import { PhysicalScan } from './operators/scan.js';
import { PhysicalIndexScan } from './operators/index-scan.js';
import { PhysicalFilter } from './operators/filter.js';
import { PhysicalProjection } from './operators/projection.js';
import { PhysicalHashJoin, PhysicalNestedLoopJoin } from './operators/join.js';
import { PhysicalHashAggregate } from './operators/aggregate.js';
import { PhysicalSort } from './operators/sort.js';
import { PhysicalLimit } from './operators/limit.js';
import { PhysicalDistinct, PhysicalUnion } from './operators/set.js';
import { PhysicalMaterialize, PhysicalCTEScan } from './operators/cte.js';
import { ExecutorError } from './errors.js';

export function createPhysicalPlan(
  node: LogicalOperator,
  rowManager: IRowManager,
  cteCache: Map<number, CTECacheEntry>,
  ctx: EvalContext,
  indexManager?: IIndexManager,
): PhysicalOperator {
  const plan = (child: LogicalOperator) =>
    createPhysicalPlan(child, rowManager, cteCache, ctx, indexManager);

  switch (node.type) {
    case LogicalOperatorType.LOGICAL_GET: {
      const get = node as LogicalGet;
      if (get.indexHint && indexManager) {
        return new PhysicalIndexScan(
          get, rowManager, indexManager,
          get.indexHint.indexDef, get.indexHint.predicates, get.indexHint.residualFilters,
        );
      }
      const childOp = get.children.length > 0 ? plan(get.children[0]) : undefined;
      return new PhysicalScan(get, rowManager, ctx, childOp);
    }

    case LogicalOperatorType.LOGICAL_FILTER: {
      const filter = node as LogicalFilter;
      return new PhysicalFilter(plan(filter.children[0]), filter.expressions[0], ctx);
    }

    case LogicalOperatorType.LOGICAL_PROJECTION: {
      const proj = node as LogicalProjection;
      return new PhysicalProjection(plan(proj.children[0]), proj, ctx);
    }

    case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY: {
      const agg = node as LogicalAggregate;
      return new PhysicalHashAggregate(plan(agg.children[0]), agg, ctx);
    }

    case LogicalOperatorType.LOGICAL_COMPARISON_JOIN: {
      const join = node as LogicalComparisonJoin;
      return new PhysicalHashJoin(plan(join.children[0]), plan(join.children[1]), join, ctx);
    }

    case LogicalOperatorType.LOGICAL_CROSS_PRODUCT:
      return new PhysicalNestedLoopJoin(plan(node.children[0]), plan(node.children[1]));

    case LogicalOperatorType.LOGICAL_ORDER_BY: {
      const order = node as LogicalOrderBy;
      return new PhysicalSort(plan(order.children[0]), order, ctx);
    }

    case LogicalOperatorType.LOGICAL_LIMIT: {
      const limit = node as LogicalLimit;
      return new PhysicalLimit(plan(limit.children[0]), limit);
    }

    case LogicalOperatorType.LOGICAL_DISTINCT:
      return new PhysicalDistinct(plan(node.children[0]));

    case LogicalOperatorType.LOGICAL_UNION: {
      const union = node as LogicalUnion;
      return new PhysicalUnion(plan(union.children[0]), plan(union.children[1]), union.all);
    }

    case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE: {
      const cte = node as LogicalMaterializedCTE;
      return new PhysicalMaterialize(plan(cte.children[0]), plan(cte.children[1]), cte.cteIndex, cteCache);
    }

    case LogicalOperatorType.LOGICAL_CTE_REF:
      return new PhysicalCTEScan(node as LogicalCTERef, cteCache);

    default:
      throw new ExecutorError(
        `Cannot create physical plan for ${node.type} — use executor directly for DML/DDL`,
      );
  }
}

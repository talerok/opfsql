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
import type { PhysicalOperator, CTECacheEntry } from './types.js';
import type { EvalContext } from './evaluate/context.js';
import { PhysicalScan } from './operators/scan.js';
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
): PhysicalOperator {
  switch (node.type) {
    case LogicalOperatorType.LOGICAL_GET: {
      const get = node as LogicalGet;
      const childOp =
        get.children.length > 0
          ? createPhysicalPlan(get.children[0], rowManager, cteCache, ctx)
          : undefined;
      return new PhysicalScan(get, rowManager, ctx, childOp);
    }

    case LogicalOperatorType.LOGICAL_FILTER: {
      const filter = node as LogicalFilter;
      const child = createPhysicalPlan(
        filter.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalFilter(child, filter.expressions[0], ctx);
    }

    case LogicalOperatorType.LOGICAL_PROJECTION: {
      const proj = node as LogicalProjection;
      const child = createPhysicalPlan(
        proj.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalProjection(child, proj, ctx);
    }

    case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY: {
      const agg = node as LogicalAggregate;
      const child = createPhysicalPlan(
        agg.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalHashAggregate(child, agg, ctx);
    }

    case LogicalOperatorType.LOGICAL_COMPARISON_JOIN: {
      const join = node as LogicalComparisonJoin;
      const probe = createPhysicalPlan(
        join.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      const build = createPhysicalPlan(
        join.children[1],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalHashJoin(probe, build, join, ctx);
    }

    case LogicalOperatorType.LOGICAL_CROSS_PRODUCT: {
      const left = createPhysicalPlan(
        node.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      const right = createPhysicalPlan(
        node.children[1],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalNestedLoopJoin(left, right);
    }

    case LogicalOperatorType.LOGICAL_ORDER_BY: {
      const order = node as LogicalOrderBy;
      const child = createPhysicalPlan(
        order.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalSort(child, order, ctx);
    }

    case LogicalOperatorType.LOGICAL_LIMIT: {
      const limit = node as LogicalLimit;
      const child = createPhysicalPlan(
        limit.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalLimit(child, limit);
    }

    case LogicalOperatorType.LOGICAL_DISTINCT: {
      const child = createPhysicalPlan(
        node.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalDistinct(child);
    }

    case LogicalOperatorType.LOGICAL_UNION: {
      const union = node as LogicalUnion;
      const left = createPhysicalPlan(
        union.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      const right = createPhysicalPlan(
        union.children[1],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalUnion(left, right, union.all);
    }

    case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE: {
      const cte = node as LogicalMaterializedCTE;
      const def = createPhysicalPlan(
        cte.children[0],
        rowManager,
        cteCache,
        ctx,
      );
      const main = createPhysicalPlan(
        cte.children[1],
        rowManager,
        cteCache,
        ctx,
      );
      return new PhysicalMaterialize(def, main, cte.cteIndex, cteCache);
    }

    case LogicalOperatorType.LOGICAL_CTE_REF: {
      const ref = node as LogicalCTERef;
      return new PhysicalCTEScan(ref, cteCache);
    }

    default:
      throw new ExecutorError(
        `Cannot create physical plan for ${node.type} — use executor directly for DML/DDL`,
      );
  }
}

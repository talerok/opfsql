import type {
  BoundExpression,
  LogicalAggregate,
  LogicalComparisonJoin,
  LogicalCTERef,
  LogicalFilter,
  LogicalGet,
  LogicalLimit,
  LogicalMaterializedCTE,
  LogicalOperator,
  LogicalOrderBy,
  LogicalProjection,
  LogicalRecursiveCTE,
  LogicalUnion,
} from "../binder/types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../binder/types.js";
import type {
  SyncIIndexManager,
  SyncIRowManager,
} from "../store/types.js";
import { ExecutorError } from "./errors.js";
import type { SyncEvalContext } from "./evaluate/context.js";
import { PhysicalHashAggregate } from "./operators/aggregate.js";
import {
  PhysicalCTEScan,
  PhysicalMaterialize,
  PhysicalRecursiveCTE,
} from "./operators/cte.js";
import { PhysicalFilter } from "./operators/filter.js";
import { PhysicalIndexScan } from "./operators/index-scan.js";
import { PhysicalIndexMinMax } from "./operators/index-min-max.js";
import { PhysicalIndexUnionScan } from "./operators/index-union-scan.js";
import { PhysicalHashJoin, PhysicalNestedLoopJoin } from "./operators/join.js";
import { PhysicalLimit } from "./operators/limit.js";
import { PhysicalProjection } from "./operators/projection.js";
import { PhysicalChildScan, PhysicalScan } from "./operators/scan.js";
import { PhysicalDistinct, PhysicalUnion } from "./operators/set.js";
import { PhysicalSort } from "./operators/sort.js";
import type { CTECacheEntry, SyncPhysicalOperator } from "./types.js";

export function createPhysicalPlan(
  node: LogicalOperator,
  rowManager: SyncIRowManager,
  cteCache: Map<number, CTECacheEntry>,
  ctx: SyncEvalContext,
  indexManager?: SyncIIndexManager,
): SyncPhysicalOperator {
  const plan = (child: LogicalOperator) =>
    createPhysicalPlan(child, rowManager, cteCache, ctx, indexManager);

  switch (node.type) {
    case LogicalOperatorType.LOGICAL_GET: {
      const get = node as LogicalGet;
      if (get.indexHint && indexManager) {
        switch (get.indexHint.kind) {
          case 'union':
            return new PhysicalIndexUnionScan(
              get, rowManager, indexManager, get.indexHint, ctx,
            );
          case 'scan':
            return new PhysicalIndexScan(
              get,
              rowManager,
              indexManager,
              get.indexHint.indexDef,
              get.indexHint.predicates,
              get.indexHint.residualFilters,
              ctx,
            );
        }
      }
      if (get.children.length > 0) {
        return new PhysicalChildScan(get, plan(get.children[0]), ctx);
      }
      return new PhysicalScan(get, rowManager, ctx);
    }

    case LogicalOperatorType.LOGICAL_FILTER: {
      const filter = node as LogicalFilter;
      let condition: BoundExpression;
      if (filter.expressions.length === 1) {
        condition = filter.expressions[0];
      } else {
        condition = {
          expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
          conjunctionType: 'AND' as const,
          children: filter.expressions,
          returnType: 'BOOLEAN' as const,
        };
      }
      return new PhysicalFilter(plan(filter.children[0]), condition, ctx);
    }

    case LogicalOperatorType.LOGICAL_PROJECTION: {
      const proj = node as LogicalProjection;
      return new PhysicalProjection(plan(proj.children[0]), proj, ctx);
    }

    case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY: {
      const agg = node as LogicalAggregate;
      if (agg.minMaxHint && indexManager) {
        return new PhysicalIndexMinMax(agg, indexManager, agg.minMaxHint);
      }
      return new PhysicalHashAggregate(plan(agg.children[0]), agg, ctx);
    }

    case LogicalOperatorType.LOGICAL_COMPARISON_JOIN: {
      const join = node as LogicalComparisonJoin;
      return new PhysicalHashJoin(
        plan(join.children[0]),
        plan(join.children[1]),
        join,
        ctx,
      );
    }

    case LogicalOperatorType.LOGICAL_CROSS_PRODUCT:
      return new PhysicalNestedLoopJoin(
        plan(node.children[0]),
        plan(node.children[1]),
      );

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
      return new PhysicalUnion(
        plan(union.children[0]),
        plan(union.children[1]),
        union.all,
      );
    }

    case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE: {
      const cte = node as LogicalMaterializedCTE;
      return new PhysicalMaterialize(
        plan(cte.children[0]),
        plan(cte.children[1]),
        cte.cteIndex,
        cteCache,
      );
    }

    case LogicalOperatorType.LOGICAL_RECURSIVE_CTE: {
      const rec = node as LogicalRecursiveCTE;
      return new PhysicalRecursiveCTE(
        plan(rec.children[0]),
        plan(rec.children[1]),
        rec.cteIndex,
        cteCache,
        rec.isUnionAll,
      );
    }

    case LogicalOperatorType.LOGICAL_CTE_REF:
      return new PhysicalCTEScan(node as LogicalCTERef, cteCache);

    default:
      throw new ExecutorError(
        `Cannot create physical plan for ${node.type} — use executor directly for DML/DDL`,
      );
  }
}

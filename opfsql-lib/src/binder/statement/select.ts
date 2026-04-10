import type { LogicalType } from '../../store/types.js';
import type { SelectNode, StarExpression, OrderByNode } from '../../parser/types.js';
import { ExpressionClass, ResultModifierType } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { makeEmptyGet, makeFilter, makeDistinct, makeOrderBy, makeLimit } from '../core/operators.js';
import { evalConstantInt } from '../core/helpers.js';
import { checkNoAggregates, detectAggregates, extractAggregates, extractAggregatesFromExpr } from '../expression/aggregate.js';
import { sameExpression, sameAggregate } from '../expression/same-expression.js';
import { bindExpression } from '../expression/index.js';
import { bindStar } from '../expression/star.js';
import { bindTableRef } from '../table-ref/index.js';
import { collectCTEs } from './cte.js';

export function bindSelect(
  ctx: BindContext,
  node: SelectNode,
  scope: BindScope,
): BT.LogicalOperator {
  const cteEntries = collectCTEs(ctx, node.cte_map, scope);

  let plan: BT.LogicalOperator = node.from_table
    ? bindTableRef(ctx, node.from_table, scope)
    : makeEmptyGet(ctx);

  if (node.where_clause) {
    checkNoAggregates(node.where_clause);
    const filter = bindExpression(ctx, node.where_clause, scope);
    plan = makeFilter(plan, [filter]);
  }

  const hasGroupBy = node.groups.group_expressions.length > 0;
  const hasAggregates =
    detectAggregates(node.select_list) ||
    (node.having !== null && detectAggregates([node.having]));

  let aggregates: BT.BoundAggregateExpression[] = [];
  let havingBound: BT.BoundExpression | null = null;
  let groups: BT.BoundExpression[] = [];
  let groupIndex = -1;

  if (hasGroupBy || hasAggregates) {
    for (const g of node.groups.group_expressions) {
      checkNoAggregates(g, 'GROUP BY clause');
    }

    groups = node.groups.group_expressions.map((g) => bindExpression(ctx, g, scope));

    aggregates = extractAggregates(ctx, node.select_list, scope);
    if (node.having) {
      const havingAggs = extractAggregatesFromExpr(ctx, node.having, scope);
      for (const agg of havingAggs) {
        if (!aggregates.some((a) => sameAggregate(a, agg))) {
          aggregates.push(agg);
        }
      }
    }

    groupIndex = ctx.nextTableIndex();
    const aggregateIndex = ctx.nextTableIndex();

    for (let i = 0; i < aggregates.length; i++) {
      aggregates[i].aggregateIndex = i;
      aggregates[i].binding = { tableIndex: aggregateIndex, columnIndex: i };
    }

    const aggTypes: LogicalType[] = [
      ...groups.map((g) => g.returnType),
      ...aggregates.map((a) => a.returnType),
    ];

    const aggCtx: AggregateContext = { aggregates, groups, groupIndex };
    havingBound = node.having
      ? bindExpression(ctx, node.having, scope, aggCtx)
      : null;

    const groupBindings: BT.ColumnBinding[] = [
      ...groups.map((_, i) => ({ tableIndex: groupIndex, columnIndex: i })),
      ...aggregates.map((_, i) => ({ tableIndex: aggregateIndex, columnIndex: i })),
    ];

    plan = {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex,
      aggregateIndex,
      children: [plan],
      expressions: aggregates,
      groups,
      havingExpression: havingBound,
      types: aggTypes,
      estimatedCardinality: 0,
      getColumnBindings: () => groupBindings,
    } satisfies BT.LogicalAggregate;
  }

  const aggCtx: AggregateContext | undefined =
    (hasGroupBy || hasAggregates) ? { aggregates, groups, groupIndex } : undefined;

  const boundSelectList: BT.BoundExpression[] = [];
  const selectAliases: (string | null)[] = [];
  for (const expr of node.select_list) {
    if (expr.expression_class === ExpressionClass.STAR) {
      const expanded = bindStar(expr as StarExpression, scope, aggCtx);
      boundSelectList.push(...expanded);
      for (let i = 0; i < expanded.length; i++) selectAliases.push(null);
    } else {
      boundSelectList.push(bindExpression(ctx, expr, scope, aggCtx));
      selectAliases.push(expr.alias);
    }
  }

  const projTableIndex = ctx.nextTableIndex();
  const projTypes = boundSelectList.map((e) => e.returnType);
  const projBindings: BT.ColumnBinding[] = boundSelectList.map((_, i) => ({
    tableIndex: projTableIndex,
    columnIndex: i,
  }));

  plan = {
    type: LogicalOperatorType.LOGICAL_PROJECTION,
    tableIndex: projTableIndex,
    children: [plan],
    expressions: boundSelectList,
    aliases: selectAliases,
    types: projTypes,
    estimatedCardinality: 0,
    getColumnBindings: () => projBindings,
  } satisfies BT.LogicalProjection;

  for (const mod of node.modifiers) {
    switch (mod.type) {
      case ResultModifierType.DISTINCT_MODIFIER:
        plan = makeDistinct(plan);
        break;
      case ResultModifierType.ORDER_MODIFIER: {
        const boundOrders = bindOrders(ctx, mod.orders, scope, aggCtx);
        const originalCount = boundSelectList.length;

        // Rewrite ORDER BY expressions to reference projection output.
        // ORDER BY sits above projection in the plan, so its expressions
        // must use the projection's output bindings, not pre-projection ones.
        const rewrittenOrders: BT.BoundOrderByNode[] = boundOrders.map((order) => {
          const idx = boundSelectList.findIndex((sel) =>
            sameExpression(sel, order.expression));
          if (idx !== -1) {
            return { ...order, expression: projRef(projBindings[idx], order.expression.returnType) };
          }
          // Expression not in select list — extend projection so sort can access it
          const ci = boundSelectList.length;
          boundSelectList.push(order.expression);
          selectAliases.push(null);
          projTypes.push(order.expression.returnType);
          const binding: BT.ColumnBinding = { tableIndex: projTableIndex, columnIndex: ci };
          projBindings.push(binding);
          return { ...order, expression: projRef(binding, order.expression.returnType) };
        });

        plan = makeOrderBy(plan, rewrittenOrders);

        // If we extended the projection, add a trimming projection above ORDER BY
        // to remove the extra columns from the final output.
        if (boundSelectList.length > originalCount) {
          const trimIdx = ctx.nextTableIndex();
          const trimBindings = Array.from({ length: originalCount }, (_, i) => ({
            tableIndex: trimIdx, columnIndex: i,
          }));
          plan = {
            type: LogicalOperatorType.LOGICAL_PROJECTION,
            tableIndex: trimIdx,
            children: [plan],
            expressions: Array.from({ length: originalCount }, (_, i) => {
              const orig = boundSelectList[i];
              const name = orig.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF
                ? (orig as BT.BoundColumnRefExpression).columnName : '';
              return projRef(projBindings[i], projTypes[i], name);
            }),
            aliases: selectAliases.slice(0, originalCount),
            types: projTypes.slice(0, originalCount),
            estimatedCardinality: 0,
            getColumnBindings: () => trimBindings,
          } satisfies BT.LogicalProjection;
        }
        break;
      }
      case ResultModifierType.LIMIT_MODIFIER: {
        const limitVal = mod.limit !== null ? evalConstantInt(mod.limit) : null;
        const offsetVal = mod.offset !== null ? evalConstantInt(mod.offset) : 0;
        plan = makeLimit(plan, limitVal, offsetVal);
        break;
      }
    }
  }

  for (let i = cteEntries.length - 1; i >= 0; i--) {
    const cte = cteEntries[i];
    const innerPlan: BT.LogicalOperator = plan;
    plan = {
      type: LogicalOperatorType.LOGICAL_MATERIALIZED_CTE,
      cteName: cte.name,
      cteIndex: cte.index,
      children: [cte.plan, innerPlan],
      expressions: [],
      types: innerPlan.types,
      estimatedCardinality: 0,
      getColumnBindings: () => innerPlan.getColumnBindings(),
    } satisfies BT.LogicalMaterializedCTE;
  }

  return plan;
}

function bindOrders(
  ctx: BindContext,
  orders: OrderByNode[],
  scope: BindScope,
  aggCtx?: AggregateContext,
): BT.BoundOrderByNode[] {
  return orders.map((o) => ({
    expression: bindExpression(ctx, o.expression, scope, aggCtx),
    orderType: o.type,
    nullOrder: o.null_order,
  }));
}

function projRef(binding: BT.ColumnBinding, returnType: LogicalType, columnName = ''): BT.BoundColumnRefExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding,
    tableName: '',
    columnName,
    returnType,
  };
}

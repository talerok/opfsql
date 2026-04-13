import type {
  ColumnRefExpression,
  OrderByNode,
  SelectNode,
  StarExpression,
} from "../../parser/types.js";
import { ExpressionClass, ResultModifierType } from "../../parser/types.js";
import type { LogicalType } from "../../store/types.js";
import type { AggregateContext, BindContext } from "../core/context.js";
import { evalConstantInt } from "../core/helpers.js";
import {
  makeDistinct,
  makeEmptyGet,
  makeFilter,
  makeLimit,
  makeOrderBy,
} from "../core/operators.js";
import type { BindScope } from "../core/scope.js";
import {
  checkNoAggregates,
  detectAggregates,
  extractAggregates,
  extractAggregatesFromExpr,
} from "../expression/aggregate.js";
import { bindExpression } from "../expression/index.js";
import {
  sameAggregate,
  sameExpression,
} from "../expression/same-expression.js";
import { bindStar } from "../expression/star.js";
import { bindTableRef } from "../table-ref/index.js";
import type * as BT from "../types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../types.js";
import { collectCTEs } from "./cte.js";

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
    plan = makeFilter(plan, [bindExpression(ctx, node.where_clause, scope)]);
  }

  // --- GROUP BY / aggregates ---
  const aggCtx = bindAggregation(ctx, node, scope, plan);
  if (aggCtx.aggregatePlan) plan = aggCtx.aggregatePlan;

  // --- SELECT list ---
  const proj = buildProjection(ctx, node, scope, plan, aggCtx.context);

  // --- Modifiers (DISTINCT, ORDER BY, LIMIT) ---
  plan = applyModifiers(ctx, node, scope, proj, aggCtx.context);

  // --- Wrap CTEs ---
  plan = wrapCTEs(plan, cteEntries);

  return plan;
}

// ---------------------------------------------------------------------------
// Aggregation (GROUP BY + aggregate functions)
// ---------------------------------------------------------------------------

interface AggregationResult {
  aggregatePlan: BT.LogicalOperator | null;
  context: AggregateContext | undefined;
}

function bindAggregation(
  ctx: BindContext,
  node: SelectNode,
  scope: BindScope,
  plan: BT.LogicalOperator,
): AggregationResult {
  const hasGroupBy = node.groups.group_expressions.length > 0;
  const hasAggregates =
    detectAggregates(node.select_list) ||
    (node.having !== null && detectAggregates([node.having]));

  if (!hasGroupBy && !hasAggregates) {
    return { aggregatePlan: null, context: undefined };
  }

  for (const g of node.groups.group_expressions) {
    checkNoAggregates(g, "GROUP BY clause");
  }

  const groups = node.groups.group_expressions.map((g) =>
    bindExpression(ctx, g, scope),
  );

  // Collect aggregates from SELECT and HAVING
  const aggregates = extractAggregates(ctx, node.select_list, scope);
  if (node.having) {
    for (const agg of extractAggregatesFromExpr(ctx, node.having, scope)) {
      if (!aggregates.some((a) => sameAggregate(a, agg))) {
        aggregates.push(agg);
      }
    }
  }

  const groupIndex = ctx.nextTableIndex();
  const aggregateIndex = ctx.nextTableIndex();

  for (let i = 0; i < aggregates.length; i++) {
    aggregates[i].aggregateIndex = i;
    aggregates[i].binding = { tableIndex: aggregateIndex, columnIndex: i };
  }

  const aggCtx: AggregateContext = { aggregates, groups, groupIndex };
  const havingBound = node.having
    ? bindExpression(ctx, node.having, scope, aggCtx)
    : null;

  const groupBindings: BT.ColumnBinding[] = [
    ...groups.map((_, i) => ({ tableIndex: groupIndex, columnIndex: i })),
    ...aggregates.map((_, i) => ({
      tableIndex: aggregateIndex,
      columnIndex: i,
    })),
  ];

  const aggregatePlan: BT.LogicalAggregate = {
    type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    groupIndex,
    aggregateIndex,
    children: [plan],
    expressions: aggregates,
    groups,
    havingExpression: havingBound,
    types: [
      ...groups.map((g) => g.returnType),
      ...aggregates.map((a) => a.returnType),
    ],
    estimatedCardinality: 0,
    getColumnBindings: () => groupBindings,
  };

  return { aggregatePlan, context: aggCtx };
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

interface ProjectionState {
  plan: BT.LogicalOperator;
  expressions: BT.BoundExpression[];
  aliases: (string | null)[];
  types: LogicalType[];
  bindings: BT.ColumnBinding[];
  tableIndex: number;
}

function buildProjection(
  ctx: BindContext,
  node: SelectNode,
  scope: BindScope,
  plan: BT.LogicalOperator,
  aggCtx: AggregateContext | undefined,
): ProjectionState {
  const expressions: BT.BoundExpression[] = [];
  const aliases: (string | null)[] = [];

  for (const expr of node.select_list) {
    if (expr.expression_class === ExpressionClass.STAR) {
      const expanded = bindStar(expr as StarExpression, scope, aggCtx);
      expressions.push(...expanded);
      for (let i = 0; i < expanded.length; i++) aliases.push(null);
    } else {
      expressions.push(bindExpression(ctx, expr, scope, aggCtx));
      aliases.push(expr.alias);
    }
  }

  const tableIndex = ctx.nextTableIndex();
  const types = expressions.map((e) => e.returnType);
  const bindings: BT.ColumnBinding[] = expressions.map((_, i) => ({
    tableIndex,
    columnIndex: i,
  }));

  const projPlan: BT.LogicalProjection = {
    type: LogicalOperatorType.LOGICAL_PROJECTION,
    tableIndex,
    children: [plan],
    expressions,
    aliases,
    types,
    estimatedCardinality: 0,
    getColumnBindings: () => bindings,
  };

  return { plan: projPlan, expressions, aliases, types, bindings, tableIndex };
}

// ---------------------------------------------------------------------------
// Modifiers (DISTINCT, ORDER BY, LIMIT)
// ---------------------------------------------------------------------------

function applyModifiers(
  ctx: BindContext,
  node: SelectNode,
  scope: BindScope,
  proj: ProjectionState,
  aggCtx: AggregateContext | undefined,
): BT.LogicalOperator {
  let plan = proj.plan;

  for (const mod of node.modifiers) {
    switch (mod.type) {
      case ResultModifierType.DISTINCT_MODIFIER:
        plan = makeDistinct(plan);
        break;

      case ResultModifierType.ORDER_MODIFIER:
        plan = applyOrderBy(ctx, mod.orders, scope, proj, aggCtx, plan);
        break;

      case ResultModifierType.LIMIT_MODIFIER:
        plan = makeLimit(
          plan,
          mod.limit !== null ? evalConstantInt(mod.limit) : null,
          mod.offset !== null ? evalConstantInt(mod.offset) : 0,
        );
        break;
    }
  }

  return plan;
}

function applyOrderBy(
  ctx: BindContext,
  orders: OrderByNode[],
  scope: BindScope,
  proj: ProjectionState,
  aggCtx: AggregateContext | undefined,
  currentPlan: BT.LogicalOperator,
): BT.LogicalOperator {
  const originalCount = proj.expressions.length;

  const rewrittenOrders: BT.BoundOrderByNode[] = orders.map((o) => {
    // 1. Check if ORDER BY references a projection alias (e.g. ORDER BY total)
    //    before binding, since the alias is not a real column and would fail.
    if (o.expression.expression_class === ExpressionClass.COLUMN_REF) {
      const ref = o.expression as ColumnRefExpression;
      if (ref.column_names.length === 1) {
        const name = ref.column_names[0].toLowerCase();
        const aliasIdx = proj.aliases.findIndex(
          (a) => a !== null && a.toLowerCase() === name,
        );
        if (aliasIdx !== -1) {
          return {
            expression: projRef(proj.bindings[aliasIdx], proj.types[aliasIdx]),
            orderType: o.type,
            nullOrder: o.null_order,
          };
        }
      }
    }

    // 2. Bind normally and try structural match against projection expressions
    const bound = bindExpression(ctx, o.expression, scope, aggCtx);
    const idx = proj.expressions.findIndex((sel) =>
      sameExpression(sel, bound),
    );

    if (idx !== -1) {
      return {
        expression: projRef(proj.bindings[idx], bound.returnType),
        orderType: o.type,
        nullOrder: o.null_order,
      };
    }

    // 3. Expression not in select list — extend projection so sort can access it
    const colIndex = proj.expressions.length;
    proj.expressions.push(bound);
    proj.aliases.push(null);
    proj.types.push(bound.returnType);
    const binding: BT.ColumnBinding = {
      tableIndex: proj.tableIndex,
      columnIndex: colIndex,
    };
    proj.bindings.push(binding);
    return {
      expression: projRef(binding, bound.returnType),
      orderType: o.type,
      nullOrder: o.null_order,
    };
  });

  // Use currentPlan (not proj.plan) to preserve any operators (e.g. DISTINCT)
  // that were applied between the projection and this ORDER BY.
  let plan: BT.LogicalOperator = makeOrderBy(currentPlan, rewrittenOrders);

  // If we extended the projection, add a trimming projection to remove extra columns
  if (proj.expressions.length > originalCount) {
    plan = buildTrimProjection(ctx, plan, proj, originalCount);
  }

  return plan;
}

function buildTrimProjection(
  ctx: BindContext,
  plan: BT.LogicalOperator,
  proj: ProjectionState,
  originalCount: number,
): BT.LogicalProjection {
  const trimIdx = ctx.nextTableIndex();
  const trimBindings = Array.from({ length: originalCount }, (_, i) => ({
    tableIndex: trimIdx,
    columnIndex: i,
  }));

  return {
    type: LogicalOperatorType.LOGICAL_PROJECTION,
    tableIndex: trimIdx,
    children: [plan],
    expressions: Array.from({ length: originalCount }, (_, i) => {
      const orig = proj.expressions[i];
      const name =
        orig.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF
          ? (orig as BT.BoundColumnRefExpression).columnName
          : "";
      return projRef(proj.bindings[i], proj.types[i], name);
    }),
    aliases: proj.aliases.slice(0, originalCount),
    types: proj.types.slice(0, originalCount),
    estimatedCardinality: 0,
    getColumnBindings: () => trimBindings,
  };
}

// ---------------------------------------------------------------------------
// CTE wrapping
// ---------------------------------------------------------------------------

function wrapCTEs(
  plan: BT.LogicalOperator,
  cteEntries: Array<{ name: string; index: number; plan: BT.LogicalOperator }>,
): BT.LogicalOperator {
  for (let i = cteEntries.length - 1; i >= 0; i--) {
    const cte = cteEntries[i];
    const inner = plan;
    plan = {
      type: LogicalOperatorType.LOGICAL_MATERIALIZED_CTE,
      cteName: cte.name,
      cteIndex: cte.index,
      children: [cte.plan, inner],
      expressions: [],
      types: inner.types,
      estimatedCardinality: 0,
      getColumnBindings: () => inner.getColumnBindings(),
    } satisfies BT.LogicalMaterializedCTE;
  }
  return plan;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function projRef(
  binding: BT.ColumnBinding,
  returnType: LogicalType,
  columnName = "",
): BT.BoundColumnRefExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding,
    tableName: "",
    columnName,
    returnType,
  };
}

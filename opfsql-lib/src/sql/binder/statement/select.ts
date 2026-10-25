import type {
  ColumnReferenceExpression,
  OrderByNode,
  ParsedExpression,
  SelectNode,
  StarExpression,
} from "../../parser/types.js";
import { ExpressionClass } from "../../parser/types.js";
import type { LogicalType } from "../../../store/types.js";
import type { AggregateContext, BindContext } from "../core/context.js";
import {
  makeAggregate,
  makeDistinct,
  makeEmptyGet,
  makeFilter,
  makeLimit,
  makeOrderBy,
  makeProjection,
} from "../core/operators.js";
import type { BindScope } from "../core/scope.js";
import { evalConstantInt } from "../core/utils/eval-constant.js";
import { nullOrderOf } from "../core/utils/null-order.js";
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
import type { QueryClauses } from "./query-clauses.js";

export function bindSelect(
  ctx: BindContext,
  node: SelectNode,
  scope: BindScope,
  clauses: QueryClauses,
): BT.LogicalOperator {
  const cteEntries = collectCTEs(ctx, clauses.with, scope);

  let plan: BT.LogicalOperator = node.from
    ? bindTableRef(ctx, node.from, scope)
    : makeEmptyGet(ctx);

  if (node.where) {
    checkNoAggregates(node.where);
    plan = makeFilter(plan, [bindExpression(ctx, node.where, scope)]);
  }

  const aggCtx = bindAggregation(ctx, node, scope, plan);
  if (aggCtx.aggregatePlan) plan = aggCtx.aggregatePlan;

  const proj = buildProjection(ctx, node, scope, plan, aggCtx.context);

  const modifiers = { ...clauses, isDistinct: node.isDistinct };
  plan = applyModifiers(ctx, modifiers, scope, proj, aggCtx.context);

  return wrapCTEs(plan, cteEntries);
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
  const hasGroupBy = node.groupBy.length > 0;
  const hasAggregates =
    detectAggregates(node.selectList) ||
    (node.having !== null && detectAggregates([node.having]));

  if (!hasGroupBy && !hasAggregates) {
    return { aggregatePlan: null, context: undefined };
  }

  for (const g of node.groupBy) {
    checkNoAggregates(g, "GROUP BY clause");
  }

  const groups = node.groupBy.map((g) => bindExpression(ctx, g, scope));
  const aggregates = collectAllAggregates(
    ctx,
    node.selectList,
    node.having,
    scope,
  );

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

  return {
    aggregatePlan: makeAggregate(
      plan,
      groups,
      aggregates,
      groupIndex,
      aggregateIndex,
      havingBound,
    ),
    context: aggCtx,
  };
}

/** Collect unique aggregates from SELECT list and HAVING clause. */
function collectAllAggregates(
  ctx: BindContext,
  selectList: readonly ParsedExpression[],
  having: ParsedExpression | null,
  scope: BindScope,
): BT.BoundAggregateExpression[] {
  const aggregates = extractAggregates(ctx, selectList, scope);
  if (having) {
    for (const agg of extractAggregatesFromExpr(ctx, having, scope)) {
      if (!aggregates.some((a) => sameAggregate(a, agg))) {
        aggregates.push(agg);
      }
    }
  }
  return aggregates;
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

  for (const expr of node.selectList) {
    if (expr.expressionClass === ExpressionClass.STAR) {
      const expanded = bindStar(expr as StarExpression, scope, aggCtx);
      expressions.push(...expanded);
      for (let i = 0; i < expanded.length; i++) aliases.push(null);
    } else {
      expressions.push(bindExpression(ctx, expr, scope, aggCtx));
      aliases.push(expr.alias);
    }
  }

  const tableIndex = ctx.nextTableIndex();
  const projPlan = makeProjection(plan, tableIndex, expressions, aliases);
  const { types, columnBindings: bindings } = projPlan;

  return { plan: projPlan, expressions, aliases, types, bindings, tableIndex };
}

// ---------------------------------------------------------------------------
// Modifiers (DISTINCT, ORDER BY, LIMIT)
// ---------------------------------------------------------------------------

/** DISTINCT, then ORDER BY, then LIMIT. */
function applyModifiers(
  ctx: BindContext,
  modifiers: QueryClauses & { isDistinct: boolean },
  scope: BindScope,
  proj: ProjectionState,
  aggCtx: AggregateContext | undefined,
): BT.LogicalOperator {
  let plan = proj.plan;
  if (modifiers.isDistinct) plan = makeDistinct(plan);
  if (modifiers.orderBy.length > 0) {
    plan = applyOrderBy(ctx, modifiers.orderBy, scope, proj, aggCtx, plan);
  }
  const limit = modifiers.limit;
  if (limit !== null) {
    plan = makeLimit(
      plan,
      limit.limit !== null ? evalConstantInt(limit.limit) : null,
      limit.offset !== null ? evalConstantInt(limit.offset) : 0,
    );
  }
  return plan;
}

function applyOrderBy(
  ctx: BindContext,
  orders: readonly OrderByNode[],
  scope: BindScope,
  proj: ProjectionState,
  aggCtx: AggregateContext | undefined,
  currentPlan: BT.LogicalOperator,
): BT.LogicalOperator {
  const originalCount = proj.expressions.length;

  const boundOrders = orders.map((o) =>
    resolveOrderExpression(ctx, o, scope, proj, aggCtx),
  );

  let plan: BT.LogicalOperator = makeOrderBy(currentPlan, boundOrders);

  if (proj.expressions.length > originalCount) {
    plan = buildTrimProjection(ctx, plan, proj, originalCount);
  }

  return plan;
}

/** Resolve a single ORDER BY expression against the projection. */
function resolveOrderExpression(
  ctx: BindContext,
  order: OrderByNode,
  scope: BindScope,
  proj: ProjectionState,
  aggCtx: AggregateContext | undefined,
): BT.BoundOrderByNode {
  // 1. Try alias match (e.g. ORDER BY total)
  const aliasIdx = tryMatchAlias(order.expression, proj);
  if (aliasIdx !== -1) {
    return makeBoundOrder(
      order,
      projRef(proj.bindings[aliasIdx], proj.types[aliasIdx]),
    );
  }

  // 2. Bind and try structural match against projection expressions
  const bound = bindExpression(ctx, order.expression, scope, aggCtx);
  const matchIdx = proj.expressions.findIndex((sel) =>
    sameExpression(sel, bound),
  );
  if (matchIdx !== -1) {
    return makeBoundOrder(
      order,
      projRef(proj.bindings[matchIdx], bound.returnType),
    );
  }

  // 3. Not in select list — extend projection so sort can access it
  const binding = extendProjection(proj, bound);
  return makeBoundOrder(order, projRef(binding, bound.returnType));
}

/** Check if a column ref with a single name matches a projection alias. */
function tryMatchAlias(expr: ParsedExpression, proj: ProjectionState): number {
  if (expr.expressionClass !== ExpressionClass.COLUMN_REF) return -1;
  const ref = expr as ColumnReferenceExpression;
  if (ref.columnNames.length !== 1) return -1;
  const name = ref.columnNames[0].toLowerCase();
  return proj.aliases.findIndex((a) => a !== null && a.toLowerCase() === name);
}

/** Add an expression to the projection and return its binding. */
function extendProjection(
  proj: ProjectionState,
  expr: BT.BoundExpression,
): BT.ColumnBinding {
  const colIndex = proj.expressions.length;
  proj.expressions.push(expr);
  proj.aliases.push(null);
  proj.types.push(expr.returnType);
  const binding: BT.ColumnBinding = {
    tableIndex: proj.tableIndex,
    columnIndex: colIndex,
  };
  proj.bindings.push(binding);
  return binding;
}

function buildTrimProjection(
  ctx: BindContext,
  plan: BT.LogicalOperator,
  proj: ProjectionState,
  originalCount: number,
): BT.LogicalProjection {
  const trimIdx = ctx.nextTableIndex();
  const trimExprs = Array.from({ length: originalCount }, (_, i) => {
    const orig = proj.expressions[i];
    const name =
      orig.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF
        ? (orig as BT.BoundColumnRefExpression).columnName
        : "";
    return projRef(proj.bindings[i], proj.types[i], name);
  });
  return makeProjection(
    plan,
    trimIdx,
    trimExprs,
    proj.aliases.slice(0, originalCount),
  );
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
      columnBindings: inner.columnBindings,
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

function makeBoundOrder(
  order: OrderByNode,
  expression: BT.BoundExpression,
): BT.BoundOrderByNode {
  return { expression, orderType: order.type, nullOrder: nullOrderOf(order) };
}

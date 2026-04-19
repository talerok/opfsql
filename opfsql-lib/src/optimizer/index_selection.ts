import type {
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundParameterExpression,
  IndexScanHint,
  IndexSearchPredicate,
  LogicalAggregate,
  LogicalFilter,
  LogicalGet,
  LogicalOperator,
  LogicalOrderBy,
  LogicalProjection,
  TableFilter,
} from "../binder/types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../binder/types.js";
import { bindIndexExpression } from "../store/index-expression.js";
import type { ICatalog, IndexDef } from "../store/types.js";
import type { TableSchema } from "../types.js";
import {
  flattenConjunction,
  flipComparison,
  isConstant,
  isParameter,
  sameExpression,
} from "./utils/index.js";

// ─── Public API ────────────────────────────────────────────────────────────

export function selectIndexes(
  plan: LogicalOperator,
  catalog: ICatalog,
): LogicalOperator {
  let result = plan;
  result = walkTree(result, catalog, annotateScanIndex);
  result = walkTree(result, catalog, annotateOrUnion);
  result = walkTree(result, catalog, annotateMinMax);
  result = walkTree(result, catalog, annotateOrderBy);
  return result;
}

// ─── Generic tree walk ─────────────────────────────────────────────────────

type Visitor = (node: LogicalOperator, catalog: ICatalog) => LogicalOperator;

function walkTree(
  node: LogicalOperator,
  catalog: ICatalog,
  visit: Visitor,
): LogicalOperator {
  for (let i = 0; i < node.children.length; i++) {
    node.children[i] = walkTree(node.children[i], catalog, visit);
  }
  return visit(node, catalog);
}

// ─── Tree navigation helpers ───────────────────────────────────────────────

/** Walk through Projection nodes to find a LogicalGet with no filters and no index hint. */
function findUnfilteredGet(node: LogicalOperator): LogicalGet | null {
  if (node.type === LogicalOperatorType.LOGICAL_GET) {
    const get = node as LogicalGet;
    return get.tableFilters.length === 0 && !get.indexHint ? get : null;
  }
  if (
    node.type === LogicalOperatorType.LOGICAL_PROJECTION &&
    node.children.length === 1
  ) {
    return findUnfilteredGet(node.children[0]);
  }
  return null;
}

/**
 * Walk through Projection/Filter to find a LogicalGet,
 * collecting intermediate Projections (outermost first).
 */
function findDescendantGetWithPath(
  node: LogicalOperator,
): { get: LogicalGet; projections: LogicalProjection[] } | null {
  if (node.type === LogicalOperatorType.LOGICAL_GET) {
    return { get: node as LogicalGet, projections: [] };
  }
  if (
    node.type === LogicalOperatorType.LOGICAL_PROJECTION ||
    node.type === LogicalOperatorType.LOGICAL_FILTER
  ) {
    const result = findDescendantGetWithPath(node.children[0]);
    if (!result) return null;
    if (node.type === LogicalOperatorType.LOGICAL_PROJECTION) {
      result.projections.unshift(node as LogicalProjection);
    }
    return result;
  }
  return null;
}

/**
 * Resolve a column ref through intermediate Projections
 * to get the underlying expression at the Get level.
 */
function resolveExpression(
  expr: BoundExpression,
  projections: LogicalProjection[],
): BoundExpression {
  let current = expr;
  for (const proj of projections) {
    if (current.expressionClass !== BoundExpressionClass.BOUND_COLUMN_REF)
      break;
    const ref = current as BoundColumnRefExpression;
    if (ref.binding.tableIndex === proj.tableIndex) {
      current = proj.expressions[ref.binding.columnIndex];
    }
  }
  return current;
}

// ─── Filter extraction (used by OR Union pass) ────────────────────────────

/**
 * Extract TableFilter[] from a single OR branch expression.
 * The branch may be a simple comparison or an AND of comparisons.
 */
function extractTableFilters(expr: BoundExpression): TableFilter[] {
  const parts = flattenConjunction(expr);
  const filters: TableFilter[] = [];
  for (const part of parts) {
    const tf = tryExtractTableFilter(part);
    if (!tf) return [];
    filters.push(tf);
  }
  return filters;
}

function tryExtractTableFilter(expr: BoundExpression): TableFilter | null {
  if (expr.expressionClass !== BoundExpressionClass.BOUND_COMPARISON)
    return null;
  const cmp = expr as BoundComparisonExpression;

  if (isConstant(cmp.right) || isParameter(cmp.right)) {
    return {
      expression: cmp.left,
      comparisonType: cmp.comparisonType,
      constant: cmp.right as BoundConstantExpression | BoundParameterExpression,
    };
  }

  if (isConstant(cmp.left) || isParameter(cmp.left)) {
    return {
      expression: cmp.right,
      comparisonType: flipComparison(cmp.comparisonType),
      constant: cmp.left as BoundConstantExpression | BoundParameterExpression,
    };
  }

  return null;
}

/** Try to match every OR branch to an index. Returns null if any branch fails. */
function tryMatchOrBranches(
  branches: BoundExpression[],
  schema: TableSchema,
  tableIndex: number,
  indexes: IndexDef[],
): IndexScanHint[] | null {
  if (branches.length < 2) return null;
  const hints: IndexScanHint[] = [];
  for (const branch of branches) {
    const filters = extractTableFilters(branch);
    if (filters.length === 0) return null;
    const hint = findBestIndex(filters, schema, tableIndex, indexes);
    if (!hint) return null;
    hints.push(hint);
  }
  return hints;
}

// ─── Index matching (shared by pass 1 & 2) ────────────────────────────────

function findBestIndex(
  tableFilters: TableFilter[],
  schema: TableSchema,
  tableIndex: number,
  indexes: IndexDef[],
): IndexScanHint | null {
  let bestScore = 0;
  let bestHint: IndexScanHint | null = null;

  for (const idx of indexes) {
    const result = matchIndex(tableFilters, schema, tableIndex, idx);
    if (result && result.score > bestScore) {
      bestScore = result.score;
      bestHint = result.hint;
    }
  }

  return bestHint;
}

function matchIndex(
  tableFilters: TableFilter[],
  schema: TableSchema,
  tableIndex: number,
  idx: IndexDef,
): { score: number; hint: IndexScanHint } | null {
  const covered: TableFilter[] = [];
  const predicates: IndexSearchPredicate[] = [];
  let prefixMatched = 0;

  for (let i = 0; i < idx.expressions.length; i++) {
    const boundIdxExpr = bindIndexExpression(idx.expressions[i], schema, tableIndex);
    const matched = matchColumnFilters(tableFilters, boundIdxExpr, i);
    predicates.push(...matched.predicates);
    covered.push(...matched.covered);
    if (matched.type === "eq") { prefixMatched++; continue; }
    break; // range or miss — can't continue prefix
  }

  if (predicates.length === 0) return null;

  const residual = tableFilters.filter((f) => !covered.includes(f));
  let score = covered.length;
  if (idx.unique && prefixMatched === idx.expressions.length) score += 10;

  return {
    score,
    hint: { kind: "scan", indexDef: idx, predicates, residualFilters: residual, coveredFilters: covered },
  };
}

/** Match filters against a single index column. */
function matchColumnFilters(
  tableFilters: TableFilter[],
  boundIdxExpr: BoundExpression,
  columnPosition: number,
): { type: "eq" | "range" | "miss"; predicates: IndexSearchPredicate[]; covered: TableFilter[] } {
  const eqFilter = tableFilters.find(
    (f) => f.comparisonType === "EQUAL" && sameExpression(f.expression, boundIdxExpr),
  );
  if (eqFilter) {
    return {
      type: "eq",
      predicates: [{ columnPosition, comparisonType: "EQUAL", value: eqFilter.constant }],
      covered: [eqFilter],
    };
  }

  const rangeFilters = tableFilters.filter(
    (f) =>
      sameExpression(f.expression, boundIdxExpr) &&
      f.comparisonType !== "EQUAL" &&
      f.comparisonType !== "NOT_EQUAL",
  );
  if (rangeFilters.length === 0) return { type: "miss", predicates: [], covered: [] };

  return {
    type: "range",
    predicates: rangeFilters.map((rf) => ({
      columnPosition,
      comparisonType: rf.comparisonType as IndexSearchPredicate["comparisonType"],
      value: rf.constant,
    })),
    covered: rangeFilters,
  };
}

// ─── Pass 1: Scan index selection ──────────────────────────────────────────

function annotateScanIndex(
  node: LogicalOperator,
  catalog: ICatalog,
): LogicalOperator {
  if (node.type !== LogicalOperatorType.LOGICAL_GET) return node;

  const get = node as LogicalGet;
  if (get.tableName === "__empty" || get.tableFilters.length === 0) return node;

  const indexes = catalog.getTableIndexes(get.tableName);
  if (indexes.length === 0) return node;

  const best = findBestIndex(
    get.tableFilters,
    get.schema,
    get.tableIndex,
    indexes,
  );
  if (best) get.indexHint = best;
  return node;
}

// ─── Pass 2: OR / Index Union ──────────────────────────────────────────────

function annotateOrUnion(
  node: LogicalOperator,
  catalog: ICatalog,
): LogicalOperator {
  if (node.type !== LogicalOperatorType.LOGICAL_FILTER) return node;
  const filter = node as LogicalFilter;

  const result = findDescendantGetWithPath(filter.children[0]);
  if (!result) return node;

  const { get } = result;
  if (get.indexHint || get.tableName === "__empty") return node;

  const indexes = catalog.getTableIndexes(get.tableName);
  if (indexes.length === 0) return node;

  for (let ei = 0; ei < filter.expressions.length; ei++) {
    const expr = filter.expressions[ei];
    if (
      expr.expressionClass !== BoundExpressionClass.BOUND_CONJUNCTION ||
      (expr as BoundConjunctionExpression).conjunctionType !== "OR"
    )
      continue;

    const branches = tryMatchOrBranches(
      (expr as BoundConjunctionExpression).children,
      get.schema,
      get.tableIndex,
      indexes,
    );
    if (!branches) continue;

    get.indexHint = { kind: "union", branches, originalFilter: expr };
    filter.expressions.splice(ei, 1);
    if (filter.expressions.length === 0) return filter.children[0];
    return node;
  }

  return node;
}

// ─── Pass 3: MIN/MAX via index ─────────────────────────────────────────────

function annotateMinMax(
  node: LogicalOperator,
  catalog: ICatalog,
): LogicalOperator {
  if (node.type !== LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY)
    return node;
  const agg = node as LogicalAggregate;

  if (agg.groups.length !== 0) return node;
  if (agg.expressions.length !== 1) return node;
  const aggExpr = agg.expressions[0];
  if (aggExpr.functionName !== "MIN" && aggExpr.functionName !== "MAX")
    return node;
  if (aggExpr.isStar || aggExpr.distinct) return node;
  if (aggExpr.children.length !== 1) return node;

  const get = findUnfilteredGet(agg.children[0]);
  if (!get) return node;

  for (const idx of catalog.getTableIndexes(get.tableName)) {
    const boundIdxExpr = bindIndexExpression(
      idx.expressions[0],
      get.schema,
      get.tableIndex,
    );
    if (sameExpression(aggExpr.children[0], boundIdxExpr)) {
      agg.minMaxHint = {
        indexDef: idx,
        functionName: aggExpr.functionName,
        keyPosition: 0,
      };
      return node;
    }
  }

  return node;
}

// ─── Pass 4: ORDER BY via index ────────────────────────────────────────────
// Runs after pass 1 (scan index): if a scan index already matches the ORDER BY
// columns, we just eliminate the Sort node. Otherwise we set an order-only hint.

function annotateOrderBy(
  node: LogicalOperator,
  catalog: ICatalog,
): LogicalOperator {
  if (node.type !== LogicalOperatorType.LOGICAL_ORDER_BY) return node;
  const orderBy = node as LogicalOrderBy;

  if (orderBy.orders.some((o) => o.orderType !== "ASCENDING")) return node;

  const result = findDescendantGetWithPath(orderBy.children[0]);
  if (!result || result.get.tableName === "__empty") return node;

  const { get, projections } = result;
  const indexes = catalog.getTableIndexes(get.tableName);
  if (indexes.length === 0) return node;

  for (const idx of indexes) {
    if (!matchesOrderPrefix(orderBy.orders, idx, get, projections)) continue;

    if (
      get.indexHint?.kind === "scan" &&
      get.indexHint.indexDef.name === idx.name
    ) {
      return orderBy.children[0];
    }

    if (!get.indexHint) {
      get.indexHint = {
        kind: "scan",
        indexDef: idx,
        predicates: [],
        residualFilters: [],
        coveredFilters: [],
      };
      return orderBy.children[0];
    }
  }

  return node;
}

function matchesOrderPrefix(
  orders: LogicalOrderBy["orders"],
  idx: IndexDef,
  get: LogicalGet,
  projections: LogicalProjection[],
): boolean {
  if (orders.length > idx.expressions.length) return false;
  for (let i = 0; i < orders.length; i++) {
    const boundIdxExpr = bindIndexExpression(
      idx.expressions[i],
      get.schema,
      get.tableIndex,
    );
    const resolved = resolveExpression(orders[i].expression, projections);
    if (!sameExpression(resolved, boundIdxExpr)) return false;
  }
  return true;
}

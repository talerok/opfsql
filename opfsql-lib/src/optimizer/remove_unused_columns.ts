import type {
  BoundColumnRefExpression,
  BoundExpression,
  LogicalAggregate,
  LogicalComparisonJoin,
  LogicalCrossProduct,
  LogicalFilter,
  LogicalGet,
  LogicalOperator,
  LogicalOrderBy,
  LogicalProjection,
} from "../binder/types.js";
import { LogicalOperatorType } from "../binder/types.js";
import { collectColumnRefs, isColumnRef } from "./utils/index.js";

// ============================================================================
// Remove Unused Columns — prunes columns that aren't needed downstream
//
// Based on DuckDB's remove_unused_columns.cpp:
// Pure top-down pass. Starting from the root, propagates the set of needed
// column bindings downward. Each operator adds its own expression refs and
// passes the combined set to its children.
//
// Key insight: the "needed" set uses scan-level column bindings (tableIndex
// from LogicalGet), since that's what expressions reference throughout the tree.
// The projection has its own tableIndex for output bindings, but nodes above
// the projection (like ORDER BY) reference underlying scan columns directly.
// ============================================================================

export function removeUnusedColumns(plan: LogicalOperator): LogicalOperator {
  // Root: start with all bindings the root outputs as needed
  const needed = new Set<string>();
  for (const b of plan.columnBindings) {
    needed.add(bk(b.tableIndex, b.columnIndex));
  }
  addNodeRefs(plan, needed);
  return prune(plan, needed);
}

function prune(op: LogicalOperator, needed: Set<string>): LogicalOperator {
  switch (op.type) {
    case LogicalOperatorType.LOGICAL_GET:
      return pruneGet(op as LogicalGet, needed);
    case LogicalOperatorType.LOGICAL_PROJECTION:
      return pruneProjection(op as LogicalProjection, needed);
    case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY:
      return pruneAggregate(op as LogicalAggregate, needed);
    case LogicalOperatorType.LOGICAL_FILTER:
      return pruneFilter(op as LogicalFilter, needed);
    case LogicalOperatorType.LOGICAL_COMPARISON_JOIN:
      return pruneJoin(op as LogicalComparisonJoin, needed);
    case LogicalOperatorType.LOGICAL_CROSS_PRODUCT:
      return pruneCross(op as LogicalCrossProduct, needed);
    case LogicalOperatorType.LOGICAL_ORDER_BY:
      return pruneOrderBy(op as LogicalOrderBy, needed);
    case LogicalOperatorType.LOGICAL_LIMIT:
    case LogicalOperatorType.LOGICAL_DISTINCT:
      return prunePassthrough(op, needed);
    case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE:
      return pruneMaterializedCTE(op, needed);
    case LogicalOperatorType.LOGICAL_RECURSIVE_CTE:
      return pruneRecursiveCTE(op, needed);
    default:
      // DML, DDL, CTE ref, etc. — recurse but keep everything children provide
      for (let i = 0; i < op.children.length; i++) {
        const childNeeded = new Set<string>();
        collectAllRefs(op.children[i], childNeeded);
        op.children[i] = prune(op.children[i], childNeeded);
      }
      return op;
  }
}

// ============================================================================
// GET — prune to only needed columns
// ============================================================================

function pruneGet(op: LogicalGet, needed: Set<string>): LogicalGet {
  // Ensure columns referenced by pushed-down table filters are kept
  for (const filter of op.tableFilters) {
    for (const ref of collectColumnRefs(filter.expression)) {
      needed.add(bk(ref.tableIndex, ref.columnIndex));
    }
  }

  const keptIds: number[] = [];
  const keptTypes: typeof op.types = [];

  for (let i = 0; i < op.columnIds.length; i++) {
    if (needed.has(bk(op.tableIndex, op.columnIds[i]))) {
      keptIds.push(op.columnIds[i]);
      keptTypes.push(op.types[i]);
    }
  }

  if (keptIds.length === op.columnIds.length) return op;

  // Keep at least one column (for COUNT(*) etc.)
  if (keptIds.length === 0 && op.columnIds.length > 0) {
    keptIds.push(op.columnIds[0]);
    keptTypes.push(op.types[0]);
  }

  op.columnIds = keptIds;
  op.types = keptTypes;
  const ti = op.tableIndex;
  op.columnBindings = keptIds.map((ci) => ({
    tableIndex: ti,
    columnIndex: ci,
  }));
  return op;
}

// ============================================================================
// PROJECTION — keep outputs that are needed, then propagate child needs
//
// A projection output at index i is needed if:
//   (a) its own binding (projTableIndex:i) is in `needed`, OR
//   (b) it's a simple column ref whose binding is in `needed`
//       (handles ORDER BY/etc. referencing scan columns above projection)
// ============================================================================

function pruneProjection(
  op: LogicalProjection,
  needed: Set<string>,
): LogicalProjection {
  const keptExprs: BoundExpression[] = [];
  const keptAliases: (string | null)[] = [];
  const keptTypes: typeof op.types = [];

  for (let i = 0; i < op.expressions.length; i++) {
    const projKey = bk(op.tableIndex, i);
    let isNeeded = needed.has(projKey);

    // Also check: if expression is a column ref whose binding is needed
    if (!isNeeded) {
      const expr = op.expressions[i];
      if (isColumnRef(expr)) {
        const ref = expr as BoundColumnRefExpression;
        isNeeded = needed.has(
          bk(ref.binding.tableIndex, ref.binding.columnIndex),
        );
      }
    }

    if (isNeeded) {
      keptExprs.push(op.expressions[i]);
      keptAliases.push(op.aliases[i]);
      keptTypes.push(op.types[i]);
    }
  }

  if (keptExprs.length === 0 && op.expressions.length > 0) {
    keptExprs.push(op.expressions[0]);
    keptAliases.push(op.aliases[0]);
    keptTypes.push(op.types[0]);
  }

  op.expressions = keptExprs;
  op.aliases = keptAliases;
  op.types = keptTypes;

  // Child needs: collect refs from all kept expressions
  const childNeeded = new Set<string>();
  for (const expr of keptExprs) {
    addExprRefs(expr, childNeeded);
  }

  op.children = [prune(op.children[0], childNeeded)] as [LogicalOperator];

  const ti = op.tableIndex;
  const count = keptExprs.length;
  op.columnBindings = Array.from({ length: count }, (_, i) => ({
    tableIndex: ti,
    columnIndex: i,
  }));

  return op;
}

// ============================================================================
// AGGREGATE — prune unused aggregate expressions
// ============================================================================

function pruneAggregate(
  op: LogicalAggregate,
  needed: Set<string>,
): LogicalAggregate {
  const keptAggs: typeof op.expressions = [];
  const keptAggTypes: typeof op.types = [];
  const groupCount = op.groups.length;

  for (let i = 0; i < op.expressions.length; i++) {
    if (needed.has(bk(op.aggregateIndex, i))) {
      keptAggs.push(op.expressions[i]);
      keptAggTypes.push(op.types[groupCount + i]);
    }
  }

  op.expressions = keptAggs;
  op.types = [...op.types.slice(0, groupCount), ...keptAggTypes];

  // Child needs: group columns + aggregate input columns + having refs
  const childNeeded = new Set<string>();
  for (const g of op.groups) addExprRefs(g, childNeeded);
  for (const a of keptAggs) addExprRefs(a, childNeeded);
  if (op.havingExpression) addExprRefs(op.havingExpression, childNeeded);

  op.children = [prune(op.children[0], childNeeded)] as [LogicalOperator];

  // Rebuild bindings: groups (unchanged) + kept aggregates (re-indexed)
  op.columnBindings = [
    ...op.groups.map((_, i) => ({ tableIndex: op.groupIndex, columnIndex: i })),
    ...keptAggs.map((_, i) => ({ tableIndex: op.aggregateIndex, columnIndex: i })),
  ];

  return op;
}

// ============================================================================
// FILTER — adds filter expression refs, passes through
// ============================================================================

function pruneFilter(op: LogicalFilter, needed: Set<string>): LogicalFilter {
  const childNeeded = new Set(needed);
  for (const expr of op.expressions) addExprRefs(expr, childNeeded);
  op.children = [prune(op.children[0], childNeeded)] as [LogicalOperator];
  op.columnBindings = op.children[0].columnBindings;
  return op;
}

// ============================================================================
// JOIN — adds condition refs, passes to both sides
// ============================================================================

function pruneJoin(
  op: LogicalComparisonJoin,
  needed: Set<string>,
): LogicalComparisonJoin {
  const allNeeded = new Set(needed);
  for (const cond of op.conditions) {
    addExprRefs(cond.left, allNeeded);
    addExprRefs(cond.right, allNeeded);
  }
  op.children[0] = prune(op.children[0], allNeeded);
  op.children[1] = prune(op.children[1], allNeeded);
  op.columnBindings = [
    ...op.children[0].columnBindings,
    ...op.children[1].columnBindings,
  ];
  return op;
}

// ============================================================================
// CROSS PRODUCT — passes through
// ============================================================================

function pruneCross(
  op: LogicalCrossProduct,
  needed: Set<string>,
): LogicalCrossProduct {
  op.children[0] = prune(op.children[0], needed);
  op.children[1] = prune(op.children[1], needed);
  op.columnBindings = [
    ...op.children[0].columnBindings,
    ...op.children[1].columnBindings,
  ];
  return op;
}

// ============================================================================
// ORDER BY — adds sort expression refs
// ============================================================================

function pruneOrderBy(op: LogicalOrderBy, needed: Set<string>): LogicalOrderBy {
  const childNeeded = new Set(needed);
  for (const order of op.orders) addExprRefs(order.expression, childNeeded);
  op.children = [prune(op.children[0], childNeeded)] as [LogicalOperator];
  op.columnBindings = op.children[0].columnBindings;
  return op;
}

// ============================================================================
// PASSTHROUGH — LIMIT, DISTINCT
// ============================================================================

function prunePassthrough(
  op: LogicalOperator,
  needed: Set<string>,
): LogicalOperator {
  op.children[0] = prune(op.children[0], needed);
  op.columnBindings = op.children[0].columnBindings;
  return op;
}

// ============================================================================
// MATERIALIZED CTE — keep all columns in the CTE inner plan
//
// The CTE inner plan (children[0]) uses its own tableIndex values, which are
// different from the outer query's CTE ref tableIndex. We can't map between
// them here, so we keep all columns in the CTE inner plan to avoid pruning
// columns the outer query needs.
// ============================================================================

function pruneMaterializedCTE(
  op: LogicalOperator,
  needed: Set<string>,
): LogicalOperator {
  // children[0] = CTE inner plan: keep all columns (don't prune)
  const cteNeeded = new Set<string>();
  collectAllRefs(op.children[0], cteNeeded);
  for (const b of op.children[0].columnBindings) {
    cteNeeded.add(bk(b.tableIndex, b.columnIndex));
  }
  op.children[0] = prune(op.children[0], cteNeeded);

  // children[1] = outer query: prune normally
  const outerNeeded = new Set(needed);
  addNodeRefs(op, outerNeeded);
  op.children[1] = prune(op.children[1]!, outerNeeded);
  return op;
}

// ============================================================================
// RECURSIVE CTE — keep all columns in anchor and recursive children
//
// Both children must produce the same column set (anchor defines the schema,
// recursive must match). Pruning either side would break the CTE output.
// ============================================================================

function pruneRecursiveCTE(
  op: LogicalOperator,
  needed: Set<string>,
): LogicalOperator {
  for (let i = 0; i < op.children.length; i++) {
    const childNeeded = new Set<string>();
    collectAllRefs(op.children[i], childNeeded);
    for (const b of op.children[i].columnBindings) {
      childNeeded.add(bk(b.tableIndex, b.columnIndex));
    }
    op.children[i] = prune(op.children[i], childNeeded);
  }
  return op;
}

// ============================================================================
// Helpers
// ============================================================================

function bk(tableIndex: number, columnIndex: number): string {
  return `${tableIndex}:${columnIndex}`;
}

function addExprRefs(expr: BoundExpression, out: Set<string>): void {
  for (const ref of collectColumnRefs(expr)) {
    out.add(bk(ref.tableIndex, ref.columnIndex));
  }
}

/** Add all expression/condition/order refs from a single node (not recursive). */
function addNodeRefs(op: LogicalOperator, out: Set<string>): void {
  for (const expr of op.expressions) addExprRefs(expr, out);
  if ("conditions" in op && Array.isArray(op.conditions)) {
    for (const c of op.conditions) {
      addExprRefs(c.left, out);
      addExprRefs(c.right, out);
    }
  }
  if ("orders" in op && Array.isArray(op.orders)) {
    for (const o of op.orders) addExprRefs(o.expression, out);
  }
  if ("groups" in op && Array.isArray(op.groups)) {
    for (const g of op.groups) addExprRefs(g, out);
  }
  if ("havingExpression" in op && op.havingExpression) {
    addExprRefs(op.havingExpression, out);
  }
}

/** Collect all refs from an entire subtree (for default/DML cases). */
function collectAllRefs(op: LogicalOperator, out: Set<string>): void {
  addNodeRefs(op, out);
  for (const child of op.children) collectAllRefs(child, out);
}

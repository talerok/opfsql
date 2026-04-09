import type {
  LogicalOperator,
  LogicalFilter,
  LogicalProjection,
  LogicalComparisonJoin,
  LogicalCrossProduct,
  LogicalGet,
  LogicalAggregate,
  LogicalOrderBy,
  LogicalLimit,
  LogicalDistinct,
  LogicalUnion,
  BoundExpression,
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConstantExpression,
  JoinCondition,
  ComparisonType,
} from '../binder/types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../binder/types.js';
import {
  flattenConjunction,
  makeConjunction,
  getExpressionTables,
  getOperatorTables,
  isColumnRef,
  isConstant,
  isComparison,
  mapExpression,
  collectColumnRefs,
} from './utils/index.js';
import { FilterCombiner } from './filter_combiner.js';

// ============================================================================
// Filter Pushdown — pushes WHERE conditions as close to scan as possible
//
// Based on DuckDB's filter_pushdown.cpp:
// Dispatches by operator type. Collects filters and tries to push them
// through each operator down to the scan.
// ============================================================================

export function pushdownFilters(plan: LogicalOperator): LogicalOperator {
  const pushdown = new FilterPushdown();
  return pushdown.rewrite(plan);
}

class FilterPushdown {
  private filters: BoundExpression[] = [];

  rewrite(op: LogicalOperator): LogicalOperator {
    switch (op.type) {
      case LogicalOperatorType.LOGICAL_FILTER:
        return this.pushdownFilter(op as LogicalFilter);
      case LogicalOperatorType.LOGICAL_PROJECTION:
        return this.pushdownProjection(op as LogicalProjection);
      case LogicalOperatorType.LOGICAL_COMPARISON_JOIN:
        return this.pushdownJoin(op as LogicalComparisonJoin);
      case LogicalOperatorType.LOGICAL_CROSS_PRODUCT:
        return this.pushdownCrossProduct(op as LogicalCrossProduct);
      case LogicalOperatorType.LOGICAL_GET:
        return this.pushdownGet(op as LogicalGet);
      case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY:
        return this.pushdownAggregate(op as LogicalAggregate);
      case LogicalOperatorType.LOGICAL_ORDER_BY:
      case LogicalOperatorType.LOGICAL_LIMIT:
      case LogicalOperatorType.LOGICAL_DISTINCT:
        return this.pushdownPassthrough(op);
      case LogicalOperatorType.LOGICAL_UNION:
        return this.pushdownSetOperation(op as LogicalUnion);
      default:
        return this.finishPushdown(op);
    }
  }

  // ============================================================================
  // FILTER — extract conditions and push down through child
  // ============================================================================

  private pushdownFilter(op: LogicalFilter): LogicalOperator {
    // Collect all filter conditions
    for (const expr of op.expressions) {
      for (const cond of flattenConjunction(expr)) {
        this.filters.push(cond);
      }
    }
    // Push through child, removing the filter node
    return this.rewrite(op.children[0]);
  }

  // ============================================================================
  // PROJECTION — remap column refs and push through
  // ============================================================================

  private pushdownProjection(op: LogicalProjection): LogicalOperator {
    // Build a mapping from projection output columns to source expressions
    const pushable: BoundExpression[] = [];
    const remaining: BoundExpression[] = [];

    for (const filter of this.filters) {
      const remapped = remapThroughProjection(filter, op);
      if (remapped) {
        pushable.push(remapped);
      } else {
        remaining.push(filter);
      }
    }

    // Push remapped filters down
    const childPushdown = new FilterPushdown();
    childPushdown.filters = pushable;
    op.children = [childPushdown.rewrite(op.children[0])] as [LogicalOperator];

    // Keep non-remappable filters above
    this.filters = remaining;
    return this.finishPushdown(op);
  }

  // ============================================================================
  // JOIN — split filters by side and push to each
  // ============================================================================

  private pushdownJoin(op: LogicalComparisonJoin): LogicalOperator {
    const leftTables = getOperatorTables(op.children[0]);
    const rightTables = getOperatorTables(op.children[1]);

    const leftFilters: BoundExpression[] = [];
    const rightFilters: BoundExpression[] = [];
    const remaining: BoundExpression[] = [];

    for (const filter of this.filters) {
      const tables = getExpressionTables(filter);
      const touchesLeft = [...tables].some((t) => leftTables.has(t));
      const touchesRight = [...tables].some((t) => rightTables.has(t));

      if (touchesLeft && !touchesRight) {
        // Only references left side
        if (op.joinType === 'INNER' || op.joinType === 'LEFT') {
          leftFilters.push(filter);
        } else {
          remaining.push(filter);
        }
      } else if (touchesRight && !touchesLeft) {
        // Only references right side
        if (op.joinType === 'INNER') {
          rightFilters.push(filter);
        } else {
          // LEFT JOIN: can't push to right side (would filter out NULLs)
          remaining.push(filter);
        }
      } else if (touchesLeft && touchesRight && op.joinType === 'INNER') {
        // References both sides — try to convert to join condition
        const joinCond = tryExtractJoinCondition(filter);
        if (joinCond) {
          op.conditions.push(joinCond);
        } else {
          remaining.push(filter);
        }
      } else {
        remaining.push(filter);
      }
    }

    // Push filters to each side
    const leftPushdown = new FilterPushdown();
    leftPushdown.filters = leftFilters;
    op.children[0] = leftPushdown.rewrite(op.children[0]);

    const rightPushdown = new FilterPushdown();
    rightPushdown.filters = rightFilters;
    op.children[1] = rightPushdown.rewrite(op.children[1]);

    this.filters = remaining;
    return this.finishPushdown(op);
  }

  // ============================================================================
  // CROSS PRODUCT — split filters; convert to join if possible
  // ============================================================================

  private pushdownCrossProduct(op: LogicalCrossProduct): LogicalOperator {
    const leftTables = getOperatorTables(op.children[0]);
    const rightTables = getOperatorTables(op.children[1]);

    const leftFilters: BoundExpression[] = [];
    const rightFilters: BoundExpression[] = [];
    const joinConditions: JoinCondition[] = [];
    const remaining: BoundExpression[] = [];

    for (const filter of this.filters) {
      const tables = getExpressionTables(filter);
      const touchesLeft = [...tables].some((t) => leftTables.has(t));
      const touchesRight = [...tables].some((t) => rightTables.has(t));

      if (touchesLeft && !touchesRight) {
        leftFilters.push(filter);
      } else if (touchesRight && !touchesLeft) {
        rightFilters.push(filter);
      } else if (touchesLeft && touchesRight) {
        const joinCond = tryExtractJoinCondition(filter);
        if (joinCond) {
          joinConditions.push(joinCond);
        } else {
          remaining.push(filter);
        }
      } else {
        // References no tables (constant filter) — keep above
        remaining.push(filter);
      }
    }

    // Push single-side filters down
    const leftPushdown = new FilterPushdown();
    leftPushdown.filters = leftFilters;
    const newLeft = leftPushdown.rewrite(op.children[0]);

    const rightPushdown = new FilterPushdown();
    rightPushdown.filters = rightFilters;
    const newRight = rightPushdown.rewrite(op.children[1]);

    // If we found join conditions, convert to comparison join
    if (joinConditions.length > 0) {
      const join: LogicalComparisonJoin = {
        type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
        joinType: 'INNER',
        children: [newLeft, newRight],
        conditions: joinConditions,
        expressions: [],
        types: op.types,
        estimatedCardinality: op.estimatedCardinality,
        getColumnBindings: () => {
          const left = join.children[0].getColumnBindings();
          const right = join.children[1].getColumnBindings();
          return [...left, ...right];
        },
      };
      this.filters = remaining;
      return this.finishPushdown(join);
    }

    op.children = [newLeft, newRight];
    this.filters = remaining;
    return this.finishPushdown(op);
  }

  // ============================================================================
  // GET — convert filters to table filters on scan
  // ============================================================================

  private pushdownGet(op: LogicalGet): LogicalOperator {
    // Use FilterCombiner to optimize and extract table filters
    const combiner = new FilterCombiner();
    for (const filter of this.filters) {
      combiner.addFilter(filter);
    }

    // Add optimized table filters to the scan
    const tableFilters = combiner.generateTableFilters(op.tableIndex);
    op.tableFilters.push(...tableFilters);

    // Generate remaining filters that couldn't be pushed to scan
    const optimizedFilters = combiner.generateFilters();

    // Filter out conditions that are already covered by tableFilters
    const remaining = optimizedFilters.filter((f) => {
      if (!isComparison(f)) return true;
      const cmp = f as BoundComparisonExpression;
      if (!isColumnRef(cmp.left) || !isConstant(cmp.right)) return true;
      const ref = cmp.left as BoundColumnRefExpression;
      if (ref.binding.tableIndex !== op.tableIndex) return true;
      // Check if this exact filter is in tableFilters
      return !tableFilters.some(
        (tf) =>
          tf.columnIndex === ref.binding.columnIndex &&
          tf.comparisonType === cmp.comparisonType &&
          tf.constant.value === (cmp.right as BoundConstantExpression).value,
      );
    });

    this.filters = remaining;
    return this.finishPushdown(op);
  }

  // ============================================================================
  // AGGREGATE — push pre-aggregation filters down
  // ============================================================================

  private pushdownAggregate(op: LogicalAggregate): LogicalOperator {
    const pushable: BoundExpression[] = [];
    const remaining: BoundExpression[] = [];

    for (const filter of this.filters) {
      // Only push filters that reference group-by columns (not aggregates)
      if (referencesOnlyGroupColumns(filter, op)) {
        pushable.push(filter);
      } else {
        remaining.push(filter);
      }
    }

    const childPushdown = new FilterPushdown();
    childPushdown.filters = pushable;
    op.children = [childPushdown.rewrite(op.children[0])] as [LogicalOperator];

    this.filters = remaining;
    return this.finishPushdown(op);
  }

  // ============================================================================
  // PASSTHROUGH — ORDER BY, LIMIT, DISTINCT: push through
  // ============================================================================

  private pushdownPassthrough(op: LogicalOperator): LogicalOperator {
    const childPushdown = new FilterPushdown();
    childPushdown.filters = this.filters;
    this.filters = [];
    op.children[0] = childPushdown.rewrite(op.children[0]);
    return op;
  }

  // ============================================================================
  // SET OPERATION (UNION) — can push if filter applies to both sides
  // ============================================================================

  private pushdownSetOperation(op: LogicalUnion): LogicalOperator {
    // For UNION, we can't easily push filters down since columns are remapped.
    // Keep all filters above.
    return this.finishPushdown(op);
  }

  // ============================================================================
  // Finish — add remaining filters as a LogicalFilter above the operator
  // ============================================================================

  private finishPushdown(op: LogicalOperator): LogicalOperator {
    if (this.filters.length === 0) return op;

    const filterNode: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [op],
      expressions: this.filters,
      types: op.types,
      estimatedCardinality: op.estimatedCardinality,
      getColumnBindings: () => op.getColumnBindings(),
    };
    this.filters = [];
    return filterNode;
  }
}

// ============================================================================
// Helpers
// ============================================================================

/**
 * Remap a filter expression through a projection.
 * Returns null if the filter can't be remapped (references non-projected columns).
 */
function remapThroughProjection(
  filter: BoundExpression,
  proj: LogicalProjection,
): BoundExpression | null {
  let canRemap = true;

  const remapped = mapExpression(filter, (expr) => {
    if (expr.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF) {
      const ref = expr as BoundColumnRefExpression;
      if (ref.binding.tableIndex === proj.tableIndex) {
        // This references a projection output column — replace with the projection expression
        const idx = ref.binding.columnIndex;
        if (idx < proj.expressions.length) {
          return proj.expressions[idx];
        }
        canRemap = false;
      }
    }
    return expr;
  });

  return canRemap ? remapped : null;
}

/**
 * Try to extract a join condition from a comparison expression.
 * Returns null if the expression is not a simple comparison.
 */
function tryExtractJoinCondition(
  expr: BoundExpression,
): JoinCondition | null {
  if (!isComparison(expr)) return null;
  const cmp = expr as BoundComparisonExpression;
  return {
    left: cmp.left,
    right: cmp.right,
    comparisonType: cmp.comparisonType,
  };
}

/**
 * Check if a filter expression only references group-by columns of an aggregate
 * (not aggregate expressions).
 */
function referencesOnlyGroupColumns(
  filter: BoundExpression,
  agg: LogicalAggregate,
): boolean {
  const refs = collectColumnRefs(filter);
  // The child of the aggregate provides the columns.
  // Group columns are in agg.groups.
  // A filter is pushable if it only references columns that appear in group expressions.
  const groupRefs = new Set<string>();
  for (const group of agg.groups) {
    for (const ref of collectColumnRefs(group)) {
      groupRefs.add(`${ref.tableIndex}:${ref.columnIndex}`);
    }
  }

  return refs.every((r) => groupRefs.has(`${r.tableIndex}:${r.columnIndex}`));
}

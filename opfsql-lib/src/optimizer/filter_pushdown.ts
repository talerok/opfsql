import type {
  LogicalOperator,
  LogicalFilter,
  LogicalProjection,
  LogicalComparisonJoin,
  LogicalCrossProduct,
  LogicalGet,
  LogicalAggregate,
  LogicalUnion,
  BoundExpression,
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConstantExpression,
  BoundParameterExpression,
  JoinCondition,
} from '../binder/types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../binder/types.js';
import {
  flattenConjunction,
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
      case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE:
        return this.pushdownMaterializedCTE(op);
      case LogicalOperatorType.LOGICAL_RECURSIVE_CTE:
        return this.pushdownRecursiveCTE(op);
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
        // Only references left (probe) side — safe for all join types
        leftFilters.push(filter);
      } else if (touchesRight && !touchesLeft) {
        // Only references right (build) side
        if (op.joinType === 'INNER' || op.joinType === 'SEMI' || op.joinType === 'ANTI') {
          rightFilters.push(filter);
        } else {
          // LEFT JOIN: can't push to right side (would filter out NULLs)
          remaining.push(filter);
        }
      } else if (touchesLeft && touchesRight && op.joinType === 'INNER') {
        // References both sides — try to convert to equi-join condition
        const joinCond = tryExtractJoinCondition(filter, leftTables, rightTables);
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
        const joinCond = tryExtractJoinCondition(filter, leftTables, rightTables);
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
      if (!isColumnRef(cmp.left)) return true;
      const ref = cmp.left as BoundColumnRefExpression;
      if (ref.binding.tableIndex !== op.tableIndex) return true;
      // Check if this exact filter is already covered by a tableFilter
      return !tableFilters.some((tf) => {
        if (tf.columnIndex !== ref.binding.columnIndex) return false;
        if (tf.comparisonType !== cmp.comparisonType) return false;
        // For constants: compare values; for parameters: compare indices
        if (isConstant(tf.constant) && isConstant(cmp.right)) {
          return (tf.constant as BoundConstantExpression).value === (cmp.right as BoundConstantExpression).value;
        }
        if (tf.constant.expressionClass === BoundExpressionClass.BOUND_PARAMETER &&
            cmp.right.expressionClass === BoundExpressionClass.BOUND_PARAMETER) {
          return (tf.constant as BoundParameterExpression).index === (cmp.right as BoundParameterExpression).index;
        }
        return false;
      });
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

    // Try to push havingExpression below aggregate if it only references group columns.
    // The binder binds HAVING refs to groupIndex (aggregate output), so we remap them
    // to the corresponding group input expressions before pushing down.
    // For conjunctions like `HAVING group_col > 5 AND COUNT(*) > 10`, we split and
    // push only the group-column parts; the aggregate parts stay as havingExpression.
    if (op.havingExpression && op.groups.length > 0) {
      const havingParts = flattenConjunction(op.havingExpression);
      const keptParts: BoundExpression[] = [];

      for (const part of havingParts) {
        const remapped = remapHavingThroughGroups(part, op);
        if (remapped) {
          pushable.push(remapped);
        } else {
          keptParts.push(part);
        }
      }

      if (keptParts.length === 0) {
        op.havingExpression = null;
      } else if (keptParts.length === 1) {
        op.havingExpression = keptParts[0];
      } else {
        op.havingExpression = {
          expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
          conjunctionType: 'AND',
          children: keptParts,
          returnType: 'BOOLEAN',
        };
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
  // MATERIALIZED CTE — push filters into main plan (children[1]),
  // optimize CTE definition (children[0]) separately without filters
  // ============================================================================

  private pushdownMaterializedCTE(op: LogicalOperator): LogicalOperator {
    // children[0] = CTE definition, children[1] = main plan
    const defPushdown = new FilterPushdown();
    op.children[0] = defPushdown.rewrite(op.children[0]);

    const mainPushdown = new FilterPushdown();
    mainPushdown.filters = this.filters;
    this.filters = [];
    op.children[1] = mainPushdown.rewrite(op.children[1]!);

    return op;
  }

  // ============================================================================
  // RECURSIVE CTE — optimize anchor and recursive children independently,
  // don't push external filters into the CTE (like UNION)
  // ============================================================================

  private pushdownRecursiveCTE(op: LogicalOperator): LogicalOperator {
    const anchorPushdown = new FilterPushdown();
    op.children[0] = anchorPushdown.rewrite(op.children[0]);

    const recPushdown = new FilterPushdown();
    op.children[1] = recPushdown.rewrite(op.children[1]!);

    return this.finishPushdown(op);
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
 * Try to extract an equi-join condition from a comparison expression.
 * Only EQUAL comparisons can be used as hash-join keys.
 * Normalizes sides so cond.left references the left child and cond.right
 * references the right child.
 * Returns null if the expression is not a usable equi-join condition.
 */
function tryExtractJoinCondition(
  expr: BoundExpression,
  leftTables: Set<number>,
  rightTables: Set<number>,
): JoinCondition | null {
  if (!isComparison(expr)) return null;
  const cmp = expr as BoundComparisonExpression;

  // Only equality conditions can be used as hash-join keys
  if (cmp.comparisonType !== 'EQUAL') return null;

  const leftExprTables = getExpressionTables(cmp.left);
  const rightExprTables = getExpressionTables(cmp.right);

  const leftInLeft = [...leftExprTables].every((t) => leftTables.has(t));
  const rightInRight = [...rightExprTables].every((t) => rightTables.has(t));

  if (leftInLeft && rightInRight) {
    return { left: cmp.left, right: cmp.right, comparisonType: 'EQUAL' };
  }

  const leftInRight = [...leftExprTables].every((t) => rightTables.has(t));
  const rightInLeft = [...rightExprTables].every((t) => leftTables.has(t));

  if (leftInRight && rightInLeft) {
    return { left: cmp.right, right: cmp.left, comparisonType: 'EQUAL' };
  }

  // Mixed references — can't be used as a join condition
  return null;
}

/**
 * Remap a HAVING expression through aggregate group columns.
 * The binder binds HAVING column refs to groupIndex (the aggregate output table).
 * This function replaces those refs with the corresponding group input expressions,
 * allowing the filter to be pushed below the aggregate.
 * Returns null if the expression references aggregate results (not just group columns).
 */
function remapHavingThroughGroups(
  expr: BoundExpression,
  agg: LogicalAggregate,
): BoundExpression | null {
  let canRemap = true;

  const remapped = mapExpression(expr, (e) => {
    if (e.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF) {
      const ref = e as BoundColumnRefExpression;
      if (ref.binding.tableIndex === agg.groupIndex) {
        const idx = ref.binding.columnIndex;
        if (idx < agg.groups.length) {
          return agg.groups[idx];
        }
        // References beyond groups — this is an error or aggregate output
        canRemap = false;
      }
      if (ref.binding.tableIndex === agg.aggregateIndex) {
        // References an aggregate result — can't push below
        canRemap = false;
      }
    }
    if (e.expressionClass === BoundExpressionClass.BOUND_AGGREGATE) {
      // HAVING COUNT(*) > 5 — references aggregate, can't push
      canRemap = false;
    }
    return e;
  });

  return canRemap ? remapped : null;
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

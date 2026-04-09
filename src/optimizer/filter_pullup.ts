import type {
  LogicalOperator,
  LogicalFilter,
  LogicalComparisonJoin,
  LogicalCrossProduct,
  BoundExpression,
} from '../binder/types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../binder/types.js';
import { flattenConjunction, makeConjunction } from './utils/index.js';

// ============================================================================
// Filter Pullup — extracts filters from INNER JOIN conditions
//
// Based on DuckDB's filter_pullup.cpp:
// Converts INNER JOIN with conditions into CrossProduct + Filter above.
// This lets filter_pushdown re-optimize the placement of all conditions.
//
// Before:
//   Filter(a > 10)
//     └─ InnerJoin(x.id = y.id)
//          ├─ Scan(X)
//          └─ Scan(Y)
//
// After:
//   Filter(a > 10 AND x.id = y.id)
//     └─ CrossProduct
//          ├─ Scan(X)
//          └─ Scan(Y)
// ============================================================================

export function pullupFilters(plan: LogicalOperator): LogicalOperator {
  // Recurse into children first (bottom-up)
  for (let i = 0; i < plan.children.length; i++) {
    plan.children[i] = pullupFilters(plan.children[i]);
  }

  switch (plan.type) {
    case LogicalOperatorType.LOGICAL_FILTER:
      return pullupFromFilterChild(plan as LogicalFilter);
    default:
      return plan;
  }
}

function pullupFromFilterChild(filter: LogicalFilter): LogicalOperator {
  const child = filter.children[0];

  // Pull conditions from INNER JOIN up into the filter
  if (child.type === LogicalOperatorType.LOGICAL_COMPARISON_JOIN) {
    const join = child as LogicalComparisonJoin;
    if (join.joinType !== 'INNER') return filter;

    // Collect all filter expressions + join conditions
    const allFilters: BoundExpression[] = [...flattenConjunction(extractFilterExpr(filter))];

    for (const cond of join.conditions) {
      allFilters.push({
        expressionClass: BoundExpressionClass.BOUND_COMPARISON,
        comparisonType: cond.comparisonType,
        left: cond.left,
        right: cond.right,
        returnType: 'BOOLEAN',
      });
    }

    // Convert join to cross product
    const crossProduct: LogicalCrossProduct = {
      type: LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
      children: [join.children[0], join.children[1]],
      expressions: [],
      types: join.types,
      estimatedCardinality: join.estimatedCardinality,
      getColumnBindings: () => {
        const left = crossProduct.children[0].getColumnBindings();
        const right = crossProduct.children[1].getColumnBindings();
        return [...left, ...right];
      },
    };

    // Create new filter with all conditions
    const combined = makeConjunction(allFilters);
    if (!combined) return crossProduct;

    const newFilter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [crossProduct],
      expressions: flattenConjunction(combined),
      types: crossProduct.types,
      estimatedCardinality: filter.estimatedCardinality,
      getColumnBindings: () => crossProduct.getColumnBindings(),
    };

    return newFilter;
  }

  return filter;
}

function extractFilterExpr(filter: LogicalFilter): BoundExpression {
  if (filter.expressions.length === 1) return filter.expressions[0];
  return {
    expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
    conjunctionType: 'AND',
    children: filter.expressions,
    returnType: 'BOOLEAN',
  };
}

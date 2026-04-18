import type {
  BoundExpression,
  BoundConstantExpression,
  BoundParameterExpression,
  BoundComparisonExpression,
  ComparisonType,
  TableFilter,
} from '../binder/types.js';
import { BoundExpressionClass } from '../binder/types.js';
import {
  isConstant,
  isColumnRef,
  isComparison,
  flattenConjunction,
  makeBoolConstant,
  expressionKey,
  getExpressionTables,
} from './utils/index.js';

function isParameter(expr: BoundExpression): expr is BoundParameterExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_PARAMETER;
}

function isValueExpression(expr: BoundExpression): boolean {
  return !isConstant(expr) && !isParameter(expr);
}

// ============================================================================
// FilterCombiner — optimizes a set of AND-combined filter conditions
//
// Based on DuckDB's filter_combiner.cpp:
// - Manages equivalence sets (x = y → they're interchangeable)
// - Detects redundant filters (x > 5 AND x > 7 → x > 7)
// - Detects unsatisfiable filters (x = 5 AND x = 6 → false)
// - Generates transitive filters (x = y AND x = 5 → y = 5)
// - Converts to TableFilter[] for scan pushdown
// ============================================================================

interface ConstantComparison {
  comparisonType: ComparisonType;
  constant: BoundConstantExpression | BoundParameterExpression;
}

type FilterResult = 'KEEP' | 'PRUNE' | 'UNSATISFIABLE';

export class FilterCombiner {
  // Equivalence classes: expression key → set of equivalent expression keys
  private equivalences = new Map<string, Set<string>>();
  // Constant comparisons keyed by expression key
  private constantFilters = new Map<string, ConstantComparison[]>();
  // Original expressions for each key (for reconstructing filters)
  private keyExpressions = new Map<string, BoundExpression>();
  // Non-optimizable expressions
  private remainingFilters: BoundExpression[] = [];

  // ============================================================================
  // Add filters to the combiner
  // ============================================================================

  addFilter(expr: BoundExpression): void {
    const conditions = flattenConjunction(expr);
    for (const cond of conditions) {
      this.addSingleFilter(cond);
    }
  }

  private addSingleFilter(expr: BoundExpression): void {
    if (!isComparison(expr)) {
      this.remainingFilters.push(expr);
      return;
    }
    const cmp = expr as BoundComparisonExpression;

    // Expression = Expression → equivalence (only for column refs)
    if (
      cmp.comparisonType === 'EQUAL' &&
      isColumnRef(cmp.left) &&
      isColumnRef(cmp.right)
    ) {
      const leftKey = expressionKey(cmp.left);
      const rightKey = expressionKey(cmp.right);
      this.addEquivalence(leftKey, rightKey);
      this.keyExpressions.set(leftKey, cmp.left);
      this.keyExpressions.set(rightKey, cmp.right);
      this.remainingFilters.push(expr);
      return;
    }

    // Expression COMP Constant/Parameter → constant filter
    if (isValueExpression(cmp.left) && (isConstant(cmp.right) || isParameter(cmp.right))) {
      const key = expressionKey(cmp.left);
      this.keyExpressions.set(key, cmp.left);
      this.addConstantFilter(key, cmp.comparisonType, cmp.right as BoundConstantExpression | BoundParameterExpression);
      return;
    }

    // Constant/Parameter COMP Expression → flip and add
    if ((isConstant(cmp.left) || isParameter(cmp.left)) && isValueExpression(cmp.right)) {
      const key = expressionKey(cmp.right);
      this.keyExpressions.set(key, cmp.right);
      const flipped = flipComparisonType(cmp.comparisonType);
      this.addConstantFilter(key, flipped, cmp.left as BoundConstantExpression | BoundParameterExpression);
      return;
    }

    this.remainingFilters.push(expr);
  }

  // ============================================================================
  // Equivalence management
  // ============================================================================

  private addEquivalence(a: string, b: string): void {
    const setA = this.equivalences.get(a);
    const setB = this.equivalences.get(b);

    if (setA && setB) {
      for (const item of setB) {
        setA.add(item);
        this.equivalences.set(item, setA);
      }
    } else if (setA) {
      setA.add(b);
      this.equivalences.set(b, setA);
    } else if (setB) {
      setB.add(a);
      this.equivalences.set(a, setB);
    } else {
      const newSet = new Set([a, b]);
      this.equivalences.set(a, newSet);
      this.equivalences.set(b, newSet);
    }
  }

  // ============================================================================
  // Constant filter management with redundancy detection
  // ============================================================================

  private addConstantFilter(
    key: string,
    comparisonType: ComparisonType,
    constant: BoundConstantExpression | BoundParameterExpression,
  ): void {
    let existing = this.constantFilters.get(key);
    if (!existing) {
      existing = [];
      this.constantFilters.set(key, existing);
    }

    for (let i = existing.length - 1; i >= 0; i--) {
      const result = compareFilters(existing[i], { comparisonType, constant });
      if (result === 'UNSATISFIABLE') {
        existing.length = 0;
        existing.push(
          { comparisonType: 'EQUAL', constant: makeBoolConstant(false) as BoundConstantExpression },
        );
        return;
      }
      if (result === 'PRUNE') {
        existing.splice(i, 1);
      }
    }

    existing.push({ comparisonType, constant });
  }

  // ============================================================================
  // Generate optimized filters
  // ============================================================================

  generateFilters(): BoundExpression[] {
    const result: BoundExpression[] = [];

    for (const [key, filters] of this.constantFilters) {
      if (
        filters.length === 1 &&
        filters[0].comparisonType === 'EQUAL' &&
        isConstant(filters[0].constant) &&
        (filters[0].constant as BoundConstantExpression).value === false &&
        filters[0].constant.returnType === 'BOOLEAN'
      ) {
        return [makeBoolConstant(false)];
      }

      const leftExpr = this.keyExpressions.get(key);
      if (!leftExpr) continue;

      for (const filter of filters) {
        result.push({
          expressionClass: BoundExpressionClass.BOUND_COMPARISON,
          comparisonType: filter.comparisonType,
          left: leftExpr,
          right: filter.constant,
          returnType: 'BOOLEAN',
        });
      }
    }

    // Generate transitive filters (only for column refs with constant values)
    for (const [key, equivSet] of this.equivalences) {
      const filters = this.constantFilters.get(key);
      if (!filters) continue;

      for (const equivKey of equivSet) {
        if (equivKey === key) continue;
        if (this.constantFilters.has(equivKey)) continue;

        const equivExpr = this.keyExpressions.get(equivKey);
        if (!equivExpr) continue;

        for (const filter of filters) {
          if (!isConstant(filter.constant)) continue;
          result.push({
            expressionClass: BoundExpressionClass.BOUND_COMPARISON,
            comparisonType: filter.comparisonType,
            left: equivExpr,
            right: filter.constant,
            returnType: 'BOOLEAN',
          });
        }
      }
    }

    result.push(...this.remainingFilters);
    return result;
  }

  // ============================================================================
  // Generate TableFilter[] for scan pushdown
  // ============================================================================

  generateTableFilters(tableIndex: number): TableFilter[] {
    const result: TableFilter[] = [];

    for (const [key, filters] of this.constantFilters) {
      const expr = this.keyExpressions.get(key);
      if (!expr) continue;

      // Check if expression belongs to this table
      const tables = getExpressionTables(expr);
      if (!tables.has(tableIndex)) continue;

      for (const filter of filters) {
        if (isConstant(filter.constant) && filter.constant.returnType === 'BOOLEAN' && (filter.constant as BoundConstantExpression).value === false) {
          continue;
        }
        result.push({
          expression: expr,
          comparisonType: filter.comparisonType,
          constant: filter.constant,
        });
      }
    }

    return result;
  }
}

// ============================================================================
// Helpers
// ============================================================================

function flipComparisonType(type: ComparisonType): ComparisonType {
  switch (type) {
    case 'LESS':
      return 'GREATER';
    case 'GREATER':
      return 'LESS';
    case 'LESS_EQUAL':
      return 'GREATER_EQUAL';
    case 'GREATER_EQUAL':
      return 'LESS_EQUAL';
    case 'EQUAL':
      return 'EQUAL';
    case 'NOT_EQUAL':
      return 'NOT_EQUAL';
  }
}

function compareFilters(
  existing: ConstantComparison,
  incoming: ConstantComparison,
): FilterResult {
  if (!isConstant(existing.constant) || !isConstant(incoming.constant)) return 'KEEP';

  const eVal = (existing.constant as BoundConstantExpression).value;
  const iVal = (incoming.constant as BoundConstantExpression).value;

  if (typeof eVal !== 'number' || typeof iVal !== 'number') {
    if (existing.comparisonType === 'EQUAL' && incoming.comparisonType === 'EQUAL') {
      if (eVal === iVal) return 'PRUNE';
      return 'UNSATISFIABLE';
    }
    return 'KEEP';
  }

  if (existing.comparisonType === 'EQUAL' && incoming.comparisonType === 'EQUAL') {
    if (eVal === iVal) return 'PRUNE';
    return 'UNSATISFIABLE';
  }

  if (existing.comparisonType === 'EQUAL') {
    if (satisfiesComparison(eVal, incoming.comparisonType, iVal)) {
      return 'KEEP';
    }
    return 'UNSATISFIABLE';
  }

  if (incoming.comparisonType === 'EQUAL') {
    if (satisfiesComparison(iVal, existing.comparisonType, eVal)) {
      return 'PRUNE';
    }
    return 'UNSATISFIABLE';
  }

  if (isSameDirection(existing.comparisonType, incoming.comparisonType)) {
    if (incoming.comparisonType === 'LESS' || incoming.comparisonType === 'LESS_EQUAL') {
      if (iVal < eVal || (iVal === eVal && incoming.comparisonType === 'LESS')) {
        return 'PRUNE';
      }
      return 'KEEP';
    }
    if (iVal > eVal || (iVal === eVal && incoming.comparisonType === 'GREATER')) {
      return 'PRUNE';
    }
    return 'KEEP';
  }

  if (isLowerBound(existing.comparisonType) && isUpperBound(incoming.comparisonType)) {
    if (eVal > iVal) return 'UNSATISFIABLE';
    if (eVal === iVal) {
      if (existing.comparisonType === 'GREATER' || incoming.comparisonType === 'LESS') {
        return 'UNSATISFIABLE';
      }
    }
  }
  if (isUpperBound(existing.comparisonType) && isLowerBound(incoming.comparisonType)) {
    if (iVal > eVal) return 'UNSATISFIABLE';
    if (iVal === eVal) {
      if (incoming.comparisonType === 'GREATER' || existing.comparisonType === 'LESS') {
        return 'UNSATISFIABLE';
      }
    }
  }

  return 'KEEP';
}

function satisfiesComparison(value: number, type: ComparisonType, bound: number): boolean {
  switch (type) {
    case 'EQUAL':
      return value === bound;
    case 'NOT_EQUAL':
      return value !== bound;
    case 'LESS':
      return value < bound;
    case 'GREATER':
      return value > bound;
    case 'LESS_EQUAL':
      return value <= bound;
    case 'GREATER_EQUAL':
      return value >= bound;
  }
}

function isLowerBound(type: ComparisonType): boolean {
  return type === 'GREATER' || type === 'GREATER_EQUAL';
}

function isUpperBound(type: ComparisonType): boolean {
  return type === 'LESS' || type === 'LESS_EQUAL';
}

function isSameDirection(a: ComparisonType, b: ComparisonType): boolean {
  return (isLowerBound(a) && isLowerBound(b)) || (isUpperBound(a) && isUpperBound(b));
}

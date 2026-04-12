import type {
  BoundExpression,
  BoundColumnRefExpression,
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
} from './utils/index.js';

function isParameter(expr: BoundExpression): expr is BoundParameterExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_PARAMETER;
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
  // Equivalence classes: column binding key → set of equivalent column binding keys
  private equivalences = new Map<string, Set<string>>();
  // Constant comparisons for each column binding key
  private constantFilters = new Map<string, ConstantComparison[]>();
  // Non-column/non-comparison expressions we can't optimize further
  private remainingFilters: BoundExpression[] = [];

  // ============================================================================
  // Add filters to the combiner
  // ============================================================================

  addFilter(expr: BoundExpression): void {
    // Flatten AND conjunctions
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

    // Column = Column → equivalence
    if (
      cmp.comparisonType === 'EQUAL' &&
      isColumnRef(cmp.left) &&
      isColumnRef(cmp.right)
    ) {
      const leftKey = bindingKey(cmp.left as BoundColumnRefExpression);
      const rightKey = bindingKey(cmp.right as BoundColumnRefExpression);
      this.addEquivalence(leftKey, rightKey);
      this.remainingFilters.push(expr);
      return;
    }

    // Column COMP Constant/Parameter → constant filter
    if (isColumnRef(cmp.left) && (isConstant(cmp.right) || isParameter(cmp.right))) {
      const key = bindingKey(cmp.left as BoundColumnRefExpression);
      this.addConstantFilter(key, cmp.comparisonType, cmp.right as BoundConstantExpression | BoundParameterExpression);
      return;
    }

    // Constant/Parameter COMP Column → flip and add
    if ((isConstant(cmp.left) || isParameter(cmp.left)) && isColumnRef(cmp.right)) {
      const key = bindingKey(cmp.right as BoundColumnRefExpression);
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
      // Merge sets
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

    // Check against existing filters for this column
    for (let i = existing.length - 1; i >= 0; i--) {
      const result = compareFilters(existing[i], { comparisonType, constant });
      if (result === 'UNSATISFIABLE') {
        // Mark as unsatisfiable — will produce FALSE
        existing.length = 0;
        existing.push(
          { comparisonType: 'EQUAL', constant: makeBoolConstant(false) as BoundConstantExpression },
        );
        return;
      }
      if (result === 'PRUNE') {
        // New filter is more restrictive, remove old
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

    // Emit constant filters
    for (const [key, filters] of this.constantFilters) {
      // Check for unsatisfiable marker (only constants can be unsatisfiable markers)
      if (
        filters.length === 1 &&
        filters[0].comparisonType === 'EQUAL' &&
        isConstant(filters[0].constant) &&
        (filters[0].constant as BoundConstantExpression).value === false &&
        filters[0].constant.returnType === 'BOOLEAN'
      ) {
        return [makeBoolConstant(false)];
      }

      const [tableIndex, columnIndex] = key.split(':').map(Number);
      for (const filter of filters) {
        result.push({
          expressionClass: BoundExpressionClass.BOUND_COMPARISON,
          comparisonType: filter.comparisonType,
          left: {
            expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
            binding: { tableIndex, columnIndex },
            tableName: '',
            columnName: '',
            returnType: filter.constant.returnType,
          } as BoundColumnRefExpression,
          right: filter.constant,
          returnType: 'BOOLEAN',
        });
      }
    }

    // Generate transitive filters (only propagate constant values, not parameters)
    for (const [key, equivSet] of this.equivalences) {
      const filters = this.constantFilters.get(key);
      if (!filters) continue;

      for (const equivKey of equivSet) {
        if (equivKey === key) continue;
        if (this.constantFilters.has(equivKey)) continue; // Already has its own filters

        const [tableIndex, columnIndex] = equivKey.split(':').map(Number);
        for (const filter of filters) {
          // Don't propagate parameter filters transitively — value is unknown at optimize time
          if (!isConstant(filter.constant)) continue;
          result.push({
            expressionClass: BoundExpressionClass.BOUND_COMPARISON,
            comparisonType: filter.comparisonType,
            left: {
              expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
              binding: { tableIndex, columnIndex },
              tableName: '',
              columnName: '',
              returnType: filter.constant.returnType,
            } as BoundColumnRefExpression,
            right: filter.constant,
            returnType: 'BOOLEAN',
          });
        }
      }
    }

    // Add remaining (non-optimizable) filters
    result.push(...this.remainingFilters);
    return result;
  }

  // ============================================================================
  // Generate TableFilter[] for scan pushdown
  // ============================================================================

  generateTableFilters(tableIndex: number): TableFilter[] {
    const result: TableFilter[] = [];

    for (const [key, filters] of this.constantFilters) {
      const [tIdx, columnIndex] = key.split(':').map(Number);
      if (tIdx !== tableIndex) continue;

      for (const filter of filters) {
        if (isConstant(filter.constant) && filter.constant.returnType === 'BOOLEAN' && (filter.constant as BoundConstantExpression).value === false) {
          continue; // Skip unsatisfiable markers
        }
        result.push({
          columnIndex,
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

function bindingKey(ref: BoundColumnRefExpression): string {
  return `${ref.binding.tableIndex}:${ref.binding.columnIndex}`;
}

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

/**
 * Compare two constant filters on the same column.
 * Returns PRUNE if the existing filter is redundant (new one is tighter).
 * Returns UNSATISFIABLE if both together can't be true.
 * Returns KEEP if both are needed.
 */
function compareFilters(
  existing: ConstantComparison,
  incoming: ConstantComparison,
): FilterResult {
  // Can't compare at optimize time when either side is a runtime parameter
  if (!isConstant(existing.constant) || !isConstant(incoming.constant)) return 'KEEP';

  const eVal = (existing.constant as BoundConstantExpression).value;
  const iVal = (incoming.constant as BoundConstantExpression).value;

  // Can only compare numeric values
  if (typeof eVal !== 'number' || typeof iVal !== 'number') {
    // For EQUAL with same type: if equal values → prune, if different → unsatisfiable
    if (existing.comparisonType === 'EQUAL' && incoming.comparisonType === 'EQUAL') {
      if (eVal === iVal) return 'PRUNE';
      return 'UNSATISFIABLE';
    }
    return 'KEEP';
  }

  // Both EQUAL
  if (existing.comparisonType === 'EQUAL' && incoming.comparisonType === 'EQUAL') {
    if (eVal === iVal) return 'PRUNE';
    return 'UNSATISFIABLE';
  }

  // Existing is EQUAL — check if incoming is compatible
  if (existing.comparisonType === 'EQUAL') {
    if (satisfiesComparison(eVal, incoming.comparisonType, iVal)) {
      return 'KEEP'; // EQUAL is already more restrictive, keep both
    }
    return 'UNSATISFIABLE';
  }

  // Incoming is EQUAL — check if existing is compatible
  if (incoming.comparisonType === 'EQUAL') {
    if (satisfiesComparison(iVal, existing.comparisonType, eVal)) {
      return 'PRUNE'; // New EQUAL is more restrictive
    }
    return 'UNSATISFIABLE';
  }

  // Both are range comparisons on same direction
  // e.g., x > 5 AND x > 7 → PRUNE x > 5 (keep x > 7)
  if (isSameDirection(existing.comparisonType, incoming.comparisonType)) {
    if (incoming.comparisonType === 'LESS' || incoming.comparisonType === 'LESS_EQUAL') {
      // Both are upper bounds — keep the tighter (smaller) one
      if (iVal < eVal || (iVal === eVal && incoming.comparisonType === 'LESS')) {
        return 'PRUNE'; // Incoming is tighter
      }
      return 'KEEP';
    }
    // Both are lower bounds — keep the tighter (larger) one
    if (iVal > eVal || (iVal === eVal && incoming.comparisonType === 'GREATER')) {
      return 'PRUNE'; // Incoming is tighter
    }
    return 'KEEP';
  }

  // Opposite directions — check for unsatisfiable
  // x > 7 AND x < 5 → UNSATISFIABLE
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

import { compare } from "../../executor/evaluate/index.js";
import { compareValues, type Value } from "../../values/index.js";
import type {
  BoundComparisonExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundParameterExpression,
  ComparisonType,
  TableFilter,
} from "../binder/types.js";
import { BoundExpressionClass } from "../binder/types.js";
import {
  expressionKey,
  flattenConjunction,
  flipComparison,
  getExpressionTables,
  isComparison,
  isConstant,
  isParameter,
  makeBoolConstant,
} from "./utils/index.js";

// ============================================================================
// FilterCombiner — optimizes a set of AND-combined filter conditions
//
// Based on DuckDB's filter_combiner.cpp:
// - Manages equivalence sets (x = y → they're interchangeable)
// - Detects redundant filters (x > 5 AND x > 7 → x > 7)
// - Detects unsatisfiable filters (x = 5 AND x = 6 → false)
// - Generates transitive filters (x = y AND x = 5 → y = 5)
// - Converts to TableFilter[] for scan pushdown
//
// Constants are ordered by compareValues and tested by the executor's own
// comparison, so what the combiner proves matches what a scan would find.
// ============================================================================

type FilterValue = BoundConstantExpression | BoundParameterExpression;

interface ConstantComparison {
  comparisonType: ComparisonType;
  constant: FilterValue;
}

type FilterResult = "KEEP" | "PRUNE" | "UNSATISFIABLE";

export class FilterCombiner {
  // Equivalence classes: expression key → set of equivalent expression keys
  private readonly equivalences = new Map<string, Set<string>>();
  // Constant comparisons keyed by expression key
  private readonly constantFilters = new Map<string, ConstantComparison[]>();
  // Original expressions for each key (for reconstructing filters)
  private readonly keyExpressions = new Map<string, BoundExpression>();
  // Non-optimizable expressions
  private readonly remainingFilters: BoundExpression[] = [];
  // Some filters contradict each other: no row passes. Kept apart from the
  // filters, so that a user's `flag = false` is never mistaken for it.
  private isUnsatisfiable = false;

  private addSingleFilter(expression: BoundExpression): void {
    if (!isComparison(expression)) {
      this.remainingFilters.push(expression);
      return;
    }
    const { left, right, comparisonType } = expression;

    // Column = column → equivalence
    if (
      comparisonType === "EQUAL" &&
      isColumnReference(left) &&
      isColumnReference(right)
    ) {
      this.addEquivalence(this.keyOf(left), this.keyOf(right));
      this.remainingFilters.push(expression);
    } else if (isValueExpression(left) && isFilterValue(right)) {
      this.addConstantFilter(this.keyOf(left), comparisonType, right);
    } else if (isFilterValue(left) && isValueExpression(right)) {
      this.addConstantFilter(
        this.keyOf(right),
        flipComparison(comparisonType),
        left,
      );
    } else {
      this.remainingFilters.push(expression);
    }
  }

  private keyOf(expression: BoundExpression): string {
    const key = expressionKey(expression);
    this.keyExpressions.set(key, expression);
    return key;
  }

  private addEquivalence(a: string, b: string): void {
    const merged = this.equivalences.get(a) ?? new Set([a]);
    const others = this.equivalences.get(b) ?? [b];
    for (const item of others) merged.add(item);
    for (const item of merged) this.equivalences.set(item, merged);
  }

  private addConstantFilter(
    key: string,
    comparisonType: ComparisonType,
    constant: FilterValue,
  ): void {
    const existing = this.constantFilters.get(key) ?? [];
    this.constantFilters.set(key, existing);
    const incoming = { comparisonType, constant };
    // A value of unknown type is converted to each constant's type before
    // comparing (comparison affinity): x = 5 and x = '5' can both hold.
    if (this.keyExpressions.get(key)?.returnType === "ANY") {
      existing.push(incoming);
      return;
    }
    for (let index = existing.length - 1; index >= 0; index--) {
      const result = compareFilters(existing[index], incoming);
      if (result === "UNSATISFIABLE") {
        this.isUnsatisfiable = true;
        return;
      }
      if (result === "PRUNE") existing.splice(index, 1);
    }
    existing.push(incoming);
  }

  /** x = y AND x <op> C gives y <op> C (constants only, not parameters). */
  private transitiveFilters(): BoundExpression[] {
    const result: BoundExpression[] = [];
    for (const [key, equivalent] of this.equivalences) {
      const filters = (this.constantFilters.get(key) ?? []).filter((filter) =>
        isConstant(filter.constant),
      );
      for (const other of this.unfilteredEquivalents(key, equivalent)) {
        result.push(...filters.map((filter) => comparison(other, filter)));
      }
    }
    return result;
  }

  /** The expressions equivalent to `key` that have no constant filter. */
  private unfilteredEquivalents(
    key: string,
    equivalent: ReadonlySet<string>,
  ): BoundExpression[] {
    const result: BoundExpression[] = [];
    for (const other of equivalent) {
      const expression = this.keyExpressions.get(other);
      if (
        other !== key &&
        expression !== undefined &&
        !this.constantFilters.has(other)
      ) {
        result.push(expression);
      }
    }
    return result;
  }

  addFilter(expression: BoundExpression): void {
    for (const condition of flattenConjunction(expression)) {
      this.addSingleFilter(condition);
    }
  }

  generateFilters(): BoundExpression[] {
    if (this.isUnsatisfiable) return [makeBoolConstant(false)];
    const result: BoundExpression[] = [];
    for (const [key, filters] of this.constantFilters) {
      const left = this.keyExpressions.get(key);
      if (left === undefined) continue;
      for (const filter of filters) result.push(comparison(left, filter));
    }
    result.push(...this.transitiveFilters(), ...this.remainingFilters);
    return result;
  }

  generateTableFilters(tableIndex: number): TableFilter[] {
    if (this.isUnsatisfiable) return [];
    const result: TableFilter[] = [];
    for (const [key, filters] of this.constantFilters) {
      const expression = this.keyExpressions.get(key);
      if (
        expression === undefined ||
        !getExpressionTables(expression).has(tableIndex)
      ) {
        continue;
      }
      for (const filter of filters) {
        result.push({
          expression,
          comparisonType: filter.comparisonType,
          constant: filter.constant,
        });
      }
    }
    return result;
  }
}

function comparison(
  left: BoundExpression,
  filter: ConstantComparison,
): BoundComparisonExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COMPARISON,
    comparisonType: filter.comparisonType,
    left,
    right: filter.constant,
    returnType: "BOOLEAN",
  };
}

function isColumnReference(expression: BoundExpression): boolean {
  return expression.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF;
}

function isFilterValue(expression: BoundExpression): expression is FilterValue {
  return isConstant(expression) || isParameter(expression);
}

function isValueExpression(expression: BoundExpression): boolean {
  return !isFilterValue(expression);
}

// ============================================================================
// How two constant comparisons on the same expression combine
// ============================================================================

function compareFilters(
  existing: ConstantComparison,
  incoming: ConstantComparison,
): FilterResult {
  const existingValue = knownValue(existing.constant);
  const incomingValue = knownValue(incoming.constant);
  // Parameters are unknown until run time; NULL compares to nothing.
  if (existingValue === null || incomingValue === null) return "KEEP";
  const a = { type: existing.comparisonType, value: existingValue };
  const b = { type: incoming.comparisonType, value: incomingValue };
  if (a.type === "EQUAL" && b.type === "EQUAL") {
    return compareValues(a.value, b.value) === 0 ? "PRUNE" : "UNSATISFIABLE";
  }
  if (a.type === "EQUAL") return holds(a.value, b) ? "KEEP" : "UNSATISFIABLE";
  if (b.type === "EQUAL") return holds(b.value, a) ? "PRUNE" : "UNSATISFIABLE";
  return compareBounds(a, b);
}

interface Bound {
  type: ComparisonType;
  value: Exclude<Value, null>;
}

/** The constant's value; null for a parameter or a NULL constant. */
function knownValue(constant: FilterValue): Exclude<Value, null> | null {
  return isConstant(constant) ? constant.value : null;
}

/** `value <bound.type> bound.value` holds. */
function holds(value: Value, bound: Bound): boolean {
  return compare(bound.type)(value, bound.value) === true;
}

/** Two range bounds, neither an equality. */
function compareBounds(existing: Bound, incoming: Bound): FilterResult {
  if (isLowerBound(existing.type) && isLowerBound(incoming.type)) {
    return isTighter(incoming, existing, 1) ? "PRUNE" : "KEEP";
  }
  if (isUpperBound(existing.type) && isUpperBound(incoming.type)) {
    return isTighter(incoming, existing, -1) ? "PRUNE" : "KEEP";
  }
  const [lower, upper] = isLowerBound(existing.type)
    ? [existing, incoming]
    : [incoming, existing];
  return isLowerBound(lower.type) &&
    isUpperBound(upper.type) &&
    isEmptyRange(lower, upper)
    ? "UNSATISFIABLE"
    : "KEEP";
}

/**
 * `bound` excludes more than `other`, two bounds in the same direction:
 * further along it (`direction` 1 for lower bounds, -1 for upper), or at the
 * same value but strict.
 */
function isTighter(bound: Bound, other: Bound, direction: 1 | -1): boolean {
  const order = compareValues(bound.value, other.value) * direction;
  return order > 0 || (order === 0 && isStrict(bound.type));
}

/** No value is above `lower` and below `upper`. */
function isEmptyRange(lower: Bound, upper: Bound): boolean {
  const order = compareValues(lower.value, upper.value);
  return order === 0 ? isStrict(lower.type) || isStrict(upper.type) : order > 0;
}

function isStrict(type: ComparisonType): boolean {
  return type === "GREATER" || type === "LESS";
}

function isLowerBound(type: ComparisonType): boolean {
  return type === "GREATER" || type === "GREATER_EQUAL";
}

function isUpperBound(type: ComparisonType): boolean {
  return type === "LESS" || type === "LESS_EQUAL";
}

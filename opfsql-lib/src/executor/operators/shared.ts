import {
  type BoundConstantExpression,
  BoundExpressionClass,
  type BoundParameterExpression,
  type IndexScanHint,
  type IndexSearchPredicate,
  type TableFilter,
  type TableSchema,
} from "../../sql/binder/types.js";
import type { Value } from "../../values/index.js";
import {
  compareTyped,
  comparisonConversion,
  compileExpression,
  type ExecutionContext,
} from "../evaluate/index.js";
import type { Resolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";

export const SCAN_BATCH = 500;
export const JOIN_BATCH = 2000;

/** A row test compiled once per plan node. */
export type RowFilter = (row: Tuple) => boolean;

/** The value a table filter or index predicate compares with. */
export function resolveFilterValue(
  constant: BoundConstantExpression | BoundParameterExpression,
  context: ExecutionContext,
): Value {
  return constant.expressionClass === BoundExpressionClass.BOUND_CONSTANT
    ? constant.value
    : (context.parameters[constant.index] ?? null);
}

/** `expression <comparison> constant`, true only when it holds. */
export function compileTableFilter(
  filter: TableFilter,
  resolver: Resolver,
  context: ExecutionContext,
): RowFilter {
  const value = compileExpression(filter.expression, resolver, context);
  const constant = resolveFilterValue(filter.constant, context);
  const holds = compareTyped(
    filter.comparisonType,
    filter.expression.returnType,
    filter.constant.returnType,
  );
  return (row) => holds(value(row), constant) === true;
}

/**
 * The value an index predicate searches for, converted as a comparison
 * with the index column would convert it (comparison affinity).
 */
export function indexSearchValue(
  predicate: IndexSearchPredicate,
  index: IndexScanHint["indexDef"],
  context: ExecutionContext,
): Value {
  const columnType =
    index.expressions[predicate.columnPosition]?.returnType ?? "ANY";
  return comparisonConversion(
    predicate.value.returnType,
    columnType,
  )(resolveFilterValue(predicate.value, context));
}

export function compileTableFilters(
  filters: readonly TableFilter[],
  resolver: Resolver,
  context: ExecutionContext,
): RowFilter[] {
  return filters.map((filter) => compileTableFilter(filter, resolver, context));
}

export function passesFilters(
  row: Tuple,
  filters: readonly RowFilter[],
): boolean {
  return filters.every((filter) => filter(row));
}

export function rowToTuple(
  row: Readonly<Record<string, Value>>,
  columnIds: readonly number[],
  schema: TableSchema,
): Tuple {
  return columnIds.map((columnId) => {
    const column = schema.columns[columnId];
    // A column added after the row was written has its default.
    return Object.hasOwn(row, column.name)
      ? row[column.name]
      : column.defaultValue;
  });
}

export function drainOperator(operator: SyncPhysicalOperator): Tuple[] {
  const result: Tuple[] = [];
  for (let batch = operator.next(); batch !== null; batch = operator.next()) {
    for (const tuple of batch) result.push(tuple);
  }
  return result;
}

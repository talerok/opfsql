import type {
  IndexScanHint,
  IndexSearchPredicate,
  TableFilter,
  TableSchema,
} from "../../sql/binder/types.js";
import type { Value } from "../../values/index.js";
import {
  compareTyped,
  comparisonConversion,
  compileExpression,
  type ExecutionContext,
  fixedValue,
  required,
} from "../evaluate/index.js";
import type { Resolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";

export const SCAN_BATCH = 500;
export const JOIN_BATCH = 2000;

/** A row test compiled once per plan node. */
export type RowFilter = (row: Tuple) => boolean;

/** `expression <comparison> constant`, true only when it holds. */
export function compileTableFilter(
  filter: TableFilter,
  resolver: Resolver,
  context: ExecutionContext,
): RowFilter {
  const value = compileExpression(filter.expression, resolver, context);
  const constant = required(
    fixedValue(filter.constant, context),
    "the constant of a table filter",
  );
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
  const convert = comparisonConversion(predicate.value.returnType, columnType);
  return convert(
    required(fixedValue(predicate.value, context), "an index search value"),
  );
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

/** Every row an operator produces, pulled batch by batch as they are read. */
export function* rowsOf(operator: SyncPhysicalOperator): Generator<Tuple> {
  for (let batch = operator.next(); batch !== null; batch = operator.next()) {
    yield* batch;
  }
}

export function drainOperator(operator: SyncPhysicalOperator): Tuple[] {
  return [...rowsOf(operator)];
}

/** The next batch of up to `size` rows from `rows`; null when they end. */
export function takeBatch(rows: Iterator<Tuple>, size: number): Tuple[] | null {
  const batch: Tuple[] = [];
  for (let next = rows.next(); next.done !== true; next = rows.next()) {
    batch.push(next.value);
    if (batch.length === size) break;
  }
  return batch.length > 0 ? batch : null;
}

/**
 * The child's next batch that `transform` leaves rows in — an operator
 * never returns an empty batch — or null when the child is done.
 */
export function nextNonEmpty(
  child: SyncPhysicalOperator,
  transform: (batch: Tuple[]) => Tuple[],
): Tuple[] | null {
  for (let batch = child.next(); batch !== null; batch = child.next()) {
    const result = transform(batch);
    if (result.length > 0) return result;
  }
  return null;
}

import type { OrderDirection, physical } from "../../sql/plan/index.js";
import type { Scope, Tuple } from "../../expressions/index.js";
import type { ColumnReader, KeyBound, KeyRange } from "../../table/index.js";
import { compareValues, type Value } from "../../values/index.js";
import { type Context, fixedScope } from "../context.js";
import { leafOperator, type Operator } from "../operator.js";
import { type AtStart, compileEqual, compileRange, NO_ROW } from "./seeks.js";
import { tableAccess } from "./table-access.js";

// ---------------------------------------------------------------------------
// Rows by rowid: listed rowids, or a range of them. A value no rowid can
// equal — NULL, a fraction, a value of another kind — selects nothing from a
// list; a range bound of another kind still compares by the total order, so
// every rowid is above `false` and below 'a'.
// ---------------------------------------------------------------------------

/** The rowids a set selects in one run: some, in order and each once, or a range. */
export type RowidSelection =
  | { readonly kind: "list"; readonly rowids: readonly number[] }
  | { readonly kind: "range"; readonly first: number; readonly last: number };

/** A range end that lets every rowid through: from the first, or to the last. */
const OPEN_START = -Infinity;
const OPEN_END = Infinity;
/** A range end that lets no rowid through. */
const START_PAST_EVERY_ROWID = Infinity;
const END_BEFORE_EVERY_ROWID = -Infinity;

/**
 * Rows by rowid, each once, in rowid order or its reverse: read backward from
 * a bound, `WHERE id < ? ORDER BY id DESC LIMIT n` reads n rows.
 */
export function rowidLookup(
  plan: physical.RowidLookup,
  context: Context,
): Operator {
  const rowsFor = rowidLookupRows(plan, context, fixedScope(context));
  return leafOperator(() => rowsFor(NO_ROW), context.query.run);
}

/**
 * The lookup's rows for the row it starts for, its seek values compiled in
 * `seeks`: a lookup of its own starts for no row; a lookup join's right
 * scan starts for each left row.
 */
export function rowidLookupRows(
  plan: physical.RowidLookup,
  context: Context,
  seeks: Scope,
): (row: Tuple) => Generator<Tuple> {
  const { reader, matching } = tableAccess(plan, context);
  const selection = compileRowidSet(plan.rowids, seeks);
  const { direction } = plan;
  return (row) => matching(rowsSelected(selection(row), reader, direction));
}

/** The set's rowids, evaluated each time its scan starts. */
export function compileRowidSet(
  set: physical.RowidSet,
  seeks: Scope,
): AtStart<RowidSelection> {
  switch (set.kind) {
    case "list": {
      const values = compileEqual(set.values, seeks);
      return (row) => listOf(values(row));
    }
    case "range": {
      const range = compileRange(set.range, seeks);
      return (row) => rangeOf(range(row));
    }
  }
}

/** The rows the selection finds, as `reader` reads them, in rowid order or its reverse. */
export function* rowsSelected(
  selection: RowidSelection,
  reader: ColumnReader,
  direction: OrderDirection,
): Generator<Tuple> {
  if (selection.kind === "range") {
    yield* reader.between(selection.first, selection.last, direction);
    return;
  }
  const { rowids } = selection;
  const ordered = direction === "ascending" ? rowids : rowids.toReversed();
  for (const rowid of ordered) {
    const row = reader.at(rowid);
    if (row !== null) yield row;
  }
}

/** The rowids among `values`, each once and in order. */
function listOf(values: readonly Value[]): RowidSelection {
  const rowids = values.filter((value): value is number =>
    Number.isSafeInteger(value),
  );
  const distinct = [...new Set(rowids)].toSorted((a, b) => a - b);
  return { kind: "list", rowids: distinct };
}

/** The rowids from the first a range lets through to the last. */
function rangeOf({ lower, upper }: KeyRange): RowidSelection {
  return { kind: "range", first: firstRowid(lower), last: lastRowid(upper) };
}

/** The smallest rowid a lower bound lets through. */
function firstRowid(lower: KeyBound | null): number {
  if (lower === null) return OPEN_START;
  const { value, isInclusive } = lower;
  if (value === null) return START_PAST_EVERY_ROWID;
  if (typeof value !== "number") {
    return sortsBeforeNumbers(value) ? OPEN_START : START_PAST_EVERY_ROWID;
  }
  const isExactStart = isInclusive && Number.isSafeInteger(value);
  return isExactStart ? value : Math.floor(value) + 1;
}

/** The largest rowid an upper bound lets through. */
function lastRowid(upper: KeyBound | null): number {
  if (upper === null) return OPEN_END;
  const { value, isInclusive } = upper;
  if (value === null) return END_BEFORE_EVERY_ROWID;
  if (typeof value !== "number") {
    return sortsBeforeNumbers(value) ? END_BEFORE_EVERY_ROWID : OPEN_END;
  }
  const isExactEnd = isInclusive && Number.isSafeInteger(value);
  return isExactEnd ? value : Math.ceil(value) - 1;
}

/** True for a value of a kind the total order puts before every number. */
function sortsBeforeNumbers(value: Value): boolean {
  return compareValues(value, 0) < 0;
}

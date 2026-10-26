import type { physical } from "../../sql/plan/index.js";
import type {
  KeyBound,
  KeyRange,
  TableRow,
  TableRows,
} from "../../table/index.js";
import { compareValues, type Value } from "../../values/index.js";
import type { Context } from "../context.js";
import { leafOperator, type Operator } from "../operator.js";
import { compileEqual, compileRange } from "./seeks.js";
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

/** Rows by rowid, each once, in rowid order. */
export function rowidLookup(
  plan: physical.RowidLookup,
  context: Context,
): Operator {
  const { rows, matching } = tableAccess(plan, context);
  const selection = compileRowidSet(plan.rowids, context);
  return leafOperator(
    () => matching(rowsSelected(selection(), rows)),
    context.query.run,
  );
}

/** The set's rowids, evaluated each time its scan starts. */
export function compileRowidSet(
  set: physical.RowidSet,
  context: Context,
): () => RowidSelection {
  switch (set.kind) {
    case "list": {
      const values = compileEqual(set.values, context);
      return () => listOf(values());
    }
    case "range": {
      const range = compileRange(set.range, context);
      return () => rangeOf(range());
    }
  }
}

/** The rows the selection finds, in rowid order. */
export function* rowsSelected(
  selection: RowidSelection,
  rows: TableRows,
): Generator<TableRow> {
  if (selection.kind === "range") {
    yield* rows.rowsBetween(selection.first, selection.last);
    return;
  }
  for (const rowid of selection.rowids) {
    const row = rows.get(rowid);
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

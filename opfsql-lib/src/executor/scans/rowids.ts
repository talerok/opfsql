import type { physical } from "../../sql/plan/index.js";
import type { TableRow, TableRows } from "../../table/index.js";
import { compareValues, type Value } from "../../values/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { compileBound, compileSeek, type SeekBound } from "./seek.js";
import { tableAccess } from "./table-access.js";

// ---------------------------------------------------------------------------
// Rows by rowid: listed rowids, or a range of them. A rowid is an INTEGER,
// so seek values convert as `rowid = value` would, and a value that no
// integer can equal selects nothing — while a range bound of another kind
// still compares by the total order: every rowid is below 'a'.
// ---------------------------------------------------------------------------

/** The rowids a set selects in one run: some, in order and each once, or a range. */
export type RowidSelection =
  | { readonly kind: "list"; readonly rowids: readonly number[] }
  | { readonly kind: "range"; readonly first: number; readonly last: number };

const NO_ROWIDS: RowidSelection = { kind: "list", rowids: [] };

/** Rows by rowid, each once. */
export function rowidLookup(
  plan: physical.RowidLookup,
  context: Context,
): Operator {
  const { rows, matching } = tableAccess(plan, context);
  const selection = compileRowidSet(plan.rowids, context);
  return new WalkOperator(() => matching(rowsSelected(selection(), rows)), []);
}

/** The set's rowids, evaluated each time its scan starts. */
export function compileRowidSet(
  set: physical.RowidSet,
  context: Context,
): () => RowidSelection {
  switch (set.kind) {
    case "list": {
      const values = set.values.map((value) => compileSeek(value, context));
      return () => listOf(values.map((value) => value()));
    }
    case "range": {
      const lower = compileBound(set.range.lower, context);
      const upper = compileBound(set.range.upper, context);
      return () => rangeOf(firstRowid(lower()), lastRowid(upper()));
    }
  }
}

/** The rows the selection finds, in rowid order. */
export function* rowsSelected(
  selection: RowidSelection,
  rows: TableRows,
): Generator<TableRow> {
  if (selection.kind === "range") {
    yield* rowsInRange(selection.first, selection.last, rows);
    return;
  }
  for (const rowid of selection.rowids) {
    const row = rows.get(rowid);
    if (row !== null) yield row;
  }
}

function* rowsInRange(
  first: number,
  last: number,
  rows: TableRows,
): Generator<TableRow> {
  const from = first === -Infinity ? undefined : first;
  for (const row of rows.scan(from)) {
    if (row.rowid > last) return;
    yield row;
  }
}

/** The integers among `values`, each once and in order: no other value equals a rowid. */
function listOf(values: readonly Value[]): RowidSelection {
  const rowids = values.filter((value): value is number =>
    Number.isSafeInteger(value),
  );
  const distinct = [...new Set(rowids)].toSorted((a, b) => a - b);
  return { kind: "list", rowids: distinct };
}

function rangeOf(first: number, last: number): RowidSelection {
  const isEmpty = first > last || first === Infinity || last === -Infinity;
  return isEmpty ? NO_ROWIDS : { kind: "range", first, last };
}

/** The smallest rowid at or above a lower bound; Infinity when none can be. */
function firstRowid(bound: SeekBound | null): number {
  if (bound === null) return -Infinity;
  const { value, isInclusive } = bound;
  if (value === null) return Infinity;
  if (typeof value !== "number") {
    return sortsBeforeNumbers(value) ? -Infinity : Infinity;
  }
  const isExactStart = isInclusive && Number.isSafeInteger(value);
  return isExactStart ? value : Math.floor(value) + 1;
}

/** The largest rowid at or below an upper bound; -Infinity when none can be. */
function lastRowid(bound: SeekBound | null): number {
  if (bound === null) return Infinity;
  const { value, isInclusive } = bound;
  if (value === null) return -Infinity;
  if (typeof value !== "number") {
    return sortsBeforeNumbers(value) ? -Infinity : Infinity;
  }
  const isExactEnd = isInclusive && Number.isSafeInteger(value);
  return isExactEnd ? value : Math.ceil(value) - 1;
}

/** True for a value of a kind the total order puts before every number. */
function sortsBeforeNumbers(value: Value): boolean {
  return compareValues(value, 0) < 0;
}

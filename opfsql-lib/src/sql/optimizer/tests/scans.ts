import {
  compileExpression,
  compilePredicate,
  type Layout,
  type Run,
  type Tuple,
} from "../../../expressions/index.js";
import type { Index, Table } from "../../../schema/index.js";
import { compareValues, type Value } from "../../../values/index.js";
import { physical, ROWID_COLUMN } from "../../plan/index.js";
import type { KeyBinder } from "../index.js";
import { naive } from "./naive.js";

// The reference interpreter's scans, from the contract alone: a table in
// rowid order; rowids and index entries found by comparing values in the
// total order (NULL first), a NULL seek value matching nothing and a range
// never holding NULL; index entries ordered by their parts, then the rowid.

export interface StoredRow {
  readonly rowid: number;
  readonly values: readonly Value[];
}

/** Each table's rows, by table name. */
export type Tables = ReadonlyMap<string, readonly StoredRow[]>;

/** What running a plan needs: the rows, the run, the enclosing queries. */
export interface Context {
  readonly tables: Tables;
  readonly run: Run;
  readonly outerLayouts: readonly Layout[];
  readonly ctes: Map<number, readonly Tuple[]>;
  readonly bindKey: KeyBinder;
}

export type Scan =
  | physical.TableScan
  | physical.RowidLookup
  | physical.IndexScan
  | physical.IndexUnion;

/** An expression's value for a row of `layout`. */
export function compiled(
  expression: physical.Expression,
  layout: Layout,
  context: Context,
): (row: Tuple) => Value {
  const { outerLayouts, run } = context;
  return compileExpression(expression, { layout, outerLayouts, run });
}

/** A seek value or a limit: the same for the whole scan. */
export function fixed(
  expression: physical.Expression,
  context: Context,
): Value {
  return compiled(expression, [], context)([]);
}

/** The rows a scan returns, its residual checked. */
export function scanned(plan: Scan, context: Context): readonly Tuple[] {
  const { outerLayouts, run } = context;
  const scope = { layout: physical.layoutOf(plan), outerLayouts, run };
  const tests = plan.residual.map((condition) =>
    compilePredicate(condition, scope),
  );
  const outputs = rowsOfScan(plan, context).map((row) =>
    plan.columns.map((column) => valueOf(row, column)),
  );
  return outputs.filter((row) => tests.every((test) => test(row)));
}

function rowsOfScan(plan: Scan, context: Context): readonly StoredRow[] {
  switch (plan.kind) {
    case "table-scan": {
      const rows = rowsOf(plan.table, context);
      return plan.direction === "descending" ? rows.toReversed() : rows;
    }
    case "rowid-lookup": {
      return rowsById(plan.table, plan.rowids, context);
    }
    case "index-scan": {
      const entries = entriesIn(plan.table, plan.index, plan.keys, context);
      const rows = entries.map((entry) => entry.row);
      return plan.direction === "descending" ? rows.toReversed() : rows;
    }
    case "index-union": {
      return unionOf(plan, context);
    }
  }
}

/** A table's rows in rowid order. */
function rowsOf(table: Table, context: Context): StoredRow[] {
  const rows = context.tables.get(table.name) ?? [];
  return rows.toSorted((a, b) => a.rowid - b.rowid);
}

function valueOf(row: StoredRow, column: number): Value {
  return column === ROWID_COLUMN ? row.rowid : (row.values[column] ?? null);
}

/** Rows whose rowid equals a listed value or lies in the range: in rowid order, each once. */
function rowsById(
  table: Table,
  rowids: physical.RowidSet,
  context: Context,
): StoredRow[] {
  const rows = rowsOf(table, context);
  if (rowids.kind === "range") {
    const range = fixedRange(rowids.range, context);
    return rows.filter((row) => isWithin(row.rowid, range));
  }
  const wanted = rowids.values.map((value) => fixed(value, context));
  return rows.filter((row) =>
    wanted.some((value) => isSeekMatch(row.rowid, value)),
  );
}

/** `=` as a seek: NULL matches nothing. */
function isSeekMatch(stored: Value, value: Value): boolean {
  return value !== null && compareValues(stored, value) === 0;
}

interface FixedBound {
  readonly value: Value;
  readonly isInclusive: boolean;
}

interface FixedRange {
  readonly lower: FixedBound | null;
  readonly upper: FixedBound | null;
}

function fixedRange(range: physical.Range, context: Context): FixedRange {
  const bound = (end: physical.Bound | null): FixedBound | null => {
    if (end === null) return null;
    return { value: fixed(end.value, context), isInclusive: end.isInclusive };
  };
  return { lower: bound(range.lower), upper: bound(range.upper) };
}

/**
 * True if `value` lies in the range in the total order. A range holds only
 * values its comparisons hold for (physical.ts, Range): never NULL, and
 * nothing at all with a NULL bound.
 */
function isWithin(value: Value, range: FixedRange): boolean {
  const { lower, upper } = range;
  if (value === null) return false;
  if (lower !== null && !isBeyond(value, lower, 1)) return false;
  return upper === null || isBeyond(value, upper, -1);
}

/** True if `value` is above (`side` 1) or below (-1) `bound`, or on an inclusive one. */
function isBeyond(value: Value, bound: FixedBound, side: 1 | -1): boolean {
  if (bound.value === null) return false;
  const order = compareValues(value, bound.value) * side;
  return order > 0 || (order === 0 && bound.isInclusive);
}

interface Entry {
  readonly parts: readonly Value[];
  readonly row: StoredRow;
}

/** The index's entries within `keys`, in key order: the parts, then the rowid. */
function entriesIn(
  table: Table,
  index: Index,
  keys: physical.KeyRange,
  context: Context,
): Entry[] {
  const equal = keys.equal.map((value) => fixed(value, context));
  const range = keys.range === null ? null : fixedRange(keys.range, context);
  const isInRange = (entry: Entry): boolean => {
    const next = entry.parts[equal.length] ?? null;
    return range === null || isWithin(next, range);
  };
  return entriesOf(table, index, context).filter((entry) => {
    const isEqual = equal.every((value, position) =>
      isSeekMatch(entry.parts[position] ?? null, value),
    );
    return isEqual && isInRange(entry);
  });
}

function entriesOf(table: Table, index: Index, context: Context): Entry[] {
  const tableIndex = 0;
  const layout = table.columns.map((_column, columnIndex) => ({
    tableIndex,
    columnIndex,
  }));
  const parts = index.keys.map((key) => {
    if (key.kind === "column") {
      return (row: StoredRow): Value => row.values[key.column] ?? null;
    }
    const bound = naive(context.bindKey(key.sql, table, tableIndex));
    const part = compiled(bound, layout, context);
    return (row: StoredRow): Value => part(row.values);
  });
  const entries = rowsOf(table, context).map((row) => ({
    parts: parts.map((part) => part(row)),
    row,
  }));
  return entries.toSorted((a, b) => {
    const order = compareParts(a.parts, b.parts);
    return order === 0 ? a.row.rowid - b.row.rowid : order;
  });
}

function compareParts(a: readonly Value[], b: readonly Value[]): number {
  for (const [position, value] of a.entries()) {
    const order = compareValues(value, b[position] ?? null);
    if (order !== 0) return order;
  }
  return 0;
}

function unionOf(plan: physical.IndexUnion, context: Context): StoredRow[] {
  const found = new Map<number, StoredRow>();
  for (const branch of plan.branches) {
    for (const row of branchRows(plan.table, branch, context)) {
      found.set(row.rowid, row);
    }
  }
  return [...found.values()].toSorted((a, b) => a.rowid - b.rowid);
}

function branchRows(
  table: Table,
  branch: physical.IndexBranch,
  context: Context,
): StoredRow[] {
  if (branch.kind === "rowids") return rowsById(table, branch.rowids, context);
  const entries = entriesIn(table, branch.index, branch.keys, context);
  return entries.map((entry) => entry.row);
}

/** MIN or MAX of the part after `equal`, NULLs skipped. */
export function extremeOf(
  plan: physical.IndexExtreme,
  context: Context,
): Value {
  const keys = { equal: plan.equal, range: null };
  const values = entriesIn(plan.table, plan.index, keys, context)
    .map((entry) => entry.parts[plan.equal.length] ?? null)
    .filter((value) => value !== null);
  return (plan.extreme === "MIN" ? values[0] : values.at(-1)) ?? null;
}

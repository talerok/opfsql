import { InternalError } from "../../errors.js";
import type { Layout, Tuple } from "../../expressions/index.js";
import { physical, positionIn, ROWID_COLUMN } from "../../sql/plan/index.js";
import { compilePlan } from "../compile.js";
import type { Context } from "../context.js";
import { EVERY_ROW, pull } from "../operator.js";

/**
 * The rowids of an UPDATE's or DELETE's source rows, found by their binding
 * to `tableIndex` wherever the source puts it: every one read before any
 * row is changed, a batch of source rows at a time, keeping only the rowids.
 */
export function sourceRowids(
  source: physical.Plan,
  tableIndex: number,
  context: Context,
): number[] {
  const layout = physical.layoutOf(source);
  const position = positionOf(layout, tableIndex, ROWID_COLUMN);
  const rows = pull(compilePlan(source, context), EVERY_ROW);
  return Array.from(rows, (row) => rowidAt(row, position));
}

/** Where column `columnIndex` of table `tableIndex` is in a write's source rows. */
export function positionOf(
  layout: Layout,
  tableIndex: number,
  columnIndex: number,
): number {
  const position = positionIn(layout, { tableIndex, columnIndex });
  if (position === null) {
    const name = columnIndex === ROWID_COLUMN ? "rowid" : String(columnIndex);
    throw new InternalError(`a write's source has no column ${name}`);
  }
  return position;
}

/** The rowid at `position` of a source row. */
export function rowidAt(row: Tuple, position: number): number {
  const rowid = row[position];
  if (typeof rowid !== "number") {
    throw new InternalError("a write's source gave a row without a rowid");
  }
  return rowid;
}

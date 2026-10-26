import { InternalError } from "../../errors.js";
import type { Layout, Tuple } from "../../expressions/index.js";
import { positionIn, ROWID_COLUMN } from "../../sql/plan/index.js";

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

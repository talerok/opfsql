import { InternalError } from "../../errors.js";
import type { Index, Table } from "../../schema/index.js";
import { ROWID_COLUMN, type ColumnBinding } from "../../sql/plan/index.js";
import {
  type ConflictConstraint,
  keyedIndex,
  SEQUENCE_TABLE_NAME,
  Sequences,
  TableEditor,
} from "../../table/index.js";
import type { Layout, Tuple } from "../../expressions/index.js";
import type { WriteTarget } from "../target.js";

/**
 * The table's editor: every index of the table, keyed with the target's
 * expression compiler, and its AUTOINCREMENT counter if it has one.
 */
export function editorOf(table: Table, target: WriteTarget): TableEditor {
  const indexes = target.schema
    .indexesOf(table.name)
    .map((index) => keyedIndex(index, table, target.compileExpression));
  return new TableEditor(
    target.pages,
    table,
    indexes,
    sequencesOf(table, target),
  );
}

/**
 * The schema's own object for the constraint ON CONFLICT names, which the
 * editor's conflicts are: the plan's may be a copy.
 */
export function constraintOf(
  constraint: Index | "rowid" | null,
  target: WriteTarget,
): ConflictConstraint | null {
  if (constraint === null || constraint === "rowid") return constraint;
  const index = target.schema.index(constraint.name);
  if (index === null) {
    throw new InternalError(
      `ON CONFLICT names a missing index ${constraint.name}`,
    );
  }
  return index;
}

/** Where column `columnIndex` of table `tableIndex` is in a write's source rows. */
export function positionOf(
  layout: Layout,
  tableIndex: number,
  columnIndex: number,
): number {
  const position = layout.findIndex((binding) =>
    isColumn(binding, tableIndex, columnIndex),
  );
  if (position === -1) {
    const name = columnIndex === ROWID_COLUMN ? "rowid" : String(columnIndex);
    throw new InternalError(`a write's source has no column ${name}`);
  }
  return position;
}

function isColumn(
  binding: ColumnBinding,
  tableIndex: number,
  columnIndex: number,
): boolean {
  return (
    binding.tableIndex === tableIndex && binding.columnIndex === columnIndex
  );
}

function sequencesOf(table: Table, target: WriteTarget): Sequences | null {
  if (!table.isAutoincrement) return null;
  const sequences = target.schema.table(SEQUENCE_TABLE_NAME);
  if (sequences === null) {
    throw new InternalError(
      `${table.name} has AUTOINCREMENT, but there is no ${SEQUENCE_TABLE_NAME}`,
    );
  }
  return new Sequences(target.pages, sequences.rootPage);
}

/** The rowid at `position` of a source row. */
export function rowidAt(row: Tuple, position: number): number {
  const rowid = row[position];
  if (typeof rowid !== "number") {
    throw new InternalError("a write's source gave a row without a rowid");
  }
  return rowid;
}

import { InternalError } from "../errors.js";
import type { Table } from "../schema/index.js";
import type { PageReader, StoredRow } from "../storage/index.js";
import { TableReader } from "../storage/index.js";
import type { Value } from "../values/index.js";
import type { KeyOrder } from "./index-rows.js";

/**
 * A stored row as its table sees it: the rowid column holds the rowid, and
 * a column added after the row was written holds its default.
 */
export class TableRow {
  constructor(
    private readonly stored: StoredRow,
    private readonly table: Table,
  ) {}

  get rowid(): number {
    return this.stored.rowid;
  }

  value(column: number): Value {
    if (column === this.table.rowidColumn) return this.stored.rowid;
    if (column < this.stored.length) return this.stored.value(column);
    return this.defaultOf(column);
  }

  values(): Value[] {
    return this.table.columns.map((_, column) => this.value(column));
  }

  private defaultOf(column: number): Value {
    const definition = this.table.columns[column];
    if (definition === undefined) {
      throw new InternalError(
        `table ${this.table.name} has no column ${String(column)}`,
      );
    }
    return definition.defaultValue;
  }
}

/** The rows of a table, read at one snapshot. */
export class TableRows {
  protected readonly stored: TableReader;

  /** `stored` is for TableEditor, which reads through the rows it writes. */
  constructor(
    pages: PageReader,
    readonly table: Table,
    stored: TableReader = new TableReader(pages, table.rootPage),
  ) {
    this.stored = stored;
  }

  get(rowid: number): TableRow | null {
    const row = this.stored.get(rowid);
    return row === null ? null : new TableRow(row, this.table);
  }

  /** Rows in rowid order, from the first at or after `from`. */
  *scan(from?: number): Generator<TableRow> {
    for (const row of this.stored.scan(from)) {
      yield new TableRow(row, this.table);
    }
  }

  /**
   * Rows whose rowid is from `first` to `last`, both included, in rowid
   * order or its reverse: -Infinity and Infinity leave an end open.
   */
  *rowsBetween(
    first: number,
    last: number,
    order: KeyOrder = "ascending",
  ): Generator<TableRow> {
    const isEmpty = first === Infinity || last === -Infinity || first > last;
    if (isEmpty) return;
    if (order === "ascending") yield* this.upward(first, last);
    else yield* this.downward(first, last);
  }

  /** Rows in reverse rowid order, from the last below `before` (or the last). */
  *scanBackward(before?: number): Generator<TableRow> {
    for (const row of this.stored.scanBackward(before)) {
      yield new TableRow(row, this.table);
    }
  }

  /** Rows from `first` up to `last`. */
  private *upward(first: number, last: number): Generator<TableRow> {
    const from = first === -Infinity ? undefined : first;
    for (const row of this.scan(from)) {
      if (row.rowid > last) return;
      yield row;
    }
  }

  /** Rows from `last` down to `first`. */
  private *downward(first: number, last: number): Generator<TableRow> {
    const before = last === Infinity ? undefined : Math.floor(last) + 1;
    for (const row of this.scanBackward(before)) {
      if (row.rowid < first) return;
      yield row;
    }
  }
}

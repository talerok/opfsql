import { InternalError } from "../errors.js";
import { ROWID_COLUMN, type Table } from "../schema/index.js";
import type { PageReader, StoredRow, TupleLayout } from "../storage/index.js";
import { ROWID_FIELD, TableReader, tupleLayout } from "../storage/index.js";
import { isJsonComposite, type Value } from "../values/index.js";
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
    // A JSON object or array is copied, so that each row's is its own.
    const fallback = defaultOf(this.table, column);
    return isJsonComposite(fallback) ? structuredClone(fallback) : fallback;
  }

  values(): Value[] {
    return this.table.columns.map((_, column) => this.value(column));
  }
}

/**
 * Chosen columns of a table's rows, in a chosen order, as tuples decoded
 * straight from the records: what a scan reads.
 */
export class ColumnReader {
  private readonly layout: TupleLayout;

  /** `columns` are column numbers; ROWID_COLUMN is the rowid. */
  constructor(
    private readonly stored: TableReader,
    table: Table,
    columns: readonly number[],
  ) {
    const fields = columns.map((column) => fieldOf(table, column));
    const missing = columns.map((column) => defaultOf(table, column));
    this.layout = tupleLayout(fields, missing);
  }

  /**
   * The rows whose rowid is from `first` to `last`, both included, in rowid
   * order or its reverse; an infinite end leaves the range open.
   */
  between(
    first: number,
    last: number,
    order: KeyOrder = "ascending",
  ): Generator<Value[]> {
    return this.stored.tuples(this.layout, [first, last], order);
  }

  /** The row with `rowid`, or null if there is none. */
  at(rowid: number): Value[] | null {
    return this.stored.tupleAt(rowid, this.layout);
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

  has(rowid: number): boolean {
    return this.stored.has(rowid);
  }

  get(rowid: number): TableRow | null {
    const row = this.stored.get(rowid);
    return row === null ? null : new TableRow(row, this.table);
  }

  /** Reads `columns` of the rows, ROWID_COLUMN their rowid. */
  columns(columns: readonly number[]): ColumnReader {
    return new ColumnReader(this.stored, this.table, columns);
  }

  /** Rows in rowid order, from the first at or after `from`. */
  *scan(from?: number): Generator<TableRow> {
    for (const row of this.stored.scan(from)) {
      yield new TableRow(row, this.table);
    }
  }
}

/** The field a column is stored in: its own, or the rowid. */
function fieldOf(table: Table, column: number): number {
  const isRowid = column === ROWID_COLUMN || column === table.rowidColumn;
  return isRowid ? ROWID_FIELD : column;
}

/** What a column is in a row written before it was added: its default. */
function defaultOf(table: Table, column: number): Value {
  if (column === ROWID_COLUMN) return null;
  const definition = table.columns[column];
  if (definition === undefined) {
    throw new InternalError(
      `table ${table.name} has no column ${String(column)}`,
    );
  }
  return definition.defaultValue;
}

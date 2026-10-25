import { InternalError } from "../errors.js";
import type { Table } from "../schema/index.js";
import type { PageReader, StoredRow } from "../storage/index.js";
import { TableReader } from "../storage/index.js";
import type { Value } from "../values/index.js";

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
}

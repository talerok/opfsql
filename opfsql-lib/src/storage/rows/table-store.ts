import { RowTooLargeError } from "../../errors.js";
import type { Value } from "../../values/index.js";
import { BTree, BTreeWriter } from "../btree/btree.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import { tableLeafCell, tableLeafCellSize } from "../format/btree-page.js";
import type { PreparedField } from "../format/record.js";
import {
  decodeBytes,
  encodeRecord,
  OVERFLOW_REFERENCE_SIZE,
  OverflowValue,
  prepareField,
  RecordReader,
  recordSize,
} from "../format/record.js";
import { freeChain, readChain, writeChain } from "./overflow.js";

// ---------------------------------------------------------------------------
// The rows of a table: records keyed by rowid, with values too big for their
// row's cell moved to overflow pages (docs/file-format.md, Overflow values).
// ---------------------------------------------------------------------------

/** A stored row, decoded one column at a time. */
export class StoredRow {
  private readonly record: RecordReader;

  constructor(
    private readonly pages: PageReader,
    readonly rowid: number,
    record: Uint8Array,
  ) {
    // A copy: a writer's page may change after the row is read.
    this.record = new RecordReader(record.slice());
  }

  /** Columns stored; columns added to the table since read as their default. */
  get length(): number {
    return this.record.length;
  }

  value(column: number): Value {
    if (!this.record.isOverflow(column)) return this.record.value(column);
    const reference = this.record.overflow(column);
    return decodeBytes(reference.holds, readChain(this.pages, reference));
  }

  /** The values of this row that live in overflow chains. */
  overflowValues(): OverflowValue[] {
    const values: OverflowValue[] = [];
    for (let column = 0; column < this.record.length; column++) {
      if (this.record.isOverflow(column)) {
        values.push(this.record.overflow(column));
      }
    }
    return values;
  }
}

export class TableReader {
  /** `tree` is for TableWriter, which reads through the tree it writes. */
  constructor(
    protected readonly pages: PageReader,
    readonly root: number,
    protected readonly tree: BTree = new BTree(pages, root, "table"),
  ) {}

  get(rowid: number): StoredRow | null {
    const found = this.tree.find(rowid);
    if (found === null) return null;
    return new StoredRow(this.pages, rowid, found.page.record(found.index));
  }

  /** Rows in rowid order, from the first at or after `from`. */
  *scan(from?: number): Generator<StoredRow> {
    for (const { page, index } of this.tree.ascending(from)) {
      yield new StoredRow(this.pages, page.rowid(index), page.record(index));
    }
  }

  /** The largest rowid in use, or null if the table is empty. */
  lastRowid(): number | null {
    const last = this.tree.last();
    return last === null ? null : last.page.rowid(last.index);
  }
}

export class TableWriter extends TableReader {
  // The reader's pages and tree, typed for writing.
  declare protected readonly pages: PageWriter;
  declare protected readonly tree: BTreeWriter;

  /** `table` names the table in errors. */
  constructor(
    pages: PageWriter,
    root: number,
    private readonly table: string,
  ) {
    super(pages, root, new BTreeWriter(pages, root, "table"));
  }

  /** Allocates the root page of a new, empty table. */
  static create(pages: PageWriter): number {
    return BTreeWriter.create(pages, "table");
  }

  /** Stores a row. False, changing nothing, if the rowid is taken. */
  insert(rowid: number, values: readonly Value[]): boolean {
    // Checked first: encoding writes overflow chains, which a refused row
    // must not leave behind.
    if (this.tree.find(rowid) !== null) return false;
    return this.tree.insert(rowid, this.encodeCell(rowid, values));
  }

  /** Replaces a row. False, changing nothing, if there is none. */
  replace(rowid: number, values: readonly Value[]): boolean {
    const old = this.get(rowid);
    if (old === null) return false;
    this.tree.replace(rowid, this.encodeCell(rowid, values));
    this.freeChains(old);
    return true;
  }

  /** Removes a row. False if there is none. */
  delete(rowid: number): boolean {
    const old = this.get(rowid);
    if (old === null) return false;
    this.tree.delete(rowid);
    this.freeChains(old);
    return true;
  }

  /** Frees every page of the table, its overflow chains included. */
  drop(): void {
    for (const row of this.scan()) this.freeChains(row);
    this.tree.drop();
  }

  private freeChains(row: StoredRow): void {
    for (const value of row.overflowValues()) {
      freeChain(this.pages, value);
    }
  }

  /** The row's cell, with the values that do not fit in overflow chains. */
  private encodeCell(rowid: number, values: readonly Value[]): Uint8Array {
    const fields = values.map((value) => prepareField(value));
    for (const { column, field } of this.valuesToMove(rowid, fields)) {
      const firstPage = writeChain(this.pages, field.bytes);
      fields[column] = overflowField(field, firstPage);
    }
    return tableLeafCell(rowid, encodeRecord(fields));
  }

  /**
   * The values to move to overflow chains for the cell to fit: the largest
   * TEXT, BLOB and JSON values first, the earlier column on a tie. Decided
   * before any chain is written, so a row that cannot fit writes nothing.
   */
  private valuesToMove(
    rowid: number,
    fields: readonly PreparedField[],
  ): InlineColumn[] {
    const limit = this.tree.maxLeafCellSize();
    const planned = [...fields];
    const moves: InlineColumn[] = [];
    while (cellSize(rowid, planned) > limit) {
      const largest = largestMovable(planned);
      if (largest === null) {
        throw new RowTooLargeError(this.table, cellSize(rowid, planned), limit);
      }
      // The chain's page number does not change the cell's size.
      planned[largest.column] = overflowField(largest.field, 0);
      moves.push(largest);
    }
    return moves;
  }
}

type InlineBytes = Extract<PreparedField, { kind: "bytes" }>;

interface InlineColumn {
  readonly column: number;
  readonly field: InlineBytes;
}

function cellSize(rowid: number, fields: readonly PreparedField[]): number {
  return tableLeafCellSize(rowid, recordSize(fields));
}

function overflowField(field: InlineBytes, firstPage: number): PreparedField {
  const { holds, bytes } = field;
  return prepareField(new OverflowValue(holds, bytes.length, firstPage));
}

/**
 * The largest inline value that moving to a chain makes smaller, the earlier
 * on a tie; null if there is none.
 */
function largestMovable(fields: readonly PreparedField[]): InlineColumn | null {
  let largest: InlineColumn | null = null;
  for (const [column, field] of fields.entries()) {
    const sizeToBeat = largest?.field.bytes.length ?? OVERFLOW_REFERENCE_SIZE;
    if (field.kind === "bytes" && field.bytes.length > sizeToBeat) {
      largest = { column, field };
    }
  }
  return largest;
}

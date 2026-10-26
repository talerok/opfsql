import { InternalError, RowTooLargeError } from "../../errors.js";
import type { Value } from "../../values/index.js";
import { BTree, BTreeWriter } from "../btree/btree.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import { tableLeafCell, tableLeafCellSize } from "../format/btree-page.js";
import type {
  FieldPlaces,
  PreparedField,
  RecordField,
} from "../format/record.js";
import {
  decodeBytes,
  decodeInto,
  decodeRecord,
  encodeRecord,
  OVERFLOW_REFERENCE_SIZE,
  OverflowValue,
  prepareField,
  recordSize,
} from "../format/record.js";
import { freeChain, readChain, writeChain } from "./overflow.js";

// ---------------------------------------------------------------------------
// The rows of a table: records keyed by rowid, with values too big for their
// row's cell moved to overflow pages (docs/file-format.md, Overflow values).
// ---------------------------------------------------------------------------

/** Where a tuple's value comes from: the row's rowid, as opposed to a field. */
export const ROWID_FIELD = -1;

/** Rowid order, or its reverse. */
export type RowOrder = "ascending" | "descending";

/** How each row is read as a tuple: where each of its values comes from. */
export interface TupleLayout {
  /**
   * The values a tuple starts from: what each value is when the record
   * holds no such field, written before its column was added.
   */
  readonly defaults: readonly Value[];
  /** Where each field goes in the tuple: its first position. */
  readonly places: FieldPlaces;
  /** The positions of the rowid. */
  readonly rowids: readonly number[];
  /** Positions that repeat a field already placed: [from, to]. */
  readonly repeats: readonly (readonly [number, number])[];
}

/**
 * The layout of tuples whose values are `fields` — field numbers, or
 * ROWID_FIELD for the rowid — each its `defaults` value if a record lacks it.
 */
export function tupleLayout(
  fields: readonly number[],
  defaults: readonly Value[],
): TupleLayout {
  const places: (number | undefined)[] = [];
  const rowids: number[] = [];
  const repeats: [number, number][] = [];
  for (const [position, field] of fields.entries()) {
    if (field === ROWID_FIELD) {
      rowids.push(position);
      continue;
    }
    const placed = places[field];
    if (placed === undefined) places[field] = position;
    else repeats.push([placed, position]);
  }
  return { defaults, places, rowids, repeats };
}

/** A stored row, its fields decoded out of the page as it is read. */
export class StoredRow {
  private readonly fields: RecordField[];

  constructor(
    private readonly pages: PageReader,
    readonly rowid: number,
    record: Uint8Array,
  ) {
    this.fields = decodeRecord(record);
  }

  /** Columns stored; columns added to the table since read as their default. */
  get length(): number {
    return this.fields.length;
  }

  /** The value of a decoded field, read from its overflow chain if it has one. */
  value(column: number): Value {
    const field = this.fields[column];
    if (field instanceof OverflowValue) return overflowValue(this.pages, field);
    if (field === undefined) {
      throw new InternalError(
        `field ${String(column)} of row ${String(this.rowid)} was not decoded`,
      );
    }
    return field;
  }

  /** The values of this row that live in overflow chains. */
  overflowValues(): OverflowValue[] {
    return this.fields.filter((field) => field instanceof OverflowValue);
  }
}

export class TableReader {
  /** `tree` is for TableWriter, which reads through the tree it writes. */
  constructor(
    protected readonly pages: PageReader,
    readonly root: number,
    protected readonly tree: BTree = new BTree(pages, root, "table"),
  ) {}

  has(rowid: number): boolean {
    return this.tree.find(rowid) !== null;
  }

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

  /** The tuple of the row with `rowid`, or null if there is none. */
  tupleAt(rowid: number, layout: TupleLayout): Value[] | null {
    const found = this.tree.find(rowid);
    if (found === null) return null;
    return this.tupleOf(rowid, found.page.record(found.index), layout);
  }

  /**
   * The tuples of the rows whose rowid is from `first` to `last`, both
   * included, in rowid order or its reverse; an infinite end leaves the
   * range open. Each record is decoded where it lies, as its leaf is walked:
   * one generator for every row.
   */
  tuples(
    layout: TupleLayout,
    [first, last]: readonly [number, number],
    order: RowOrder,
  ): Generator<Value[]> {
    if (order === "ascending") return this.tuplesUp(layout, first, last);
    return this.tuplesDown(layout, first, last);
  }

  /** The largest rowid in use, or null if the table is empty. */
  lastRowid(): number | null {
    const last = this.tree.last();
    return last === null ? null : last.page.rowid(last.index);
  }

  private *tuplesUp(
    layout: TupleLayout,
    first: number,
    last: number,
  ): Generator<Value[]> {
    if (first > last) return;
    const from = first === -Infinity ? undefined : first;
    for (const { page, index } of this.tree.leavesFrom(from)) {
      for (let cell = index; cell < page.cellCount; cell++) {
        const rowid = page.rowid(cell);
        if (rowid > last) return;
        yield this.tupleOf(rowid, page.record(cell), layout);
      }
    }
  }

  private *tuplesDown(
    layout: TupleLayout,
    first: number,
    last: number,
  ): Generator<Value[]> {
    if (first > last) return;
    const before = last === Infinity ? undefined : Math.floor(last) + 1;
    for (const { page, index } of this.tree.leavesBefore(before)) {
      for (let cell = index; cell >= 0; cell--) {
        const rowid = page.rowid(cell);
        if (rowid < first) return;
        yield this.tupleOf(rowid, page.record(cell), layout);
      }
    }
  }

  private tupleOf(
    rowid: number,
    record: Uint8Array,
    layout: TupleLayout,
  ): Value[] {
    // Defaults first: a field the record lacks keeps its column's default.
    const tuple: RecordField[] = [...layout.defaults];
    const hasOverflow = decodeInto(record, layout.places, tuple);
    for (const position of layout.rowids) tuple[position] = rowid;
    for (const [from, to] of layout.repeats) tuple[to] = tuple[from] ?? null;
    // Without an overflow reference among them, every field is a Value.
    if (!hasOverflow) return tuple as Value[];
    return tuple.map((value) =>
      value instanceof OverflowValue ? overflowValue(this.pages, value) : value,
    );
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

/** The value whose bytes an overflow chain holds. */
function overflowValue(pages: PageReader, reference: OverflowValue): Value {
  return decodeBytes(reference.holds, readChain(pages, reference));
}

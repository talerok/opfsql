import { CorruptError, InternalError } from "../../errors.js";
import { compareBytes } from "../../values/index.js";
import {
  ByteWriter,
  readUint32,
  readVarint,
  skipVarint,
  unzigzag,
  varintSize,
  viewOf,
  writeUint32,
  zigzag,
} from "./bytes.js";

// ---------------------------------------------------------------------------
// B-tree pages (docs/file-format.md, B-tree pages): a header, an array of
// cell pointers in key order, and cells packed against the end of the page
// with no gaps, so the free space is one zeroed run in the middle.
//
// BTreePage reads a page in place. Pages from the page store are shared with
// snapshots, so only BTreePageEditor changes bytes, and only on the private
// copy a writer gets for a transaction.
// ---------------------------------------------------------------------------

export type BTreePageType =
  "table-interior" | "table-leaf" | "index-interior" | "index-leaf";

const TYPE_BYTE: Record<BTreePageType, number> = {
  "table-interior": 0x01,
  "table-leaf": 0x02,
  "index-interior": 0x03,
  "index-leaf": 0x04,
};

const TYPE_OF_BYTE: Readonly<Partial<Record<number, BTreePageType>>> = {
  0x01: "table-interior",
  0x02: "table-leaf",
  0x03: "index-interior",
  0x04: "index-leaf",
};

const CELL_COUNT_OFFSET = 2;
const CONTENT_START_OFFSET = 4;
const RIGHT_CHILD_OFFSET = 8;
const LEAF_HEADER_SIZE = 8;
const INTERIOR_HEADER_SIZE = 12;
const POINTER_SIZE = 2;
const CHILD_SIZE = 4;

export function headerSize(type: BTreePageType): number {
  return isInterior(type) ? INTERIOR_HEADER_SIZE : LEAF_HEADER_SIZE;
}

export function isInterior(type: BTreePageType): boolean {
  return type === "table-interior" || type === "index-interior";
}

/**
 * Largest cell a page of `type` accepts: a cell and its pointer take at most
 * a quarter of the space after the header.
 */
export function maxCellSize(type: BTreePageType, pageSize: number): number {
  return Math.floor((pageSize - headerSize(type)) / 4) - POINTER_SIZE;
}

/**
 * Longest index key: one whose interior cell — left child, length, key —
 * fits. Leaf cells, with a smaller header and no child, fit too.
 */
export function maxIndexKeySize(pageSize: number): number {
  const cell = maxCellSize("index-interior", pageSize);
  return cell - CHILD_SIZE - varintSize(cell);
}

/** Bytes a cell takes in a page, its pointer included. */
export function cellFootprint(cell: Uint8Array): number {
  return cell.length + POINTER_SIZE;
}

/** Bytes of a table leaf cell holding `rowid` and a record of `recordLength` bytes. */
export function tableLeafCellSize(rowid: number, recordLength: number): number {
  return varintSize(zigzag(rowid)) + varintSize(recordLength) + recordLength;
}

// --- Cells -------------------------------------------------------------------

export function tableLeafCell(rowid: number, record: Uint8Array): Uint8Array {
  const writer = new ByteWriter(record.length + 16);
  writer.varint(zigzag(rowid));
  writer.varint(record.length);
  writer.bytes(record);
  return writer.toBytes();
}

export function tableInteriorCell(child: number, rowid: number): Uint8Array {
  const writer = new ByteWriter(16);
  writer.uint32(child);
  writer.varint(zigzag(rowid));
  return writer.toBytes();
}

export function indexLeafCell(key: Uint8Array): Uint8Array {
  const writer = new ByteWriter(key.length + 8);
  writer.varint(key.length);
  writer.bytes(key);
  return writer.toBytes();
}

export function indexInteriorCell(child: number, key: Uint8Array): Uint8Array {
  const writer = new ByteWriter(key.length + 12);
  writer.uint32(child);
  writer.varint(key.length);
  writer.bytes(key);
  return writer.toBytes();
}

/** The key of a leaf cell: its rowid in a table, its encoded key in an index. */
export function leafCellKey(
  type: BTreePageType,
  cell: Uint8Array,
): number | Uint8Array {
  if (type === "table-leaf") return unzigzag(readVarint(cell, 0));
  const start = skipVarint(cell, 0);
  return new Uint8Array(cell.subarray(start, start + readVarint(cell, 0)));
}

/** The key of an interior cell. */
export function interiorCellKey(
  type: BTreePageType,
  cell: Uint8Array,
): number | Uint8Array {
  if (type === "table-interior") return unzigzag(readVarint(cell, CHILD_SIZE));
  const start = skipVarint(cell, CHILD_SIZE);
  return new Uint8Array(
    cell.subarray(start, start + readVarint(cell, CHILD_SIZE)),
  );
}

/** The left child of an interior cell. */
export function interiorCellChild(cell: Uint8Array): number {
  return readUint32(cell, 0);
}

/** A copy of an interior cell pointing at another left child. */
export function withInteriorChild(cell: Uint8Array, child: number): Uint8Array {
  const copy = new Uint8Array(cell);
  writeUint32(copy, 0, child);
  return copy;
}

// --- Reading -----------------------------------------------------------------

export class BTreePage {
  readonly type: BTreePageType;
  protected readonly view: DataView;

  constructor(readonly bytes: Uint8Array) {
    this.view = viewOf(bytes);
    const type = TYPE_OF_BYTE[bytes[0] ?? 0];
    if (type === undefined) {
      throw new CorruptError(
        `page type ${String(bytes[0])} is not a B-tree page`,
      );
    }
    this.type = type;
  }

  get isLeaf(): boolean {
    return !isInterior(this.type);
  }

  get cellCount(): number {
    return this.view.getUint16(CELL_COUNT_OFFSET);
  }

  /**
   * Offset of the first byte of the cell area. It is stored in 16 bits, so
   * an empty 64 KiB page stores its end, 65536, as 0.
   */
  get contentStart(): number {
    const start = this.view.getUint16(CONTENT_START_OFFSET);
    return start === 0 ? this.bytes.length : start;
  }

  get rightChild(): number {
    this.requireInterior();
    return this.view.getUint32(RIGHT_CHILD_OFFSET);
  }

  /** Bytes between the last pointer and the cell area. */
  get freeSpace(): number {
    return this.contentStart - this.pointersEnd;
  }

  protected get pointersEnd(): number {
    return headerSize(this.type) + this.cellCount * POINTER_SIZE;
  }

  cellOffset(index: number): number {
    return this.view.getUint16(headerSize(this.type) + index * POINTER_SIZE);
  }

  /** The cell at `index` with every byte of it. */
  cell(index: number): Uint8Array {
    const offset = this.cellOffset(index);
    return this.bytes.subarray(offset, offset + this.cellSizeAt(offset));
  }

  /** Child page of an interior page: cell `index`'s left child, or the rightmost. */
  child(index: number): number {
    this.requireInterior();
    if (index === this.cellCount) return this.rightChild;
    return this.view.getUint32(this.cellOffset(index));
  }

  rowid(index: number): number {
    return unzigzag(readVarint(this.bytes, this.keyOffset(index)));
  }

  /** The record of a table leaf cell: after the rowid. */
  record(index: number): Uint8Array {
    return this.sizedBytes(skipVarint(this.bytes, this.cellOffset(index)));
  }

  /** The key of an index cell. */
  key(index: number): Uint8Array {
    return this.sizedBytes(this.keyOffset(index));
  }

  /**
   * The first cell whose key is at least `key` (a rowid in a table page, an
   * encoded key in an index page): where `key` is, or would go.
   */
  lowerBound(key: number | Uint8Array): number {
    return this.firstCellWhere((index) => this.compareCell(index, key) >= 0);
  }

  /** Which child of an interior page holds `key`: an index for child(). */
  childIndexFor(key: number | Uint8Array): number {
    return this.firstCellWhere((index) => this.compareCell(index, key) > 0);
  }

  /** True if `cell` and its pointer fit in the free space. */
  fits(cell: Uint8Array): boolean {
    return cellFootprint(cell) <= this.freeSpace;
  }

  /** True if the cell at `index` has exactly `key`. */
  holds(index: number, key: number | Uint8Array): boolean {
    return index < this.cellCount && this.compareCell(index, key) === 0;
  }

  /** Negative, zero or positive as cell `index`'s key is below, at or above `key`. */
  compareCell(index: number, key: number | Uint8Array): number {
    if (typeof key === "number") return this.rowid(index) - key;
    return compareBytes(this.key(index), key);
  }

  /**
   * What is wrong with the page's layout: a cell area overlapping the
   * pointers, cells that overlap or leave gaps, free space that is not zero.
   */
  layoutProblems(): string[] {
    const start = this.contentStart;
    if (start < this.pointersEnd || start > this.bytes.length) {
      return [
        `cell area starts at ${String(start)}, inside the header or pointers`,
      ];
    }
    const problems = this.packingProblems();
    const free = this.bytes.subarray(this.pointersEnd, start);
    if (free.some((byte) => byte !== 0)) {
      problems.push("free space is not zero");
    }
    return problems;
  }

  protected cellSizeAt(offset: number): number {
    switch (this.type) {
      case "table-leaf": {
        return this.sizedEnd(skipVarint(this.bytes, offset)) - offset;
      }
      case "table-interior": {
        return skipVarint(this.bytes, offset + CHILD_SIZE) - offset;
      }
      case "index-leaf": {
        return this.sizedEnd(offset) - offset;
      }
      case "index-interior": {
        return this.sizedEnd(offset + CHILD_SIZE) - offset;
      }
    }
  }

  /** Where a cell's key starts: past the left child in an interior page. */
  private keyOffset(index: number): number {
    return this.cellOffset(index) + (this.isLeaf ? 0 : CHILD_SIZE);
  }

  /** The bytes after the varint at `offset` that gives their length. */
  private sizedBytes(offset: number): Uint8Array {
    const start = skipVarint(this.bytes, offset);
    return this.bytes.subarray(start, this.sizedEnd(offset));
  }

  private sizedEnd(offset: number): number {
    return skipVarint(this.bytes, offset) + readVarint(this.bytes, offset);
  }

  /** The first cell for which `isPast` holds — false, then true — or the cell count. */
  private firstCellWhere(isPast: (index: number) => boolean): number {
    let low = 0;
    let high = this.cellCount;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (isPast(middle)) high = middle;
      else low = middle + 1;
    }
    return low;
  }

  /** Cells must fill the area from its start to the end of the page. */
  private packingProblems(): string[] {
    const problems: string[] = [];
    let expected = this.contentStart;
    for (const { offset, end } of this.cellExtents()) {
      if (offset !== expected) {
        problems.push(
          `cell at ${String(offset)} should start at ${String(expected)}`,
        );
      }
      expected = end;
    }
    if (expected !== this.bytes.length) {
      problems.push(
        `cells end at ${String(expected)}, not at the end of the page`,
      );
    }
    return problems;
  }

  /** Where each cell starts and ends, in the order they lie in the page. */
  private cellExtents(): { offset: number; end: number }[] {
    const extents = Array.from({ length: this.cellCount }, (_, index) => {
      const offset = this.cellOffset(index);
      return { offset, end: offset + this.cellSizeAt(offset) };
    });
    return extents.toSorted((a, b) => a.offset - b.offset);
  }

  private requireInterior(): void {
    if (this.isLeaf) throw new CorruptError("a leaf page has no children");
  }
}

// --- Writing -----------------------------------------------------------------

export class BTreePageEditor extends BTreePage {
  /** Makes `bytes` an empty page of `type`; an interior page gets `rightChild`. */
  static initialize(
    bytes: Uint8Array,
    type: BTreePageType,
    rightChild = 0,
  ): BTreePageEditor {
    bytes.fill(0);
    bytes[0] = TYPE_BYTE[type];
    const editor = new BTreePageEditor(bytes);
    editor.setContentStart(bytes.length);
    if (isInterior(type)) editor.setRightChild(rightChild);
    return editor;
  }

  /** Makes `bytes` a page of `type` holding `cells` in order. */
  static build(
    bytes: Uint8Array,
    type: BTreePageType,
    cells: readonly Uint8Array[],
    rightChild = 0,
  ): BTreePageEditor {
    const editor = this.initialize(bytes, type, rightChild);
    for (const [index, cell] of cells.entries()) editor.insertCell(index, cell);
    return editor;
  }

  /**
   * Inserts `cell` at `index`. The caller has checked that it fits, against
   * the same header: if it does not, the caller skipped the check.
   */
  insertCell(index: number, cell: Uint8Array): void {
    if (!this.fits(cell)) {
      throw new InternalError(
        "a cell was inserted into a page it does not fit",
      );
    }
    const start = this.contentStart - cell.length;
    this.bytes.set(cell, start);
    this.setContentStart(start);
    this.insertPointer(index, start);
  }

  /** Removes cell `index`, closing its gap and zeroing the bytes it frees. */
  removeCell(index: number): void {
    const offset = this.cellOffset(index);
    this.removePointer(index);
    this.closeGap(offset, this.cellSizeAt(offset));
  }

  setRightChild(child: number): void {
    this.view.setUint32(RIGHT_CHILD_OFFSET, child);
  }

  /** Points cell `index` of an interior page, or its rightmost child, at `child`. */
  setChild(index: number, child: number): void {
    if (index === this.cellCount) this.setRightChild(child);
    else this.view.setUint32(this.cellOffset(index), child);
  }

  /** Replaces every cell; an interior page also gets a new rightmost child. */
  rebuild(cells: readonly Uint8Array[], rightChild = 0): void {
    BTreePageEditor.build(this.bytes, this.type, cells, rightChild);
  }

  private insertPointer(index: number, cellOffset: number): void {
    const pointer = this.pointerAt(index);
    this.bytes.copyWithin(pointer + POINTER_SIZE, pointer, this.pointersEnd);
    this.view.setUint16(pointer, cellOffset);
    this.setCellCount(this.cellCount + 1);
  }

  private removePointer(index: number): void {
    const pointer = this.pointerAt(index);
    const end = this.pointersEnd;
    this.bytes.copyWithin(pointer, pointer + POINTER_SIZE, end);
    this.bytes.fill(0, end - POINTER_SIZE, end);
    this.setCellCount(this.cellCount - 1);
  }

  /**
   * Closes the gap a removed cell of `size` bytes left at `offset`: the cells
   * stored before it move toward the end of the page, and their pointers with
   * them.
   */
  private closeGap(offset: number, size: number): void {
    const start = this.contentStart;
    this.bytes.copyWithin(start + size, start, offset);
    this.bytes.fill(0, start, start + size);
    this.setContentStart(start + size);
    this.movePointersBefore(offset, size);
  }

  /** Points the cells stored before `offset` at where they moved: `size` bytes on. */
  private movePointersBefore(offset: number, size: number): void {
    for (let index = 0; index < this.cellCount; index++) {
      const cellOffset = this.cellOffset(index);
      if (cellOffset < offset) {
        this.view.setUint16(this.pointerAt(index), cellOffset + size);
      }
    }
  }

  private pointerAt(index: number): number {
    return headerSize(this.type) + index * POINTER_SIZE;
  }

  private setCellCount(count: number): void {
    this.view.setUint16(CELL_COUNT_OFFSET, count);
  }

  /** See contentStart: 65536 wraps to 0. */
  private setContentStart(start: number): void {
    this.view.setUint16(CONTENT_START_OFFSET, start & 0xff_ff);
  }
}

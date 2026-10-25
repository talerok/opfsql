import type { PageReader } from "../btree/page-store.js";

// ---------------------------------------------------------------------------
// Allocation maps (docs/file-format.md, Allocation maps). Pages are grouped:
// group g covers pages g·G to (g+1)·G − 1, and its map — the group's second
// page — holds one bit per page of the group, set when the page is in use.
// New pages are taken lowest-numbered first, so data packs toward the start
// of the file.
// ---------------------------------------------------------------------------

export const MAP_PAGE_TYPE = 0x06;
const MAP_HEADER_SIZE = 8;

/** Pages a transaction can change: what an AllocationMap edits. */
export interface MapPages extends PageReader {
  write(pageNo: number): Uint8Array;
}

/** Pages one map covers: G. */
export function groupSize(pageSize: number): number {
  return (pageSize - MAP_HEADER_SIZE) * 8;
}

/** The map page of the group `pageNo` is in: the group's second page. */
export function mapPageOf(pageNo: number, pageSize: number): number {
  return groupStartOf(pageNo, groupSize(pageSize)) + 1;
}

export function isMapPage(pageNo: number, pageSize: number): boolean {
  return pageNo === mapPageOf(pageNo, pageSize);
}

/** True for the first page of a group, which a new group's map follows. */
export function isGroupStart(pageNo: number, pageSize: number): boolean {
  return pageNo % groupSize(pageSize) === 0;
}

/** The page numbers of the map pages of a database of `pageCount` pages. */
export function mapPageNumbers(pageSize: number, pageCount: number): number[] {
  const size = groupSize(pageSize);
  return groupStarts(0, pageCount, size).map((start) => start + 1);
}

/** A new map page at `mapPageNo`, marking itself and `alsoInUse` in use. */
export function newMapPage(
  pageSize: number,
  mapPageNo: number,
  alsoInUse: readonly number[] = [],
): Uint8Array {
  const map = new Uint8Array(pageSize);
  map[0] = MAP_PAGE_TYPE;
  const size = groupSize(pageSize);
  for (const pageNo of [mapPageNo, ...alsoInUse]) setBit(map, pageNo % size);
  return map;
}

/** Which pages are in use, as the map pages of one snapshot say. */
export class AllocationMapReader {
  protected readonly size: number;

  constructor(protected readonly pages: PageReader) {
    this.size = groupSize(pages.pageSize);
  }

  isInUse(pageNo: number): boolean {
    return isBitSet(this.mapOf(pageNo), pageNo % this.size);
  }

  /** The lowest free page from `from` up to, not including, `pageCount`. */
  findFree(from: number, pageCount: number): number | null {
    for (const start of groupStarts(from, pageCount, this.size)) {
      const fromBit = Math.max(from - start, 0);
      const endBit = this.endBit(start, pageCount);
      const bit = firstClearBit(this.mapOf(start), fromBit, endBit);
      if (bit !== null) return start + bit;
    }
    return null;
  }

  /** The highest page in use below `pageCount`; the header always is. */
  highestInUse(pageCount: number): number {
    const starts = groupStarts(0, pageCount, this.size);
    for (const start of starts.toReversed()) {
      const bit = lastSetBit(this.mapOf(start), this.endBit(start, pageCount));
      if (bit !== null) return start + bit;
    }
    return 0;
  }

  /** True if a page at or past `pageCount`, which cannot be in use, is marked so. */
  hasBitsPast(pageCount: number): boolean {
    const lastStart = groupStartOf(pageCount - 1, this.size);
    const highest = lastSetBit(this.mapOf(lastStart), this.size);
    return highest !== null && lastStart + highest >= pageCount;
  }

  /** The map page of the group `pageNo` is in. */
  protected mapOf(pageNo: number): Uint8Array {
    return this.pages.read(groupStartOf(pageNo, this.size) + 1);
  }

  /** Bits of the group starting at `start` that stand for pages below `pageCount`. */
  private endBit(start: number, pageCount: number): number {
    return Math.min(pageCount - start, this.size);
  }
}

/** Which pages are in use, changed within a write transaction. */
export class AllocationMap extends AllocationMapReader {
  constructor(private readonly writer: MapPages) {
    super(writer);
  }

  markInUse(pageNo: number): void {
    setBit(this.mapToChange(pageNo), pageNo % this.size);
  }

  markFree(pageNo: number): void {
    clearBit(this.mapToChange(pageNo), pageNo % this.size);
  }

  private mapToChange(pageNo: number): Uint8Array {
    return this.writer.write(groupStartOf(pageNo, this.size) + 1);
  }
}

function groupStartOf(pageNo: number, size: number): number {
  return pageNo - (pageNo % size);
}

/** The first page of each group from the one holding `from` to the one holding `pageCount − 1`. */
function groupStarts(from: number, pageCount: number, size: number): number[] {
  const starts: number[] = [];
  for (let start = groupStartOf(from, size); start < pageCount; start += size) {
    starts.push(start);
  }
  return starts;
}

// --- Bits of a map page ---------------------------------------------------------

function byteOf(bit: number): number {
  return MAP_HEADER_SIZE + Math.floor(bit / 8);
}

function maskOf(bit: number): number {
  return 1 << (bit % 8);
}

function isBitSet(map: Uint8Array, bit: number): boolean {
  return ((map[byteOf(bit)] ?? 0) & maskOf(bit)) !== 0;
}

function setBit(map: Uint8Array, bit: number): void {
  map[byteOf(bit)] = (map[byteOf(bit)] ?? 0) | maskOf(bit);
}

function clearBit(map: Uint8Array, bit: number): void {
  map[byteOf(bit)] = (map[byteOf(bit)] ?? 0) & ~maskOf(bit);
}

function isFullByteAt(map: Uint8Array, bit: number): boolean {
  return bit % 8 === 0 && map[byteOf(bit)] === 0xff;
}

/** The first clear bit from `from` up to `end`, passing full bytes at once. */
function firstClearBit(
  map: Uint8Array,
  from: number,
  end: number,
): number | null {
  let bit = from;
  while (bit < end) {
    if (isFullByteAt(map, bit)) {
      bit += 8;
      continue;
    }
    if (!isBitSet(map, bit)) return bit;
    bit++;
  }
  return null;
}

/** The last set bit below `end`, or null. */
function lastSetBit(map: Uint8Array, end: number): number | null {
  for (let bit = end - 1; bit >= 0; bit--) {
    if (isBitSet(map, bit)) return bit;
  }
  return null;
}

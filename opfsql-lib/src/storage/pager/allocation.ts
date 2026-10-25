// ---------------------------------------------------------------------------
// Allocation maps (docs/file-format.md, Allocation maps). Pages are grouped:
// group g covers pages g·G to (g+1)·G − 1, and its map — the group's second
// page — holds one bit per page of the group, set when the page is in use.
// New pages are taken lowest-numbered first, so data packs toward the start
// of the file.
// ---------------------------------------------------------------------------

export const MAP_PAGE_TYPE = 0x06;
const MAP_HEADER_SIZE = 8;

/** Reads and edits map pages within one snapshot or transaction. */
export interface MapPages {
  read(pageNo: number): Uint8Array;
  write(pageNo: number): Uint8Array;
}

/** Pages one map covers: G. */
export function groupSize(pageSize: number): number {
  return (pageSize - MAP_HEADER_SIZE) * 8;
}

export function isMapPage(pageNo: number, pageSize: number): boolean {
  return pageNo % groupSize(pageSize) === 1;
}

/** A new map page at `mapPageNo`, marking itself and `alsoInUse` in use. */
export function newMapPage(
  pageSize: number,
  mapPageNo: number,
  alsoInUse: readonly number[] = [],
): Uint8Array {
  const map = new Uint8Array(pageSize);
  map[0] = MAP_PAGE_TYPE;
  for (const pageNo of [mapPageNo, ...alsoInUse]) {
    setBit(map, pageNo % groupSize(pageSize), true);
  }
  return map;
}

export class AllocationMap {
  private readonly size: number;

  constructor(
    private readonly pages: MapPages,
    pageSize: number,
  ) {
    this.size = groupSize(pageSize);
  }

  isInUse(pageNo: number): boolean {
    const map = this.pages.read(this.mapPageOf(pageNo));
    return isBitSet(map, pageNo % this.size);
  }

  setInUse(pageNo: number, isInUse: boolean): void {
    const map = this.pages.write(this.mapPageOf(pageNo));
    setBit(map, pageNo % this.size, isInUse);
  }

  /** The lowest free page from `from` up to, not including, `pageCount`. */
  findFree(from: number, pageCount: number): number | null {
    const firstGroup = from - (from % this.size);
    for (let start = firstGroup; start < pageCount; start += this.size) {
      const map = this.pages.read(start + 1);
      const fromBit = Math.max(from - start, 0);
      const endBit = Math.min(pageCount - start, this.size);
      const bit = firstClearBit(map, fromBit, endBit);
      if (bit !== null) return start + bit;
    }
    return null;
  }

  private mapPageOf(pageNo: number): number {
    return pageNo - (pageNo % this.size) + 1;
  }
}

/** The highest page in use below `pageCount`, reading map pages with `read`. */
export function highestInUse(
  read: (pageNo: number) => Uint8Array,
  pageSize: number,
  pageCount: number,
): number {
  const size = groupSize(pageSize);
  const lastGroup = pageCount - 1 - ((pageCount - 1) % size);
  for (let start = lastGroup; start >= 0; start -= size) {
    const endBit = Math.min(pageCount - start, size);
    const bit = lastSetBit(read(start + 1), endBit);
    if (bit !== null) return start + bit;
  }
  return 0;
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

function setBit(map: Uint8Array, bit: number, isSet: boolean): void {
  const byte = map[byteOf(bit)] ?? 0;
  map[byteOf(bit)] = isSet ? byte | maskOf(bit) : byte & ~maskOf(bit);
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

// ---------------------------------------------------------------------------
// Allocation maps (docs/file-format.md, Allocation maps): one bit per page,
// grouped so that group g's map is page g × G + 1. New pages are taken
// lowest-numbered first, so data packs toward the start of the file.
// ---------------------------------------------------------------------------

export const MAP_PAGE_TYPE = 0x06;
const MAP_HEADER_SIZE = 8;

/** Reads and edits map pages within one snapshot or transaction. */
export interface MapPages {
  read(pageNo: number): Uint8Array;
  write(pageNo: number): Uint8Array;
}

/** Pages one map page covers. */
export function groupSize(pageSize: number): number {
  return (pageSize - MAP_HEADER_SIZE) * 8;
}

export function isMapPage(pageNo: number, pageSize: number): boolean {
  return pageNo % groupSize(pageSize) === 1;
}

/**
 * A new map page for the group whose map is page `mapPageNo`, marking the
 * map page itself and `alsoInUse` as in use.
 */
export function newMapPage(
  pageSize: number,
  mapPageNo: number,
  alsoInUse: readonly number[] = [],
): Uint8Array {
  const bytes = new Uint8Array(pageSize);
  bytes.set([MAP_PAGE_TYPE]);
  for (const pageNo of [mapPageNo, ...alsoInUse]) {
    setBit(bytes, pageNo % groupSize(pageSize), true);
  }
  return bytes;
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
    const bytes = this.pages.read(this.mapPageOf(pageNo));
    const bit = pageNo % this.size;
    return (
      ((bytes[MAP_HEADER_SIZE + (bit >>> 3)] ?? 0) & (1 << (bit & 7))) !== 0
    );
  }

  setInUse(pageNo: number, isInUse: boolean): void {
    setBit(
      this.pages.write(this.mapPageOf(pageNo)),
      pageNo % this.size,
      isInUse,
    );
  }

  /** The lowest free page from `from` up to (not including) `pageCount`. */
  findFree(from: number, pageCount: number): number | null {
    for (
      let groupStart = from - (from % this.size);
      groupStart < pageCount;
      groupStart += this.size
    ) {
      const found = this.findFreeInGroup(
        groupStart,
        Math.max(from, groupStart),
        pageCount,
      );
      if (found !== null) return found;
    }
    return null;
  }

  private mapPageOf(pageNo: number): number {
    return pageNo - (pageNo % this.size) + 1;
  }

  private findFreeInGroup(
    groupStart: number,
    from: number,
    pageCount: number,
  ): number | null {
    const bytes = this.pages.read(groupStart + 1);
    const end = Math.min(pageCount - groupStart, this.size);
    for (let bit = from - groupStart; bit < end;) {
      const byte = bytes[MAP_HEADER_SIZE + (bit >>> 3)] ?? 0;
      if (byte === 0xff && (bit & 7) === 0) {
        bit += 8;
        continue;
      }
      if ((byte & (1 << (bit & 7))) === 0) return groupStart + bit;
      bit++;
    }
    return null;
  }
}

function setBit(bytes: Uint8Array, bit: number, isSet: boolean): void {
  const index = MAP_HEADER_SIZE + (bit >>> 3);
  const mask = 1 << (bit & 7);
  const byte = bytes[index] ?? 0;
  bytes.set([isSet ? byte | mask : byte & ~mask], index);
}

/** The highest set bit below `limit`, or null. */
function highestBit(bytes: Uint8Array, limit: number): number | null {
  for (let bit = limit - 1; bit >= 0; bit--) {
    if (
      ((bytes[MAP_HEADER_SIZE + (bit >>> 3)] ?? 0) & (1 << (bit & 7))) !==
      0
    ) {
      return bit;
    }
  }
  return null;
}

/** The highest page in use below `pageCount`, reading map pages with `read`. */
export function highestInUse(
  read: (pageNo: number) => Uint8Array,
  pageSize: number,
  pageCount: number,
): number {
  const size = groupSize(pageSize);
  const lastGroup = pageCount - 1 - ((pageCount - 1) % size);
  for (let groupStart = lastGroup; groupStart >= 0; groupStart -= size) {
    const limit = Math.min(pageCount - groupStart, size);
    const highest = highestBit(read(groupStart + 1), limit);
    if (highest !== null) return groupStart + highest;
  }
  return 0;
}

import { CorruptError } from "../errors.js";
import type { Index } from "../schema/index.js";
import type { PageReader } from "../storage/index.js";
import {
  decodeKey,
  encodeKey,
  IndexReader,
  keyRowid,
  prefixEnd,
} from "../storage/index.js";
import { compareBytes, type Value } from "../values/index.js";
import type { ColumnReader } from "./rows.js";

// ---------------------------------------------------------------------------
// A table's rows through one of its indexes (docs/file-format.md, Index
// keys). Encoded keys compare as their values do, so the entries whose first
// parts equal some values run from that prefix up to prefixEnd(prefix), and
// a range on the next part narrows them by its encoded ends. Values are
// matched as they are, as `=` matches them; a NULL seek value matches
// nothing, as `=` and `<` with NULL never hold.
// ---------------------------------------------------------------------------

/** One end of a range on a key part, and whether it is included. */
export interface KeyBound {
  readonly value: Value;
  readonly isInclusive: boolean;
}

/** A range on one key part; null at an end leaves it open. It never holds NULL. */
export interface KeyRange {
  readonly lower: KeyBound | null;
  readonly upper: KeyBound | null;
}

/** The entries whose first key parts equal `equal`, the next part in `range` if one is given. */
export interface KeySeek {
  readonly equal: readonly Value[];
  readonly range: KeyRange | null;
}

/** Key order, or its reverse. */
export type KeyOrder = "ascending" | "descending";

/** Which end of an index MIN and MAX read from. */
export type IndexEnd = "first" | "last";

/** Entries from `start`, included, up to `end`, excluded. */
interface KeyBounds {
  readonly start: Uint8Array;
  readonly end: Uint8Array;
}

export class IndexRows {
  private readonly entries: IndexReader;

  constructor(
    pages: PageReader,
    readonly index: Index,
  ) {
    this.entries = new IndexReader(pages, index.rootPage);
  }

  /** The rowids of the entries `seek` finds, in key order or its reverse. */
  rowids(seek: KeySeek, order: KeyOrder): Generator<number> {
    return rowidsIn(this.entries, seek, order);
  }

  /**
   * The rows of those entries, as `reader` reads its table's rows; CORRUPT
   * for an entry whose row is missing.
   */
  *rows(
    seek: KeySeek,
    order: KeyOrder,
    reader: ColumnReader,
  ): Generator<Value[]> {
    for (const rowid of this.rowids(seek, order)) {
      yield reader.at(rowid) ?? this.missingRow(rowid);
    }
  }

  /**
   * The first non-NULL value of the key part after `equal`, read from the
   * first or the last entry that starts with `equal`: MIN or MAX of it. NULL
   * if there is none, or if `equal` holds a NULL.
   */
  extreme(equal: readonly Value[], end: IndexEnd): Value {
    if (equal.includes(null)) return null;
    const bounds = {
      start: afterNulls(equal),
      end: prefixEnd(encodeKey(equal)),
    };
    const order = end === "first" ? "ascending" : "descending";
    const found = keysIn(this.entries, bounds, order).next();
    if (found.done === true) return null;
    return decodeKey(found.value)[equal.length] ?? null;
  }

  private missingRow(rowid: number): never {
    throw new CorruptError(
      `index ${this.index.name} has an entry for a missing row ${String(rowid)}`,
    );
  }
}

/**
 * The rowids of the entries `seek` finds in `entries` — an index being read,
 * or one being written, which reads through its own changes.
 */
export function* rowidsIn(
  entries: IndexReader,
  seek: KeySeek,
  order: KeyOrder,
): Generator<number> {
  const bounds = boundsOf(seek);
  if (bounds === null) return;
  for (const key of keysIn(entries, bounds, order)) yield keyRowid(key);
}

/** The entries within `bounds`, in key order or its reverse. */
function* keysIn(
  entries: IndexReader,
  bounds: KeyBounds,
  order: KeyOrder,
): Generator<Uint8Array> {
  if (order === "ascending") {
    for (const key of entries.keysFrom(bounds.start)) {
      if (compareBytes(key, bounds.end) >= 0) return;
      yield key;
    }
    return;
  }
  for (const key of entries.keysBefore(bounds.end)) {
    if (compareBytes(key, bounds.start) < 0) return;
    yield key;
  }
}

/** The entries a seek finds, as bytes; null if it can find none. */
function boundsOf(seek: KeySeek): KeyBounds | null {
  if (!canMatch(seek)) return null;
  const { equal, range } = seek;
  if (range === null) {
    const prefix = encodeKey(equal);
    return { start: prefix, end: prefixEnd(prefix) };
  }
  return { start: rangeStart(equal, range), end: rangeEnd(equal, range) };
}

/** False when a seek value is NULL, which no comparison matches. */
function canMatch({ equal, range }: KeySeek): boolean {
  const hasNullBound =
    range?.lower?.value === null || range?.upper?.value === null;
  return !equal.includes(null) && !hasNullBound;
}

/**
 * Where a range starts: at its lower bound, or without one after the NULLs,
 * which sort first. A range never holds NULL: it holds only values its
 * comparisons hold for, and none holds for NULL.
 */
function rangeStart(equal: readonly Value[], { lower }: KeyRange): Uint8Array {
  return lower === null ? afterNulls(equal) : boundStart(equal, lower);
}

/** Where a range ends: past its upper bound, or past the prefix. */
function rangeEnd(equal: readonly Value[], range: KeyRange): Uint8Array {
  const { upper } = range;
  if (upper === null) return prefixEnd(encodeKey(equal));
  const key = encodeKey([...equal, upper.value]);
  return upper.isInclusive ? prefixEnd(key) : key;
}

/** The first entry at a lower bound: on its value, or after it. */
function boundStart(equal: readonly Value[], lower: KeyBound): Uint8Array {
  const key = encodeKey([...equal, lower.value]);
  return lower.isInclusive ? key : prefixEnd(key);
}

/** The first entry after `equal` whose next part is not NULL. */
function afterNulls(equal: readonly Value[]): Uint8Array {
  return prefixEnd(encodeKey([...equal, null]));
}

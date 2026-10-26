import { InternalError } from "../../errors.js";
import type { StaticType } from "../../functions/index.js";
import type { Index, Table } from "../../schema/index.js";
import {
  logical,
  type OrderDirection,
  type physical,
} from "../../sql/plan/index.js";
import type { IndexReader } from "../../storage/index.js";
import { encodeKey, prefixEnd } from "../../storage/index.js";
import { compareBytes, type Value } from "../../values/index.js";
import type { Context } from "../context.js";
import { compileBound, compileSeek, type SeekBound } from "./seek.js";

// ---------------------------------------------------------------------------
// Key ranges as byte ranges (docs/file-format.md, Index keys): encoded keys
// compare as their values do, so the entries whose first parts equal some
// values are those from that prefix up to prefixEnd(prefix), and a range on
// the next part narrows that by the encoded bounds.
// ---------------------------------------------------------------------------

/** Index entries from `start`, included, up to `end`, excluded. */
export interface KeyBounds {
  readonly start: Uint8Array;
  readonly end: Uint8Array;
}

/** The range for one run: an equal prefix, and maybe a range on the next part. */
interface SeekRange {
  readonly equal: readonly Value[];
  readonly range: {
    readonly lower: SeekBound | null;
    readonly upper: SeekBound | null;
  } | null;
}

/**
 * A key range compiled for a scan: its bounds, evaluated each time the scan
 * starts, or null when no entry can match — a NULL seek value, which `=`
 * and `<` never match.
 */
export function compileKeyRange(
  keys: physical.KeyRange,
  index: Index,
  table: Table,
  context: Context,
): () => KeyBounds | null {
  const equal = keys.equal.map((value, position) =>
    compileSeek(value, keyPartType(index, table, position), context),
  );
  const range = compileRange(keys, index, table, context);
  return () =>
    boundsOf({ equal: equal.map((value) => value()), range: range() });
}

/**
 * The entries after `equal` whose next part is not NULL, for MIN and MAX;
 * null when `equal` holds a NULL.
 */
export function compileNonNullRange(
  equalValues: readonly physical.Expression[],
  index: Index,
  table: Table,
  context: Context,
): () => KeyBounds | null {
  const equal = equalValues.map((value, position) =>
    compileSeek(value, keyPartType(index, table, position), context),
  );
  return () => {
    const values = equal.map((value) => value());
    if (values.includes(null)) return null;
    return { start: lowerKey(values, null, true), end: upperKey(values, null) };
  };
}

/** The entries within `bounds`, in key order or reverse. */
export function* keysIn(
  entries: IndexReader,
  bounds: KeyBounds,
  direction: OrderDirection,
): Generator<Uint8Array> {
  if (direction === "ascending") {
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

/** The type of key part `position`: its column's, or its expression's. */
function keyPartType(index: Index, table: Table, position: number): StaticType {
  const key = index.keys[position];
  if (key === undefined) {
    throw new InternalError(
      `index ${index.name} has no key part ${String(position)}`,
    );
  }
  if (key.kind === "expression") return key.type;
  return logical.columnType(table, key.column);
}

function compileRange(
  keys: physical.KeyRange,
  index: Index,
  table: Table,
  context: Context,
): () => SeekRange["range"] {
  const { range } = keys;
  if (range === null) return () => null;
  const type = keyPartType(index, table, keys.equal.length);
  const lower = compileBound(range.lower, type, context);
  const upper = compileBound(range.upper, type, context);
  return () => ({ lower: lower(), upper: upper() });
}

/**
 * The byte range of a seek, or null if it can match nothing. NULLs in the
 * ranged part are skipped under an upper bound: they sort first, so only an
 * open lower end could reach them, and `<` never matches them.
 */
function boundsOf(seek: SeekRange): KeyBounds | null {
  if (!canMatch(seek)) return null;
  const { equal, range } = seek;
  if (range === null) {
    const prefix = encodeKey(equal);
    return { start: prefix, end: prefixEnd(prefix) };
  }
  return {
    start: lowerKey(equal, range.lower, range.upper !== null),
    end: upperKey(equal, range.upper),
  };
}

/** False when a seek value is NULL, which no comparison matches. */
function canMatch({ equal, range }: SeekRange): boolean {
  const hasNullBound =
    range?.lower?.value === null || range?.upper?.value === null;
  return !equal.includes(null) && !hasNullBound;
}

function lowerKey(
  equal: readonly Value[],
  lower: SeekBound | null,
  areNullsSkipped: boolean,
): Uint8Array {
  if (lower === null) {
    return areNullsSkipped
      ? prefixEnd(encodeKey([...equal, null]))
      : encodeKey(equal);
  }
  const key = encodeKey([...equal, lower.value]);
  return lower.isInclusive ? key : prefixEnd(key);
}

function upperKey(
  equal: readonly Value[],
  upper: SeekBound | null,
): Uint8Array {
  if (upper === null) return prefixEnd(encodeKey(equal));
  const key = encodeKey([...equal, upper.value]);
  return upper.isInclusive ? prefixEnd(key) : key;
}

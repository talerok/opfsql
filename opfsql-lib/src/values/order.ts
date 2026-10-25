import type {
  JsonComposite,
  JsonObject,
  JsonValue,
  Value,
  ValueKind,
} from "./value.js";
import { kindOf } from "./value.js";

// ---------------------------------------------------------------------------
// The total order of docs/semantics.md: NULL < BOOLEAN < NUMBER < TEXT <
// BLOB < JSON, each kind ordered within itself. ORDER BY, MIN/MAX, grouping
// and index keys all use it, so they can never disagree.
// ---------------------------------------------------------------------------

const KIND_RANK: Record<ValueKind, number> = {
  null: 0,
  boolean: 1,
  number: 2,
  text: 3,
  blob: 4,
  json: 5,
};

/** Negative, zero or positive as `a` sorts before, with or after `b`. */
export function compareValues(a: Value, b: Value): number {
  const kind = kindOf(a);
  const rankDifference = KIND_RANK[kind] - KIND_RANK[kindOf(b)];
  if (rankDifference !== 0) return rankDifference;
  return compareSameKind(kind, a, b);
}

/** Equal under the total order: NULL equals NULL, 1 equals 1.0. */
export function areEqual(a: Value, b: Value): boolean {
  return compareValues(a, b) === 0;
}

function compareSameKind(kind: ValueKind, a: Value, b: Value): number {
  switch (kind) {
    case "null": {
      return 0;
    }
    case "boolean": {
      return Number(a) - Number(b);
    }
    case "number": {
      return compareNumbers(a as number, b as number);
    }
    case "text": {
      return compareText(a as string, b as string);
    }
    case "blob": {
      return compareBytes(a as Uint8Array, b as Uint8Array);
    }
    case "json": {
      return compareText(
        canonicalJson(a as JsonComposite),
        canonicalJson(b as JsonComposite),
      );
    }
  }
}

function compareNumbers(a: number, b: number): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

/**
 * Compares well-formed strings by Unicode code point — the order of their
 * UTF-8 bytes. JavaScript's `<` compares UTF-16 code units instead, which
 * puts code points above U+FFFF (stored as surrogate pairs) before
 * U+E000–U+FFFF.
 */
export function compareText(a: string, b: string): number {
  if (a === b) return 0;
  const length = Math.min(a.length, b.length);
  let position = 0;
  while (position < length) {
    const codePointA = a.codePointAt(position) ?? 0;
    const codePointB = b.codePointAt(position) ?? 0;
    if (codePointA !== codePointB) return codePointA - codePointB;
    const isSurrogatePair = codePointA > 0xff_ff;
    position += isSurrogatePair ? 2 : 1;
  }
  return a.length - b.length;
}

/** Compares byte strings lexicographically; a prefix sorts first. */
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length; index++) {
    const difference = (a[index] ?? 0) - (b[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return a.length - b.length;
}

/** JSON text with object keys sorted by code point and no whitespace. */
export function canonicalJson(value: JsonValue): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  if (value !== null && typeof value === "object")
    return canonicalObject(value);
  return JSON.stringify(value);
}

function canonicalObject(object: JsonObject): string {
  const keys = Object.keys(object).toSorted((x, y) => compareText(x, y));
  const members = keys.map((key) => {
    const member = canonicalJson(object[key] ?? null);
    return `${JSON.stringify(key)}:${member}`;
  });
  return `{${members.join(",")}}`;
}

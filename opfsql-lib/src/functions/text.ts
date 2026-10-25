import { bytesToHex, type Value } from "../values/index.js";
import {
  characters,
  eager,
  integer,
  returns,
  strict,
  text,
} from "./arguments.js";
import type { ScalarFunction, StaticType } from "./types.js";

// ---------------------------------------------------------------------------
// Text functions, as SQLite defines them. Positions and lengths count
// characters (code points) in text and bytes in BLOBs.
// ---------------------------------------------------------------------------

const encoder = new TextEncoder();

const upper: ScalarFunction = {
  kind: "scalar",
  name: "upper",
  signature: { required: ["text"] },
  returnType: returns("TEXT"),
  implement: () => strict(([value]) => text(value).toUpperCase()),
};

const lower: ScalarFunction = {
  kind: "scalar",
  name: "lower",
  signature: { required: ["text"] },
  returnType: returns("TEXT"),
  implement: () => strict(([value]) => text(value).toLowerCase()),
};

const length: ScalarFunction = {
  kind: "scalar",
  name: "length",
  signature: { required: ["text-or-blob"] },
  returnType: returns("INTEGER"),
  implement: () =>
    strict(([value]) => {
      if (value instanceof Uint8Array) return value.length;
      return characters(text(value)).length;
    }),
};

/** Which ends TRIM, LTRIM and RTRIM remove characters from. */
type Ends = "start" | "end" | "both";

/** TRIM, LTRIM, RTRIM: remove characters (spaces by default) from the ends. */
function trimFunction(name: string, ends: Ends): ScalarFunction {
  return {
    kind: "scalar",
    name,
    signature: { required: ["text"], optional: ["text"] },
    returnType: returns("TEXT"),
    implement: () =>
      strict(([value, set = " "]) => {
        const chars = characters(text(value));
        const removable = new Set(characters(text(set)));
        const [from, to] = trimmedRange(chars, removable, ends);
        return chars.slice(from, to).join("");
      }),
  };
}

/** The range of `chars` left once `removable` ones are taken off `ends`. */
function trimmedRange(
  chars: readonly string[],
  removable: ReadonlySet<string>,
  ends: Ends,
): [number, number] {
  const first = chars.findIndex((character) => !removable.has(character));
  if (first === -1) return [0, 0];
  const last = chars.findLastIndex((character) => !removable.has(character));
  const from = ends === "end" ? 0 : first;
  const to = ends === "start" ? chars.length : last + 1;
  return [from, to];
}

const substr: ScalarFunction = {
  kind: "scalar",
  name: "substr",
  signature: { required: ["text-or-blob", "number"], optional: ["number"] },
  returnType: ([type = "ANY"]) => substrType(type),
  implement: () =>
    strict(([value, start, count]) => {
      const size = count === undefined ? undefined : integer(count);
      if (value instanceof Uint8Array) {
        const [from, to] = substringRange(value.length, integer(start), size);
        return value.slice(from, to);
      }
      const chars = characters(text(value));
      const [from, to] = substringRange(chars.length, integer(start), size);
      return chars.slice(from, to).join("");
    }),
};

function substrType(type: StaticType): StaticType {
  return type === "BLOB" || type === "ANY" ? type : "TEXT";
}

/**
 * SQLite's substr(X, start, count) as a range [from, to) of positions:
 * `count` characters from `start`, or the `-count` characters before it,
 * clipped to the text. `count` absent means to the end.
 */
export function substringRange(
  total: number,
  start: number,
  count: number | undefined,
): [number, number] {
  const position = zeroBasedStart(start, total);
  const size = count ?? Infinity;
  const low = size < 0 ? position + size : position;
  const high = size < 0 ? position : position + size;
  const from = clamp(low, 0, total);
  return [from, clamp(high, from, total)];
}

/**
 * `start` as a 0-based position: 1-based from the front, negative from the
 * back, and 0 just before the first character — so `substr(x, 0, 2)` is one
 * character, as in SQLite.
 */
function zeroBasedStart(start: number, total: number): number {
  if (start > 0) return start - 1;
  if (start < 0) return total + start;
  return -1;
}

function clamp(value: number, low: number, high: number): number {
  return Math.min(Math.max(value, low), high);
}

const replace: ScalarFunction = {
  kind: "scalar",
  name: "replace",
  signature: { required: ["text", "text", "text"] },
  returnType: returns("TEXT"),
  implement: () =>
    strict(([value, pattern, replacement]) => {
      const original = text(value);
      const search = text(pattern);
      if (search === "") return original;
      // split/join, not replaceAll: "$&" in the replacement is plain text.
      return original.split(search).join(text(replacement));
    }),
};

const concat: ScalarFunction = {
  kind: "scalar",
  name: "concat",
  signature: { required: ["text"], rest: "text" },
  returnType: returns("TEXT"),
  // NULL arguments are skipped, as in SQLite.
  implement: () => eager((operands) => join(operands, "")),
};

const concatWithSeparator: ScalarFunction = {
  kind: "scalar",
  name: "concat_ws",
  signature: { required: ["text", "text"], rest: "text" },
  returnType: returns("TEXT"),
  implement: () =>
    eager(([separator = null, ...operands]) => {
      if (separator === null) return null;
      return join(operands, text(separator));
    }),
};

function join(operands: readonly Value[], separator: string): string {
  return operands
    .filter((operand) => operand !== null)
    .map((operand) => text(operand))
    .join(separator);
}

const instr: ScalarFunction = {
  kind: "scalar",
  name: "instr",
  signature: { required: ["text-or-blob", "text-or-blob"] },
  returnType: returns("INTEGER"),
  implement: () =>
    strict(([haystack, needle]) => {
      const areBytes =
        haystack instanceof Uint8Array && needle instanceof Uint8Array;
      if (areBytes) return bytePosition(haystack, needle);
      return characterPosition(text(haystack), text(needle));
    }),
};

function characterPosition(haystack: string, needle: string): number {
  const index = haystack.indexOf(needle);
  return index === -1 ? 0 : characters(haystack.slice(0, index)).length + 1;
}

function bytePosition(haystack: Uint8Array, needle: Uint8Array): number {
  for (let from = 0; from + needle.length <= haystack.length; from++) {
    if (needle.every((byte, offset) => haystack[from + offset] === byte)) {
      return from + 1;
    }
  }
  return 0;
}

const hex: ScalarFunction = {
  kind: "scalar",
  name: "hex",
  signature: { required: ["any"] },
  returnType: returns("TEXT"),
  // As in SQLite: the bytes of a BLOB, else of the value's UTF-8 text;
  // uppercase; NULL has no bytes and gives ''.
  implement: () =>
    eager(([value]) => {
      if (value === null || value === undefined) return "";
      const bytes =
        value instanceof Uint8Array ? value : encoder.encode(text(value));
      return bytesToHex(bytes).toUpperCase();
    }),
};

export const textFunctions: readonly ScalarFunction[] = [
  upper,
  lower,
  length,
  trimFunction("trim", "both"),
  trimFunction("ltrim", "start"),
  trimFunction("rtrim", "end"),
  substr,
  { ...substr, name: "substring" },
  replace,
  concat,
  concatWithSeparator,
  instr,
  hex,
];

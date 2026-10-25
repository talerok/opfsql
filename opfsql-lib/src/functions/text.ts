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
    strict(([value]) =>
      value instanceof Uint8Array
        ? value.length
        : characters(text(value)).length,
    ),
};

/** TRIM, LTRIM, RTRIM: remove characters (spaces by default) from the ends. */
function trimFunction(
  name: string,
  shouldTrimStart: boolean,
  shouldTrimEnd: boolean,
): ScalarFunction {
  return {
    kind: "scalar",
    name,
    signature: { required: ["text"], optional: ["text"] },
    returnType: returns("TEXT"),
    implement: () =>
      strict(([value, set = " "]) => {
        const removable = new Set(characters(text(set)));
        const chars = characters(text(value));
        let from = 0;
        let to = chars.length;
        while (
          shouldTrimStart &&
          from < to &&
          removable.has(chars[from] ?? "")
        ) {
          from++;
        }
        while (
          shouldTrimEnd &&
          to > from &&
          removable.has(chars[to - 1] ?? "")
        ) {
          to--;
        }
        return chars.slice(from, to).join("");
      }),
  };
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
 * `start` is 1-based, negative counts from the end, 0 is just before the
 * first character; a negative `count` takes the characters before `start`.
 */
export function substringRange(
  total: number,
  start: number,
  count: number | undefined,
): [number, number] {
  let size = count === undefined ? Infinity : Math.abs(count);
  let from = start;
  if (from < 0) {
    from += total;
    if (from < 0) {
      size = Math.max(0, size + from);
      from = 0;
    }
  } else if (from > 0) {
    from--;
  } else if (size > 0) {
    size--;
  }
  if (count !== undefined && count < 0) {
    [from, size] = takeBefore(from, size);
  }
  return [from, from + size];
}

function takeBefore(position: number, size: number): [number, number] {
  const from = position - size;
  return from < 0 ? [0, size + from] : [from, size];
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
      // split/join, not replaceAll: "$&" in the replacement is plain text.
      return search === ""
        ? original
        : original.split(search).join(text(replacement));
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
    eager(([separator, ...operands]) =>
      separator === null || separator === undefined
        ? null
        : join(operands, text(separator)),
    ),
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
    strict(([haystack, needle]) =>
      haystack instanceof Uint8Array && needle instanceof Uint8Array
        ? bytePosition(haystack, needle)
        : characterPosition(text(haystack), text(needle)),
    ),
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
  trimFunction("trim", true, true),
  trimFunction("ltrim", true, false),
  trimFunction("rtrim", false, true),
  substr,
  { ...substr, name: "substring" },
  replace,
  concat,
  concatWithSeparator,
  instr,
  hex,
];

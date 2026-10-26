import { MisuseError } from "../errors.js";
import { normalizeNumber } from "./convert.js";
import type { JsonValue, Value } from "./value.js";

// ---------------------------------------------------------------------------
// Parameters: values from a caller, checked once, where they come in. What
// passes is a value exactly as docs/semantics.md defines it, normalized —
// NaN is NULL and -0 is 0, inside JSON too — and the caller's no more: a
// BLOB is copied and JSON rebuilt, so changing what was passed changes
// nothing that runs. Anything else is MISUSE, named where it was found.
// Below the call, the type says the check was made: only parametersOf makes
// ParameterValues.
// ---------------------------------------------------------------------------

declare const checked: unique symbol;

/** A statement's parameters, as parametersOf checked and copied them. */
export type ParameterValues = readonly Value[] & { readonly [checked]: true };

/** JSON nested deeper than this is refused rather than overflow the stack. */
const MAX_JSON_DEPTH = 1000;

/** Where in a parameter a part is, for messages: `$2`, `$2.tags[0]`. */
interface Place {
  readonly name: string;
  /** Objects and arrays on the way here: one met again refers to itself. */
  readonly ancestors: ReadonlySet<object>;
}

/** A statement's parameters when it takes none. */
export const NO_PARAMETERS: ParameterValues = checkedList([]);

/** Checks a caller's parameters; none when `values` is undefined. */
export function parametersOf(values: unknown): ParameterValues {
  if (values === undefined) return NO_PARAMETERS;
  if (!Array.isArray(values)) {
    throw new MisuseError(
      `parameters are an array of values, not ${describeForeign(values)}`,
    );
  }
  const checkedValues = Array.from(values, (value: unknown, index) =>
    parameterValue(value, `$${String(index + 1)}`),
  );
  return checkedList(checkedValues);
}

/** The one place a list becomes ParameterValues: every item was checked. */
function checkedList(values: readonly Value[]): ParameterValues {
  return values as ParameterValues;
}

function parameterValue(value: unknown, name: string): Value {
  if (value instanceof Uint8Array) return new Uint8Array(value);
  return jsonValue(value, { name, ancestors: new Set() });
}

function jsonValue(value: unknown, place: Place): JsonValue {
  switch (typeof value) {
    case "number": {
      return normalizeNumber(value);
    }
    case "string": {
      return wellFormed(value, place);
    }
    case "boolean": {
      return value;
    }
    case "object": {
      return value === null ? null : composite(value, place);
    }
    case "undefined":
    case "bigint":
    case "symbol":
    case "function": {
      throw refusal(place, notAValue(value));
    }
  }
}

function composite(value: object, place: Place): JsonValue {
  if (place.ancestors.has(value)) throw refusal(place, "refers to itself");
  if (place.ancestors.size >= MAX_JSON_DEPTH) {
    throw refusal(place, `nests deeper than ${String(MAX_JSON_DEPTH)} levels`);
  }
  const inside = { ...place, ancestors: new Set(place.ancestors).add(value) };
  if (Array.isArray(value)) return jsonArray(value, inside);
  if (isPlainObject(value)) return jsonObject(value, inside);
  throw refusal(place, notAValue(value));
}

function jsonArray(array: readonly unknown[], place: Place): JsonValue[] {
  return Array.from({ length: array.length }, (_, index) => {
    const at = { ...place, name: `${place.name}[${String(index)}]` };
    if (!Object.hasOwn(array, index))
      throw refusal(at, "is a hole in the array");
    return jsonValue(array[index], at);
  });
}

function jsonObject(object: object, place: Place): JsonValue {
  if (Object.getOwnPropertySymbols(object).length > 0) {
    throw refusal(place, "has symbol keys, which JSON cannot hold");
  }
  const entries = Object.entries(object).map(
    ([key, item]: [string, unknown]) => {
      const at = { ...place, name: `${place.name}.${key}` };
      return [wellFormed(key, at), jsonValue(item, at)] as const;
    },
  );
  // fromEntries defines own properties: a key "__proto__" stays a key.
  return Object.fromEntries(entries);
}

function wellFormed(text: string, place: Place): string {
  if (text.isWellFormed()) return text;
  throw refusal(place, "has a lone surrogate, which is not valid Unicode");
}

function isPlainObject(value: object): boolean {
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function refusal(place: Place, problem: string): MisuseError {
  return new MisuseError(`parameter ${place.name} ${problem}`);
}

/** Why a foreign value is refused, and what to pass instead. */
function notAValue(value: unknown): string {
  if (value === undefined) return "is undefined: pass null for NULL";
  if (typeof value === "bigint") {
    return "is a BigInt: INTEGER values are numbers, safe integers";
  }
  if (value instanceof Date) {
    return "is a Date, which is not a value: pass its ISO text or its time in ms";
  }
  const isBytes = value instanceof ArrayBuffer || ArrayBuffer.isView(value);
  if (isBytes) {
    return `is ${describeForeign(value)}: a BLOB is a Uint8Array, and only a whole parameter`;
  }
  return `is ${describeForeign(value)}, which is not a value: JSON objects are plain objects`;
}

/** "a function", "an Int16Array", "a Map". */
function describeForeign(value: unknown): string {
  const name =
    typeof value === "object" ? constructorName(value) : typeof value;
  // "a Uint8Array": a U names a sound that takes "a".
  return /^[aeio]/iu.test(name) ? `an ${name}` : `a ${name}`;
}

function constructorName(value: object | null): string {
  const constructor: unknown =
    value === null ? undefined : Reflect.get(value, "constructor");
  return typeof constructor === "function" ? constructor.name : "object";
}

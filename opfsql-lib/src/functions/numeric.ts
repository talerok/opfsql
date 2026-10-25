import { withoutNegativeZero } from "../values/index.js";
import {
  firstArgumentType,
  integer,
  number,
  returns,
  strict,
} from "./arguments.js";
import type { ScalarFunction, StaticType } from "./types.js";

// ---------------------------------------------------------------------------
// Numeric functions, as SQLite defines them.
// ---------------------------------------------------------------------------

/** Above this magnitude every double is an integer: nothing to round. */
const INTEGRAL_DOUBLE = 2 ** 52;

/** SQLite rounds to at most this many decimals. */
const MAX_DIGITS = 30;

const abs: ScalarFunction = {
  kind: "scalar",
  name: "abs",
  signature: { required: ["number"] },
  returnType: firstArgumentType,
  implement: () => strict(([value]) => Math.abs(number(value))),
};

const sign: ScalarFunction = {
  kind: "scalar",
  name: "sign",
  signature: { required: ["number"] },
  returnType: returns("INTEGER"),
  implement: () =>
    strict(([value]) => withoutNegativeZero(Math.sign(number(value)))),
};

/** ROUND(x [, digits]): half away from zero, as SQLite rounds; a REAL. */
const round: ScalarFunction = {
  kind: "scalar",
  name: "round",
  signature: { required: ["number"], optional: ["number"] },
  returnType: returns("REAL"),
  implement: () =>
    strict(([value, digits]) =>
      roundHalfAwayFromZero(
        number(value),
        digits === undefined ? 0 : integer(digits),
      ),
    ),
};

/**
 * Rounds to `digits` decimal places (0 to 30, as SQLite clamps them),
 * halves away from zero, as SQLite does: to an integer by adding 0.5 to
 * the magnitude and truncating; to decimals by the exact decimal
 * expansion of the double, so 1.005 — stored as 1.00499… — rounds to 1.
 */
export function roundHalfAwayFromZero(value: number, digits: number): number {
  const places = Math.min(Math.max(digits, 0), MAX_DIGITS);
  const magnitude = Math.abs(value);
  if (!Number.isFinite(value) || magnitude >= INTEGRAL_DOUBLE) return value;
  const rounded = roundMagnitude(magnitude, places);
  return withoutNegativeZero(value < 0 ? -rounded : rounded);
}

function roundMagnitude(magnitude: number, places: number): number {
  if (places === 0) return Math.trunc(magnitude + 0.5);
  // toFixed picks the larger of two equally close results: away from zero
  // for a non-negative number.
  return Number(magnitude.toFixed(places));
}

const INTEGRAL_TYPES: ReadonlySet<StaticType> = new Set<StaticType>([
  "INTEGER",
  "ANY",
  "NULL",
]);

/** FLOOR and CEIL keep an INTEGER an INTEGER. */
function integralType(types: readonly StaticType[]): StaticType {
  const type = firstArgumentType(types);
  return INTEGRAL_TYPES.has(type) ? type : "REAL";
}

const floor: ScalarFunction = {
  kind: "scalar",
  name: "floor",
  signature: { required: ["number"] },
  returnType: integralType,
  implement: () =>
    strict(([value]) => withoutNegativeZero(Math.floor(number(value)))),
};

const ceil: ScalarFunction = {
  kind: "scalar",
  name: "ceil",
  signature: { required: ["number"] },
  returnType: integralType,
  implement: () =>
    strict(([value]) => withoutNegativeZero(Math.ceil(number(value)))),
};

export const numericFunctions: readonly ScalarFunction[] = [
  abs,
  sign,
  round,
  floor,
  ceil,
  { ...ceil, name: "ceiling" },
];

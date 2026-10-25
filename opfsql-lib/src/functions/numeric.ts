import {
  firstArgumentType,
  integer,
  number,
  positiveZero,
  returns,
  strict,
} from "./arguments.js";
import type { ScalarFunction, StaticType } from "./types.js";

// ---------------------------------------------------------------------------
// Numeric functions, as SQLite defines them.
// ---------------------------------------------------------------------------

/** Above this magnitude every double is an integer: nothing to round. */
const INTEGRAL_DOUBLE = 2 ** 52;

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
  implement: () => strict(([value]) => positiveZero(Math.sign(number(value)))),
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
 * halves away from zero. Works on the decimal expansion of the double, so
 * 2.5 rounds to 3 and -2.5 to -3, while 1.005 — stored as 1.00499… —
 * rounds to 1.
 */
export function roundHalfAwayFromZero(value: number, digits: number): number {
  const places = Math.min(Math.max(digits, 0), 30);
  const magnitude = Math.abs(value);
  if (!Number.isFinite(value) || magnitude >= INTEGRAL_DOUBLE) return value;
  // toFixed picks the larger of two equally close results: away from zero
  // for a non-negative number.
  const rounded = Number(magnitude.toFixed(places));
  return positiveZero(value < 0 ? -rounded : rounded);
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
  implement: () => strict(([value]) => positiveZero(Math.floor(number(value)))),
};

const ceil: ScalarFunction = {
  kind: "scalar",
  name: "ceil",
  signature: { required: ["number"] },
  returnType: integralType,
  implement: () => strict(([value]) => positiveZero(Math.ceil(number(value)))),
};

export const numericFunctions: readonly ScalarFunction[] = [
  abs,
  sign,
  round,
  floor,
  ceil,
  { ...ceil, name: "ceiling" },
];

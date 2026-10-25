import { areEqual, kindOf, truthOf, type Value } from "../values/index.js";
import { commonType, eager, firstArgumentType, returns } from "./arguments.js";
import type { Argument, Implementation, ScalarFunction } from "./types.js";

// ---------------------------------------------------------------------------
// Functions about NULLs, choices and types.
// ---------------------------------------------------------------------------

/** The first non-NULL argument; the ones after it are not evaluated. */
const firstNonNull: Implementation = {
  kind: "lazy",
  call: <Row>(operands: readonly Argument<Row>[], row: Row): Value => {
    for (const argument of operands) {
      const value = argument(row);
      if (value !== null) return value;
    }
    return null;
  },
};

const coalesce: ScalarFunction = {
  kind: "scalar",
  name: "coalesce",
  signature: { required: ["any", "any"], rest: "any" },
  returnType: commonType,
  implement: () => firstNonNull,
};

const ifnull: ScalarFunction = {
  kind: "scalar",
  name: "ifnull",
  signature: { required: ["any", "any"] },
  returnType: commonType,
  implement: () => firstNonNull,
};

/** NULLIF(x, y): NULL if x = y, else x. */
const nullif: ScalarFunction = {
  kind: "scalar",
  name: "nullif",
  signature: { required: ["any", "any"] },
  returnType: firstArgumentType,
  implement: () =>
    eager(([value = null, other = null]) =>
      areEqual(value, other) ? null : value,
    ),
};

/** IIF(condition, then [, else]): CASE WHEN condition THEN … ELSE … END. */
const iif: ScalarFunction = {
  kind: "scalar",
  name: "iif",
  signature: { required: ["boolean", "any"], optional: ["any"] },
  returnType: (types) => commonType(types.slice(1)),
  implement: () => ({
    kind: "lazy",
    call: <Row>(operands: readonly Argument<Row>[], row: Row): Value => {
      const [condition, then, otherwise] = operands;
      const chosen =
        truthOf(condition?.(row) ?? null) === true ? then : otherwise;
      return chosen?.(row) ?? null;
    },
  }),
};

/**
 * TYPEOF(x): 'null', 'integer', 'real', 'text', 'blob' as in SQLite, plus
 * 'boolean' and 'json'. A number is 'real' if its static type is REAL —
 * JavaScript cannot tell 2.0 from 2 — else by its value.
 */
const typeOf: ScalarFunction = {
  kind: "scalar",
  name: "typeof",
  signature: { required: ["any"] },
  returnType: returns("TEXT"),
  implement: ({ argumentTypes }) => {
    const isReal = argumentTypes[0] === "REAL";
    return eager(([value = null]) => typeName(value, isReal));
  },
};

function typeName(value: Value, isReal: boolean): string {
  const kind = kindOf(value);
  if (kind !== "number") return kind;
  return !isReal && Number.isSafeInteger(value) ? "integer" : "real";
}

export const conditionalFunctions: readonly ScalarFunction[] = [
  coalesce,
  ifnull,
  nullif,
  iif,
  typeOf,
];

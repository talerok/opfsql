import {
  checkInteger,
  compareValues,
  firstSighting,
  normalizeNumber,
  type Value,
} from "../values/index.js";
import { firstArgumentType, number, returns } from "./arguments.js";
import type { Accumulator, AggregateFunction, StaticType } from "./types.js";

// ---------------------------------------------------------------------------
// Aggregate functions. Every one skips rows with a NULL argument, as SQL
// aggregates do, and DISTINCT keeps the first of each value — both applied
// by `startAggregate`, so the accumulators only see what they count.
// ---------------------------------------------------------------------------

/** COUNT(*) or COUNT(x): the rows, or the non-NULL values. */
const count: AggregateFunction = {
  kind: "aggregate",
  name: "count",
  signature: { required: [], optional: ["any"] },
  returnType: returns("INTEGER"),
  accumulator: () => {
    let rows = 0;
    return {
      add: () => {
        rows++;
      },
      result: () => rows,
    };
  },
};

/**
 * SUM: an INTEGER over integers — failing with INTEGER_OVERFLOW outside the
 * safe range — else a REAL; NULL over no rows.
 */
const sum: AggregateFunction = {
  kind: "aggregate",
  name: "sum",
  signature: { required: ["number"] },
  returnType: ([type]) => numericResultType(type),
  accumulator: ([type]) => numericTotal(type),
};

/** AVG: the mean as a REAL; NULL over no rows. */
const avg: AggregateFunction = {
  kind: "aggregate",
  name: "avg",
  signature: { required: ["number"] },
  returnType: returns("REAL"),
  accumulator: () => {
    const total = compensatedSum();
    let rows = 0;
    return {
      add: ([value]) => {
        total.add(number(value));
        rows++;
      },
      result: () => (rows === 0 ? null : normalizeNumber(total.value() / rows)),
    };
  },
};

function numericResultType(type: StaticType | undefined): StaticType {
  return type === "INTEGER" || type === "REAL" ? type : "ANY";
}

function numericTotal(type: StaticType | undefined): Accumulator {
  let integers = 0;
  const reals = compensatedSum();
  let rows = 0;
  let areIntegers = type !== "REAL";
  return {
    add: ([value]) => {
      const operand = number(value);
      areIntegers &&= Number.isSafeInteger(operand);
      if (areIntegers) integers = checkInteger(integers + operand);
      reals.add(operand);
      rows++;
    },
    result: () => {
      if (rows === 0) return null;
      return areIntegers ? integers : normalizeNumber(reals.value());
    },
  };
}

/**
 * A sum of numbers with Kahan-Babuska-Neumaier compensation, as SQLite
 * sums REALs: ten times 0.1 is 1, not 0.9999999999999999.
 */
function compensatedSum(): { add(value: number): void; value(): number } {
  let total = 0;
  let compensation = 0;
  return {
    add: (value) => {
      const next = total + value;
      // An infinite total has nothing left to compensate.
      if (Number.isFinite(next)) compensation += lostPart(total, value, next);
      total = next;
    },
    value: () => total + compensation,
  };
}

/** What rounding dropped from `a + b`, which came out as `rounded`. */
function lostPart(a: number, b: number, rounded: number): number {
  if (Math.abs(a) >= Math.abs(b)) return a - rounded + b;
  return b - rounded + a;
}

/** MIN and MAX by the total order of values. */
function extreme(name: string, wanted: 1 | -1): AggregateFunction {
  return {
    kind: "aggregate",
    name,
    signature: { required: ["any"] },
    returnType: firstArgumentType,
    accumulator: () => {
      let best: Value = null;
      return {
        add: ([value = null]) => {
          if (best === null || compareValues(value, best) * wanted > 0) {
            best = value;
          }
        },
        result: () => best,
      };
    },
  };
}

export const aggregateFunctions: readonly AggregateFunction[] = [
  count,
  sum,
  avg,
  extreme("min", -1),
  extreme("max", 1),
];

/**
 * A new accumulator for one group: skips rows with a NULL argument and,
 * for DISTINCT, rows whose arguments it has seen.
 */
export function startAggregate(
  aggregate: AggregateFunction,
  argumentTypes: readonly StaticType[],
  isDistinct: boolean,
): Accumulator {
  const inner = aggregate.accumulator(argumentTypes);
  const isFirstSighting = isDistinct ? firstSighting() : () => true;
  return {
    add: (operands) => {
      if (operands.includes(null) || !isFirstSighting(operands)) return;
      inner.add(operands);
    },
    result: () => inner.result(),
  };
}

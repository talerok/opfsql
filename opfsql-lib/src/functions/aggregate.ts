import {
  checkInteger,
  compareValues,
  rowKey,
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
    let total = 0;
    let rows = 0;
    return {
      add: ([value]) => {
        total += number(value);
        rows++;
      },
      result: () => (rows === 0 ? null : total / rows),
    };
  },
};

function numericResultType(type: StaticType | undefined): StaticType {
  return type === "INTEGER" || type === "REAL" ? type : "ANY";
}

function numericTotal(type: StaticType | undefined): Accumulator {
  let total = 0;
  let rows = 0;
  let areIntegers = type !== "REAL";
  return {
    add: ([value]) => {
      const operand = number(value);
      areIntegers &&= Number.isSafeInteger(operand);
      total += operand;
      if (areIntegers) checkInteger(total);
      rows++;
    },
    result: () => (rows === 0 ? null : total),
  };
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
  const seen = isDistinct ? new Set<string>() : null;
  return {
    add: (operands) => {
      if (operands.includes(null)) return;
      if (seen !== null) {
        const key = rowKey(operands);
        if (seen.has(key)) return;
        seen.add(key);
      }
      inner.add(operands);
    },
    result: () => inner.result(),
  };
}

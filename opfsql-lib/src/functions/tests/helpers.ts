import type { Value, StaticType } from "../../values/index.js";
import {
  aggregateFunction,
  resolveCall,
  scalarFunction,
  startAggregate,
} from "../index.js";

/** The static type a test value would have as a literal. */
export function typeOf(value: Value): StaticType {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return "BOOLEAN";
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? "INTEGER" : "REAL";
  }
  if (typeof value === "string") return "TEXT";
  return value instanceof Uint8Array ? "BLOB" : "JSON";
}

/**
 * Calls scalar function `name` the way the executor does, with every
 * argument varying per row unless `constant` says otherwise.
 */
export function call(
  name: string,
  values: readonly Value[],
  options: {
    types?: readonly StaticType[];
    constant?: boolean;
  } = {},
): Value {
  const definition = scalarFunction(name);
  const types = options.types ?? values.map((value) => typeOf(value));
  resolveCall(definition, types);
  const implementation = definition.implement({
    argumentTypes: types,
    fixedValues: values.map((value) =>
      options.constant === true ? value : undefined,
    ),
  });
  return implementation.kind === "eager"
    ? implementation.call(values)
    : implementation.call(
        values.map((value) => () => value),
        null,
      );
}

/** Runs aggregate `name` over rows of argument values. */
export function aggregate(
  name: string,
  rows: readonly (readonly Value[])[],
  options: { types?: readonly StaticType[]; distinct?: boolean } = {},
): Value {
  const definition = aggregateFunction(name);
  const types = options.types ?? (rows[0] ?? []).map((value) => typeOf(value));
  resolveCall(definition, types);
  const accumulator = startAggregate(
    definition,
    types,
    options.distinct === true,
  );
  for (const row of rows) accumulator.add(row);
  return accumulator.result();
}

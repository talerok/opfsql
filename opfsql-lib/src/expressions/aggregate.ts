import { type Accumulator, startAggregate } from "../functions/index.js";
import type { AggregateCall, physical } from "../sql/plan/index.js";
import type { Value } from "../values/index.js";
import { compileExpression } from "./compile.js";
import type { CompiledExpression, Scope, Tuple } from "./scope.js";

/**
 * An aggregate call compiled for its node's input rows: everything the
 * aggregate operator needs, so that it evaluates nothing itself.
 */
export interface CompiledAggregate {
  /**
   * A new, empty accumulator for one group: it skips rows with a NULL
   * argument and, for DISTINCT, arguments it has seen.
   */
  readonly start: () => Accumulator;
  /**
   * The call's argument values for a row, in one array reused for every
   * row, as `Accumulator.add` takes them. None for COUNT(*).
   */
  readonly argumentsOf: (row: Tuple) => readonly Value[];
}

export function compileAggregate(
  call: AggregateCall<physical.Plan>,
  scope: Scope,
): CompiledAggregate {
  const types = call.arguments.map((argument) => argument.type);
  const operands = call.arguments.map((argument) =>
    compileExpression(argument, scope),
  );
  return {
    start: () => startAggregate(call.function, types, call.isDistinct),
    argumentsOf: argumentReader(operands),
  };
}

/** Reads a row's argument values into one array, reused for every row. */
function argumentReader(
  operands: readonly CompiledExpression[],
): (row: Tuple) => readonly Value[] {
  const values: Value[] = [];
  return (row) => {
    for (const [position, operand] of operands.entries()) {
      values[position] = operand(row);
    }
    return values;
  };
}

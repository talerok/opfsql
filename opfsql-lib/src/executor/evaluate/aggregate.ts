import { BoundAggregateExpression } from "../../binder";
import { ExecutorError } from "../errors";
import { Resolver } from "../resolve";
import { Tuple, Value } from "../types";

// Aggregates are pre-computed by PhysicalHashAggregate,
// referenced via binding set by the binder
export async function evalAggregate(
  expr: BoundAggregateExpression,
  tuple: Tuple,
  resolver: Resolver,
): Promise<Value> {
  const pos = resolver(expr.binding!);
  if (pos === undefined) {
    throwError(expr);
  }

  return tuple[pos] ?? null;
}

function throwError(expr: BoundAggregateExpression): never {
  const tableIndex = expr.binding!.tableIndex;
  const columnIndex = expr.binding!.columnIndex;
  throw new ExecutorError(
    `Aggregate binding {tableIndex:${tableIndex}, columnIndex:${columnIndex}} not found in layout`,
  );
}

import type {
  BoundBetweenExpression,
  BoundExpression,
} from "../../binder/types.js";
import { compareValues } from "./utils/compare.js";
import type { Resolver } from "../resolve.js";
import type { Tuple, Value } from "../types.js";
import type { SyncEvalContext } from "./context.js";
import { evaluateExpression } from "./index.js";

export function evalBetween(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const bt = expr as BoundBetweenExpression;
  const input = evaluateExpression(bt.input, tuple, resolver, ctx);
  const lower = evaluateExpression(bt.lower, tuple, resolver, ctx);
  const upper = evaluateExpression(bt.upper, tuple, resolver, ctx);

  if (input === null || lower === null || upper === null) return null;
  return compareValues(input, lower) >= 0 && compareValues(input, upper) <= 0;
}

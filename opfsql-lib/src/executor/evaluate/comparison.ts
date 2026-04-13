import type {
  BoundComparisonExpression,
  BoundExpression,
} from "../../binder/types.js";
import { applyComparison } from "./utils/compare.js";
import type { Resolver } from "../resolve.js";
import type { Tuple, Value } from "../types.js";
import type { SyncEvalContext } from "./context.js";
import { evaluateExpression } from "./index.js";

export function evalComparison(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const cmp = expr as BoundComparisonExpression;
  const left = evaluateExpression(cmp.left, tuple, resolver, ctx);
  const right = evaluateExpression(cmp.right, tuple, resolver, ctx);
  return applyComparison(left, right, cmp.comparisonType);
}

import type {
  BoundCastExpression,
  BoundExpression,
} from "../../binder/types.js";
import { castValue } from "./utils/cast.js";
import type { Resolver } from "../resolve.js";
import type { Tuple, Value } from "../types.js";
import type { SyncEvalContext } from "./context.js";
import { evaluateExpression } from "./index.js";

export function evalCast(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const cast = expr as BoundCastExpression;
  const val = evaluateExpression(cast.child, tuple, resolver, ctx);
  return castValue(val, cast.castType);
}

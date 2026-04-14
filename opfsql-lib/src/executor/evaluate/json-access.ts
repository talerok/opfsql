import type {
  BoundExpression,
  BoundJsonAccessExpression,
} from "../../binder/types.js";
import type { Resolver } from "../resolve.js";
import type { Tuple, Value } from "../types.js";
import { evalColumnRef } from "./column-ref.js";
import type { SyncEvalContext } from "./context.js";

export function evalJsonAccess(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const access = expr as BoundJsonAccessExpression;
  const obj = evalColumnRef(access.child, tuple, resolver, ctx);
  if (obj === null || typeof obj !== "object") {
    return null;
  }

  let current: unknown = obj;
  for (const seg of access.path) {
    if (current == null) {
      return null;
    }
    if (seg.type === "field") {
      if (typeof current !== "object" || Array.isArray(current)) return null;
      current = (current as Record<string, unknown>)[seg.name];
    } else {
      if (!Array.isArray(current)) return null;
      current = current[seg.value];
    }
  }

  return (current ?? null) as Value;
}

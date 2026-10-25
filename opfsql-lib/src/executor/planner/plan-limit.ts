import type { LogicalLimit, LogicalOperator } from "../../sql/binder/types.js";
import { PhysicalLimit } from "../operators/limit.js";
import type { PlannerContext } from "./types.js";

export function planLimit(node: LogicalOperator, pc: PlannerContext) {
  const limit = node as LogicalLimit;
  return new PhysicalLimit(pc.plan(limit.children[0]), limit);
}

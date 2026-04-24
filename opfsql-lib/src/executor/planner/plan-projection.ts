import type { LogicalOperator, LogicalProjection } from "../../binder/types.js";
import { PhysicalProjection } from "../operators/projection.js";
import type { PlannerContext } from "./types.js";

export function planProjection(node: LogicalOperator, pc: PlannerContext) {
  const proj = node as LogicalProjection;
  return new PhysicalProjection(pc.plan(proj.children[0]), proj, pc.ctx);
}

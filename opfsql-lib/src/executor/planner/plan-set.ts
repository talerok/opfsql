import type { LogicalOperator, LogicalUnion } from "../../binder/types.js";
import { PhysicalDistinct, PhysicalUnion } from "../operators/set.js";
import type { PlannerContext } from "./types.js";

export function planDistinct(node: LogicalOperator, pc: PlannerContext) {
  return new PhysicalDistinct(pc.plan(node.children[0]));
}

export function planUnion(node: LogicalOperator, pc: PlannerContext) {
  const union = node as LogicalUnion;
  return new PhysicalUnion(
    pc.plan(union.children[0]), pc.plan(union.children[1]), union.all,
  );
}

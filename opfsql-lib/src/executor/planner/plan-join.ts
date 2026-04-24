import type { LogicalComparisonJoin, LogicalOperator } from "../../binder/types.js";
import { PhysicalHashJoin, PhysicalNestedLoopJoin } from "../operators/join.js";
import type { PlannerContext } from "./types.js";

export function planComparisonJoin(node: LogicalOperator, pc: PlannerContext) {
  const join = node as LogicalComparisonJoin;
  return new PhysicalHashJoin(
    pc.plan(join.children[0]), pc.plan(join.children[1]), join, pc.ctx,
  );
}

export function planCrossProduct(node: LogicalOperator, pc: PlannerContext) {
  return new PhysicalNestedLoopJoin(
    pc.plan(node.children[0]!), pc.plan(node.children[1]!),
  );
}

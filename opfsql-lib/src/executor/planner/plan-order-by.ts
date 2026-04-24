import type { LogicalOperator, LogicalOrderBy } from "../../binder/types.js";
import { PhysicalSort } from "../operators/sort.js";
import type { PlannerContext } from "./types.js";

export function planOrderBy(node: LogicalOperator, pc: PlannerContext) {
  const order = node as LogicalOrderBy;
  return new PhysicalSort(pc.plan(order.children[0]), order, pc.ctx);
}

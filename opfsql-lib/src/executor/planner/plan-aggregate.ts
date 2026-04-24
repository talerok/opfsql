import type { LogicalAggregate, LogicalOperator } from "../../binder/types.js";
import { PhysicalHashAggregate } from "../operators/aggregate.js";
import { PhysicalIndexMinMax } from "../operators/index-min-max.js";
import type { PlannerContext } from "./types.js";

export function planAggregate(node: LogicalOperator, pc: PlannerContext) {
  const agg = node as LogicalAggregate;

  if (agg.minMaxHint && pc.indexManager)
    return new PhysicalIndexMinMax(agg, pc.indexManager, agg.minMaxHint);

  return new PhysicalHashAggregate(pc.plan(agg.children[0]), agg, pc.ctx);
}

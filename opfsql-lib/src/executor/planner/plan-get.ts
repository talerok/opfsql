import type { LogicalGet, LogicalOperator } from "../../binder/types.js";
import { PhysicalIndexScan } from "../operators/index-scan.js";
import { PhysicalIndexUnionScan } from "../operators/index-union-scan.js";
import { PhysicalChildScan, PhysicalScan } from "../operators/scan.js";
import type { PlannerContext } from "./types.js";

export function planGet(node: LogicalOperator, pc: PlannerContext) {
  const get = node as LogicalGet;

  if (get.indexHint && pc.indexManager) {
    if (get.indexHint.kind === "union")
      return new PhysicalIndexUnionScan(
        get, pc.rowManager, pc.indexManager, get.indexHint, pc.ctx,
      );

    if (get.indexHint.kind === "scan")
      return new PhysicalIndexScan(
        get, pc.rowManager, pc.indexManager,
        get.indexHint.indexDef, get.indexHint.predicates,
        get.indexHint.residualFilters, pc.ctx,
      );
  }

  if (get.children.length > 0)
    return new PhysicalChildScan(get, pc.plan(get.children[0]), pc.ctx);

  return new PhysicalScan(get, pc.rowManager, pc.ctx);
}

import type {
  LogicalCTERef,
  LogicalMaterializedCTE,
  LogicalOperator,
  LogicalRecursiveCTE,
} from "../../binder/types.js";
import {
  PhysicalCTEScan,
  PhysicalMaterialize,
  PhysicalRecursiveCTE,
} from "../operators/cte.js";
import type { PlannerContext } from "./types.js";

export function planMaterializedCTE(node: LogicalOperator, pc: PlannerContext) {
  const cte = node as LogicalMaterializedCTE;
  return new PhysicalMaterialize(
    pc.plan(cte.children[0]), pc.plan(cte.children[1]),
    cte.cteIndex, pc.cteCache,
  );
}

export function planRecursiveCTE(node: LogicalOperator, pc: PlannerContext) {
  const rec = node as LogicalRecursiveCTE;
  return new PhysicalRecursiveCTE(
    pc.plan(rec.children[0]), pc.plan(rec.children[1]),
    rec.cteIndex, pc.cteCache, rec.isUnionAll,
  );
}

export function planCTERef(node: LogicalOperator, pc: PlannerContext) {
  return new PhysicalCTEScan(node as LogicalCTERef, pc.cteCache);
}

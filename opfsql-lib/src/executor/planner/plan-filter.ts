import type { BoundConjunctionExpression, LogicalFilter, LogicalOperator } from "../../binder/types.js";
import { BoundExpressionClass } from "../../binder/types.js";
import { PhysicalFilter } from "../operators/filter.js";
import type { PlannerContext } from "./types.js";

export function planFilter(node: LogicalOperator, pc: PlannerContext) {
  const filter = node as LogicalFilter;

  const condition = filter.expressions.length === 1
    ? filter.expressions[0]
    : {
        expressionClass: BoundExpressionClass.BOUND_CONJUNCTION as const,
        conjunctionType: "AND" as const,
        children: filter.expressions,
        returnType: "BOOLEAN" as const,
      } satisfies BoundConjunctionExpression;

  return new PhysicalFilter(pc.plan(filter.children[0]), condition, pc.ctx);
}

import type { AggregateCall, logical, physical } from "../plan/index.js";
import { rebuildChildren } from "./expression-tree.js";
import type { Planner } from "./planner.js";

// ---------------------------------------------------------------------------
// Expressions of a logical plan as expressions of a physical one: the same
// tree, each subquery's plan planned through every pass on the way, since a
// physical plan cannot hold a logical one.
// ---------------------------------------------------------------------------

export function lowerExpression(
  expression: logical.Expression,
  planner: Planner,
): physical.Expression {
  return rebuildChildren(expression, {
    expression: (child) => lowerExpression(child, planner),
    plan: (plan) => planner.planSubquery(plan),
  });
}

export function lowerOptional(
  expression: logical.Expression | null,
  planner: Planner,
): physical.Expression | null {
  return expression === null ? null : lowerExpression(expression, planner);
}

export function lowerExpressions(
  expressions: readonly logical.Expression[],
  planner: Planner,
): physical.Expression[] {
  return expressions.map((expression) => lowerExpression(expression, planner));
}

export function lowerOrderKeys(
  keys: readonly logical.OrderKey[],
  planner: Planner,
): physical.OrderKey[] {
  return keys.map((key) => ({
    ...key,
    expression: lowerExpression(key.expression, planner),
  }));
}

export function lowerAggregateCall(
  call: AggregateCall<logical.Plan>,
  planner: Planner,
): AggregateCall<physical.Plan> {
  return { ...call, arguments: lowerExpressions(call.arguments, planner) };
}

/** `plan` under `projects` (outermost first), lowered. */
export function underProjects(
  plan: physical.Plan,
  projects: readonly logical.Project[],
  planner: Planner,
): physical.Plan {
  let result = plan;
  for (const project of projects.toReversed()) {
    result = {
      kind: "project",
      tableIndex: project.tableIndex,
      child: result,
      expressions: lowerExpressions(project.expressions, planner),
    };
  }
  return result;
}

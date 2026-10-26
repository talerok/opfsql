import type { logical } from "../plan/index.js";
import { type Replacement, substitute } from "./expression-tree.js";

// ---------------------------------------------------------------------------
// Walking logical plans: a node rebuilt with new children. Each pass decides
// what it does at a node and leaves the rest of the tree to this walk.
// ---------------------------------------------------------------------------

/** `plan` with each child plan replaced by what `map` makes of it. */
export function withChildren(
  plan: logical.Plan,
  map: (child: logical.Plan) => logical.Plan,
): logical.Plan {
  switch (plan.kind) {
    case "get":
    case "values":
    case "cte-scan": {
      return plan;
    }
    case "filter":
    case "project":
    case "aggregate":
    case "order":
    case "limit":
    case "distinct": {
      return { ...plan, child: map(plan.child) };
    }
    case "join":
    case "set-operation": {
      return { ...plan, left: map(plan.left), right: map(plan.right) };
    }
    case "cte": {
      const definition = map(plan.definition);
      return { ...plan, definition, body: map(plan.body) };
    }
    case "recursive-cte": {
      return { ...plan, anchor: map(plan.anchor), step: map(plan.step) };
    }
  }
}

/** A table read with the filter right on it. */
export interface TableRead {
  readonly get: logical.Get;
  readonly conditions: readonly logical.Expression[];
}

/** `plan` as a table read: a get, or a filter over one; null for anything else. */
export function tableReadOf(plan: logical.Plan): TableRead | null {
  if (plan.kind === "get") return { get: plan, conditions: [] };
  if (plan.kind !== "filter" || plan.child.kind !== "get") return null;
  return { get: plan.child, conditions: plan.conditions };
}

/** The projects at the top of `plan`, outermost first, and the plan below them. */
export function projectsOf(plan: logical.Plan): {
  readonly projects: logical.Project[];
  readonly below: logical.Plan;
} {
  const projects: logical.Project[] = [];
  let below = plan;
  while (below.kind === "project") {
    projects.push(below);
    below = below.child;
  }
  return { projects, below };
}

/** A project's columns replaced by the expressions it computes them from. */
export function projectedBy(
  project: logical.Project,
): Replacement<logical.Plan> {
  return (column) => {
    const { tableIndex, columnIndex } = column.binding;
    const isProjected = tableIndex === project.tableIndex;
    return isProjected ? project.expressions[columnIndex] : undefined;
  };
}

/** An aggregate's group columns replaced by the expressions it groups by. */
export function groupedBy(
  aggregate: logical.Aggregate,
): Replacement<logical.Plan> {
  return (column) => {
    const { tableIndex, columnIndex } = column.binding;
    const isGroup = tableIndex === aggregate.groupIndex;
    return isGroup ? aggregate.groups[columnIndex] : undefined;
  };
}

/** `expression` over the outermost project's columns, rewritten over the innermost's child. */
export function throughProjects(
  expression: logical.Expression,
  projects: readonly logical.Project[],
): logical.Expression | null {
  let result = expression;
  for (const project of projects) {
    const rewritten = substitute(result, projectedBy(project));
    if (rewritten === null) return null;
    result = rewritten;
  }
  return result;
}

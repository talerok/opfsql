import type { logical, physical } from "../plan/index.js";
import { chooseAccessPath } from "./access-path.js";
import { emptyPlan } from "./empty.js";
import { hasNeverTrue } from "./expression-tree.js";
import { indexExtreme } from "./extreme.js";
import { planJoin } from "./join.js";
import {
  lowerAggregateCall,
  lowerExpressions,
  lowerOptional,
  underProjects,
} from "./lower-expression.js";
import { lowerOrder } from "./order.js";
import { projectsOf } from "./plan-tree.js";
import type { Planner } from "./planner.js";

// ---------------------------------------------------------------------------
// Pass 7, the logical plan as a physical one, after simplification and
// pushdown. A table read gets its access path (access-path.ts) with the
// filter right above it; a join is split into keys and residual (join.ts);
// an ORDER BY becomes a sort, a top-n or nothing (order.ts); a MIN or MAX
// may read one end of an index (extreme.ts). The rest maps directly:
// aggregate → hash-aggregate, distinct → hash-distinct; set operations,
// CTEs, values, projects and limits keep their shape. A filter or join no
// row passes reads nothing (empty.ts).
// ---------------------------------------------------------------------------

export function lowerPlan(plan: logical.Plan, planner: Planner): physical.Plan {
  const lower = (child: logical.Plan): physical.Plan =>
    lowerPlan(child, planner);
  switch (plan.kind) {
    case "get": {
      return scanOf(plan, [], planner);
    }
    case "filter": {
      return lowerFilter(plan, planner);
    }
    case "join": {
      return lowerJoin(plan, planner);
    }
    case "aggregate": {
      return indexExtreme(plan, planner) ?? hashAggregate(plan, planner);
    }
    case "order": {
      return lowerOrder(plan, null, planner, lower);
    }
    case "limit": {
      return lowerLimit(plan, planner);
    }
    case "project": {
      return underProjects(lower(plan.child), [plan], planner);
    }
    case "distinct": {
      return { kind: "hash-distinct", child: lower(plan.child) };
    }
    case "set-operation": {
      return { ...plan, left: lower(plan.left), right: lower(plan.right) };
    }
    case "cte": {
      return {
        ...plan,
        definition: lower(plan.definition),
        body: lower(plan.body),
      };
    }
    case "recursive-cte": {
      return { ...plan, anchor: lower(plan.anchor), step: lower(plan.step) };
    }
    case "cte-scan": {
      return plan;
    }
    case "values": {
      const rows = plan.rows.map((row) => lowerExpressions(row, planner));
      return { ...plan, rows };
    }
  }
}

function scanOf(
  get: logical.Get,
  conditions: readonly logical.Expression[],
  planner: Planner,
): physical.Plan {
  const lowered = lowerExpressions(conditions, planner);
  return chooseAccessPath({ get, conditions: lowered, order: null }, planner)
    .plan;
}

function lowerFilter(plan: logical.Filter, planner: Planner): physical.Plan {
  if (hasNeverTrue(plan.conditions)) {
    return emptyPlan(plan.child);
  }
  if (plan.child.kind === "get")
    return scanOf(plan.child, plan.conditions, planner);
  return {
    kind: "filter",
    child: lowerPlan(plan.child, planner),
    conditions: lowerExpressions(plan.conditions, planner),
  };
}

/**
 * A join whose ON no pair passes: nothing for an inner join, and for a left
 * join every left row with NULLs, the right side read as empty.
 */
function lowerJoin(plan: logical.Join, planner: Planner): physical.Plan {
  const isEmptyOn = hasNeverTrue(plan.conditions);
  if (isEmptyOn && plan.joinType === "inner") return emptyPlan(plan);
  const left = lowerPlan(plan.left, planner);
  const right = isEmptyOn
    ? emptyPlan(plan.right)
    : lowerPlan(plan.right, planner);
  const conditions = lowerExpressions(plan.conditions, planner);
  return planJoin(plan.joinType, left, right, conditions);
}

function hashAggregate(
  plan: logical.Aggregate,
  planner: Planner,
): physical.Plan {
  return {
    kind: "hash-aggregate",
    groupIndex: plan.groupIndex,
    aggregateIndex: plan.aggregateIndex,
    child: lowerPlan(plan.child, planner),
    groups: lowerExpressions(plan.groups, planner),
    aggregates: plan.aggregates.map((call) =>
      lowerAggregateCall(call, planner),
    ),
  };
}

/**
 * A LIMIT over an ORDER BY, through any projects between them: a project
 * keeps rows one for one, so the limit applies below it as well as above,
 * and goes with the order (order.ts).
 */
function lowerLimit(plan: logical.Limit, planner: Planner): physical.Plan {
  const lower = (child: logical.Plan): physical.Plan =>
    lowerPlan(child, planner);
  const { projects, below } = projectsOf(plan.child);
  if (below.kind === "order") {
    const ordered = lowerOrder(below, plan, planner, lower);
    return underProjects(ordered, projects, planner);
  }
  return {
    kind: "limit",
    child: lower(plan.child),
    limit: lowerOptional(plan.limit, planner),
    offset: lowerOptional(plan.offset, planner),
  };
}

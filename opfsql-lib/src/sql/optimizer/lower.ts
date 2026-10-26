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
import { placeLimit } from "./limit.js";
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
      return readTable(plan, [], planner);
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

/** A table read with its filter's conditions, through its access path. */
function readTable(
  get: logical.Get,
  conditions: readonly logical.Expression[],
  planner: Planner,
): physical.Plan {
  const lowered = lowerExpressions(conditions, planner);
  const request = { get, conditions: lowered, order: null };
  return chooseAccessPath(request, planner).plan;
}

function lowerFilter(plan: logical.Filter, planner: Planner): physical.Plan {
  if (hasNeverTrue(plan.conditions)) {
    return emptyPlan(plan.child);
  }
  if (plan.child.kind === "get") {
    return readTable(plan.child, plan.conditions, planner);
  }
  return {
    kind: "filter",
    child: lowerPlan(plan.child, planner),
    conditions: lowerExpressions(plan.conditions, planner),
  };
}

function lowerJoin(plan: logical.Join, planner: Planner): physical.Plan {
  if (hasNeverTrue(plan.conditions)) return lowerEmptyJoin(plan, planner);
  const left = lowerPlan(plan.left, planner);
  const right = lowerPlan(plan.right, planner);
  const conditions = lowerExpressions(plan.conditions, planner);
  return planJoin(plan.joinType, left, right, conditions);
}

/**
 * A join whose ON no pair passes: nothing for an inner join, and for a left
 * join every left row with NULLs, the right side read as empty.
 */
function lowerEmptyJoin(plan: logical.Join, planner: Planner): physical.Plan {
  if (plan.joinType === "inner") return emptyPlan(plan);
  const left = lowerPlan(plan.left, planner);
  const conditions = lowerExpressions(plan.conditions, planner);
  return planJoin("left", left, emptyPlan(plan.right), conditions);
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
 * A LIMIT, placed below the projects it sits on (limit.ts); over an ORDER
 * BY, the two lower together (order.ts).
 */
function lowerLimit(plan: logical.Limit, planner: Planner): physical.Plan {
  const { projects, limit } = placeLimit(plan);
  return underProjects(lowerPlacedLimit(limit, planner), projects, planner);
}

function lowerPlacedLimit(
  limit: logical.Limit,
  planner: Planner,
): physical.Plan {
  const lower = (child: logical.Plan): physical.Plan =>
    lowerPlan(child, planner);
  const { child } = limit;
  if (child.kind === "order") return lowerOrder(child, limit, planner, lower);
  return {
    kind: "limit",
    child: lower(child),
    limit: lowerOptional(limit.limit, planner),
    offset: lowerOptional(limit.offset, planner),
  };
}

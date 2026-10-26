import { logical, type physical } from "../plan/index.js";

// ---------------------------------------------------------------------------
// A plan that reads nothing, for a filter or join no row passes (a condition
// that folded to FALSE or NULL). It keeps the layout of the plan it stands
// for, so the nodes above still find their columns: a `values` without rows
// under the same table index where the layout is one relation's columns, an
// empty rowid list for a table, and the same node over empty children where
// the layout is made of several.
// ---------------------------------------------------------------------------

export function emptyPlan(plan: logical.Plan): physical.Plan {
  switch (plan.kind) {
    case "get": {
      const { tableIndex, table, columns } = plan;
      const rowids = { kind: "list", values: [] } as const;
      const access = { tableIndex, table, columns, residual: [] };
      return {
        kind: "rowid-lookup",
        ...access,
        rowids,
        direction: "ascending",
      };
    }
    case "values":
    case "project":
    case "set-operation":
    case "cte-scan":
    case "recursive-cte": {
      return noRows(plan.tableIndex, plan);
    }
    case "aggregate": {
      return emptyAggregate(plan);
    }
    case "join": {
      const left = emptyPlan(plan.left);
      const right = emptyPlan(plan.right);
      return {
        kind: "nested-loop-join",
        joinType: "inner",
        left,
        right,
        conditions: [],
      };
    }
    case "filter":
    case "order":
    case "limit":
    case "distinct": {
      return emptyPlan(plan.child);
    }
    case "cte": {
      return emptyPlan(plan.body);
    }
  }
}

function noRows(tableIndex: number, plan: logical.Plan): physical.Values {
  return { kind: "values", tableIndex, rows: [], types: logical.typesOf(plan) };
}

/**
 * Its layout is the groups' relation, then the aggregates': each an empty
 * `values` (one of them has no columns when there are no groups or no
 * aggregates), side by side.
 */
function emptyAggregate(plan: logical.Aggregate): physical.Plan {
  const groups: physical.Values = {
    kind: "values",
    tableIndex: plan.groupIndex,
    rows: [],
    types: plan.groups.map((group) => group.type),
  };
  const aggregates: physical.Values = {
    kind: "values",
    tableIndex: plan.aggregateIndex,
    rows: [],
    types: plan.aggregates.map((call) => call.type),
  };
  if (plan.groups.length === 0) return aggregates;
  if (plan.aggregates.length === 0) return groups;
  return {
    kind: "nested-loop-join",
    joinType: "inner",
    left: groups,
    right: aggregates,
    conditions: [],
  };
}

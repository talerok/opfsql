import {
  type AggregateCall,
  type logical,
  type physical,
  rebuildChildren,
} from "../../plan/index.js";

// A logical plan lowered without choosing anything: table scans, filters,
// nested loops, sorts. Run by the reference interpreter, it is the yardstick
// the optimizer's plans are held to.

export function naivePlan(plan: logical.Plan): physical.Plan {
  switch (plan.kind) {
    case "get": {
      const { tableIndex, table, columns } = plan;
      const access = { tableIndex, table, columns, residual: [] };
      return { kind: "table-scan", ...access, direction: "ascending" };
    }
    case "filter": {
      const conditions = naiveAll(plan.conditions);
      return { kind: "filter", child: naivePlan(plan.child), conditions };
    }
    case "join": {
      const { joinType } = plan;
      const sides = {
        left: naivePlan(plan.left),
        right: naivePlan(plan.right),
      };
      const conditions = naiveAll(plan.conditions);
      return { kind: "nested-loop-join", joinType, ...sides, conditions };
    }
    case "order": {
      const keys = plan.keys.map((key) => ({
        ...key,
        expression: naive(key.expression),
      }));
      return { kind: "sort", child: naivePlan(plan.child), keys };
    }
    case "distinct": {
      return { kind: "hash-distinct", child: naivePlan(plan.child) };
    }
    case "aggregate":
    case "project":
    case "limit":
    case "values": {
      return naiveCompute(plan);
    }
    case "set-operation":
    case "cte":
    case "recursive-cte":
    case "cte-scan": {
      return naiveCombine(plan);
    }
  }
}

/** Nodes that compute expressions: the same node, its expressions lowered. */
function naiveCompute(
  plan: logical.Aggregate | logical.Project | logical.Limit | logical.Values,
): physical.Plan {
  switch (plan.kind) {
    case "aggregate": {
      const aggregates = plan.aggregates.map((call) => naiveCall(call));
      const groups = naiveAll(plan.groups);
      const child = naivePlan(plan.child);
      return { ...plan, kind: "hash-aggregate", child, groups, aggregates };
    }
    case "project": {
      const expressions = naiveAll(plan.expressions);
      return { ...plan, child: naivePlan(plan.child), expressions };
    }
    case "limit": {
      const limit = naiveOptional(plan.limit);
      const offset = naiveOptional(plan.offset);
      return { ...plan, child: naivePlan(plan.child), limit, offset };
    }
    case "values": {
      return { ...plan, rows: plan.rows.map((row) => naiveAll(row)) };
    }
  }
}

/** Nodes over other plans: the same node over its children lowered. */
function naiveCombine(
  plan:
    logical.SetOperation | logical.Cte | logical.RecursiveCte | logical.CteScan,
): physical.Plan {
  switch (plan.kind) {
    case "set-operation": {
      return {
        ...plan,
        left: naivePlan(plan.left),
        right: naivePlan(plan.right),
      };
    }
    case "cte": {
      const definition = naivePlan(plan.definition);
      return { ...plan, definition, body: naivePlan(plan.body) };
    }
    case "recursive-cte": {
      return {
        ...plan,
        anchor: naivePlan(plan.anchor),
        step: naivePlan(plan.step),
      };
    }
    case "cte-scan": {
      return plan;
    }
  }
}

/** An expression, its subquery plans lowered naively too. */
export function naive(expression: logical.Expression): physical.Expression {
  return rebuildChildren(expression, {
    expression: (child) => naive(child),
    plan: (plan) => naivePlan(plan),
  });
}

function naiveAll(
  expressions: readonly logical.Expression[],
): physical.Expression[] {
  return expressions.map((expression) => naive(expression));
}

function naiveOptional(
  expression: logical.Expression | null,
): physical.Expression | null {
  return expression === null ? null : naive(expression);
}

function naiveCall(
  call: AggregateCall<logical.Plan>,
): AggregateCall<physical.Plan> {
  return { ...call, arguments: naiveAll(call.arguments) };
}

import {
  type AggregateCall,
  type ColumnReference,
  type logical,
  nodesOf,
  type OrderKey,
  rebuildChildren,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// The binder's walks over bound expressions and logical plans, built on
// sql/plan's (walk.ts). Mapping returns a new tree and leaves the old one as
// it was; a subquery's plan is a level further in, so mapping an expression
// stops at it, and the caller decides what to do.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;
type Plan = logical.Plan;
type ExpressionMapper = (expression: Expression) => Expression;

/** `expression` with each direct sub-expression mapped; subquery plans untouched. */
export function mapChildren(
  expression: Expression,
  map: ExpressionMapper,
): Expression {
  return rebuildChildren(expression, { expression: map, plan: (plan) => plan });
}

function mapAll(
  expressions: readonly Expression[],
  map: ExpressionMapper,
): Expression[] {
  return expressions.map((expression) => map(expression));
}

/** `plan` with every expression of every node mapped, its subplans included. */
export function mapPlan(plan: Plan, map: ExpressionMapper): Plan {
  switch (plan.kind) {
    case "get":
    case "cte-scan": {
      return plan;
    }
    case "values": {
      return { ...plan, rows: plan.rows.map((row) => mapAll(row, map)) };
    }
    case "filter": {
      const child = mapPlan(plan.child, map);
      return { ...plan, child, conditions: mapAll(plan.conditions, map) };
    }
    case "project": {
      const child = mapPlan(plan.child, map);
      return { ...plan, child, expressions: mapAll(plan.expressions, map) };
    }
    case "aggregate": {
      return mapAggregate(plan, map);
    }
    case "join": {
      const left = mapPlan(plan.left, map);
      const right = mapPlan(plan.right, map);
      return { ...plan, left, right, conditions: mapAll(plan.conditions, map) };
    }
    case "order": {
      const keys = plan.keys.map((key) => mapKey(key, map));
      return { ...plan, child: mapPlan(plan.child, map), keys };
    }
    case "limit": {
      return mapLimit(plan, map);
    }
    case "distinct": {
      return { ...plan, child: mapPlan(plan.child, map) };
    }
    case "set-operation": {
      const left = mapPlan(plan.left, map);
      return { ...plan, left, right: mapPlan(plan.right, map) };
    }
    case "cte": {
      const definition = mapPlan(plan.definition, map);
      return { ...plan, definition, body: mapPlan(plan.body, map) };
    }
    case "recursive-cte": {
      const anchor = mapPlan(plan.anchor, map);
      return { ...plan, anchor, step: mapPlan(plan.step, map) };
    }
  }
}

function mapAggregate(plan: logical.Aggregate, map: ExpressionMapper): Plan {
  const aggregates = plan.aggregates.map((call): AggregateCall<Plan> => ({
    ...call,
    arguments: mapAll(call.arguments, map),
  }));
  return {
    ...plan,
    child: mapPlan(plan.child, map),
    groups: mapAll(plan.groups, map),
    aggregates,
  };
}

function mapKey(key: OrderKey<Plan>, map: ExpressionMapper): OrderKey<Plan> {
  return { ...key, expression: map(key.expression) };
}

function mapLimit(plan: logical.Limit, map: ExpressionMapper): Plan {
  const { limit, offset } = plan;
  return {
    ...plan,
    child: mapPlan(plan.child, map),
    limit: limit === null ? null : map(limit),
    offset: offset === null ? null : map(offset),
  };
}

/** The columns `expression` reads at its own level: not those inside subqueries. */
export function* columnsReadBy(
  expression: Expression,
): Generator<ColumnReference> {
  for (const node of nodesOf(expression)) {
    if (node.kind === "column") yield node;
  }
}

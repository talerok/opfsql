import type {
  AggregateCall,
  ColumnReference,
  logical,
  OrderKey,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// Walks over bound expressions and logical plans. Mapping returns a new tree
// and leaves the old one as it was; a subquery's plan is a level further in,
// so mapping an expression stops at it, and the caller decides what to do.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;
type Plan = logical.Plan;
type Map = (expression: Expression) => Expression;

/** `expression` with each direct sub-expression mapped; subquery plans untouched. */
export function mapChildren(expression: Expression, map: Map): Expression {
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "json-access":
    case "scalar-subquery":
    case "exists": {
      return expression;
    }
    case "comparison":
    case "arithmetic":
    case "concatenate": {
      const left = map(expression.left);
      return { ...expression, left, right: map(expression.right) };
    }
    case "conjunction": {
      return { ...expression, operands: expression.operands.map(map) };
    }
    case "not":
    case "is-null":
    case "negate":
    case "cast": {
      return { ...expression, operand: map(expression.operand) };
    }
    case "in-list": {
      const needle = map(expression.needle);
      return { ...expression, needle, list: expression.list.map(map) };
    }
    case "between": {
      const input = map(expression.input);
      const lower = map(expression.lower);
      return { ...expression, input, lower, upper: map(expression.upper) };
    }
    case "function": {
      return { ...expression, arguments: expression.arguments.map(map) };
    }
    case "case": {
      return mapCase(expression, map);
    }
    case "quantified-comparison": {
      return { ...expression, needle: map(expression.needle) };
    }
  }
}

function mapCase(
  expression: Extract<Expression, { kind: "case" }>,
  map: Map,
): Expression {
  const { operand, otherwise } = expression;
  const branches = expression.branches.map((branch) => ({
    when: map(branch.when),
    result: map(branch.result),
  }));
  return {
    ...expression,
    operand: operand === null ? null : map(operand),
    branches,
    otherwise: otherwise === null ? null : map(otherwise),
  };
}

/** `plan` with every expression of every node mapped, its subplans included. */
export function mapPlan(plan: Plan, map: Map): Plan {
  switch (plan.kind) {
    case "get":
    case "cte-scan": {
      return plan;
    }
    case "values": {
      return { ...plan, rows: plan.rows.map((row) => row.map(map)) };
    }
    case "filter": {
      const child = mapPlan(plan.child, map);
      return { ...plan, child, conditions: plan.conditions.map(map) };
    }
    case "project": {
      const child = mapPlan(plan.child, map);
      return { ...plan, child, expressions: plan.expressions.map(map) };
    }
    case "aggregate": {
      return mapAggregate(plan, map);
    }
    case "join": {
      const left = mapPlan(plan.left, map);
      const right = mapPlan(plan.right, map);
      return { ...plan, left, right, conditions: plan.conditions.map(map) };
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

function mapAggregate(plan: logical.Aggregate, map: Map): Plan {
  const aggregates = plan.aggregates.map((call): AggregateCall<Plan> => ({
    ...call,
    arguments: call.arguments.map(map),
  }));
  return {
    ...plan,
    child: mapPlan(plan.child, map),
    groups: plan.groups.map(map),
    aggregates,
  };
}

function mapKey(key: OrderKey<Plan>, map: Map): OrderKey<Plan> {
  return { ...key, expression: map(key.expression) };
}

function mapLimit(plan: logical.Limit, map: Map): Plan {
  const { limit, offset } = plan;
  return {
    ...plan,
    child: mapPlan(plan.child, map),
    limit: limit === null ? null : map(limit),
    offset: offset === null ? null : map(offset),
  };
}

/**
 * The direct sub-expressions in a fixed order, with the gaps a CASE may
 * have; a subquery's plan is not among them.
 */
export function partsOf(
  expression: Expression,
): readonly (Expression | null)[] {
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "json-access":
    case "scalar-subquery":
    case "exists": {
      return [];
    }
    case "comparison":
    case "arithmetic":
    case "concatenate": {
      return [expression.left, expression.right];
    }
    case "conjunction": {
      return expression.operands;
    }
    case "not":
    case "is-null":
    case "negate":
    case "cast": {
      return [expression.operand];
    }
    case "in-list": {
      return [expression.needle, ...expression.list];
    }
    case "between": {
      return [expression.input, expression.lower, expression.upper];
    }
    case "function": {
      return expression.arguments;
    }
    case "case": {
      const branches = expression.branches.flatMap((branch) => [
        branch.when,
        branch.result,
      ]);
      return [expression.operand, ...branches, expression.otherwise];
    }
    case "quantified-comparison": {
      return [expression.needle];
    }
  }
}

/** The columns `expression` reads at its own level: not those inside subqueries. */
export function* columnsOf(expression: Expression): Generator<ColumnReference> {
  if (expression.kind === "column") {
    yield expression;
    return;
  }
  if (expression.kind === "json-access") {
    yield expression.column;
    return;
  }
  for (const part of partsOf(expression)) {
    if (part !== null) yield* columnsOf(part);
  }
}

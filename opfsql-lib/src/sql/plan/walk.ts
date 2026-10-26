import type {
  Between,
  Case,
  Expression,
  SubqueryExpression,
} from "./expression.js";

// ---------------------------------------------------------------------------
// Walking bound expressions: a node's direct children, every node below it,
// and a copy with its children rebuilt. Generic in the plan their subqueries
// hold, so one rebuild serves the binder's rewrites, the optimizer's and the
// lowering to physical plans. A walk never enters a subquery's plan: the
// expressions in there belong to another query level, and a column at depth
// 0 there is not this query's.
// ---------------------------------------------------------------------------

/** The direct child expressions of `expression`, subquery plans aside. */
export function childrenOf<Plan>(
  expression: Expression<Plan>,
): readonly Expression<Plan>[] {
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "scalar-subquery":
    case "exists": {
      return [];
    }
    case "json-access": {
      return [expression.column];
    }
    case "comparison":
    case "arithmetic":
    case "concatenate": {
      return [expression.left, expression.right];
    }
    case "not":
    case "is-null":
    case "negate":
    case "cast": {
      return [expression.operand];
    }
    case "conjunction": {
      return expression.operands;
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
      return caseChildren(expression);
    }
    case "quantified-comparison": {
      return [expression.needle];
    }
  }
}

function caseChildren<Plan>(expression: Case<Plan>): Expression<Plan>[] {
  const { operand, branches, otherwise } = expression;
  const children = operand === null ? [] : [operand];
  for (const branch of branches) children.push(branch.when, branch.result);
  if (otherwise !== null) children.push(otherwise);
  return children;
}

/** `expression` and every expression inside it, subquery plans aside. */
export function* nodesOf<Plan>(
  expression: Expression<Plan>,
): Generator<Expression<Plan>> {
  yield expression;
  for (const child of childrenOf(expression)) yield* nodesOf(child);
}

/** How to rebuild the parts of an expression: child expressions, subquery plans. */
export interface Rebuild<From, To> {
  readonly expression: (expression: Expression<From>) => Expression<To>;
  readonly plan: (plan: From) => To;
}

/** `expression` with each direct part rebuilt; leaves come back as they are. */
export function rebuildChildren<From, To>(
  expression: Expression<From>,
  rebuild: Rebuild<From, To>,
): Expression<To> {
  const child = rebuild.expression;
  const each = (expressions: readonly Expression<From>[]): Expression<To>[] =>
    expressions.map((item) => child(item));
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "json-access": {
      return expression;
    }
    case "comparison":
    case "arithmetic":
    case "concatenate": {
      const { left, right } = expression;
      return { ...expression, left: child(left), right: child(right) };
    }
    case "not":
    case "is-null":
    case "negate":
    case "cast": {
      return { ...expression, operand: child(expression.operand) };
    }
    case "conjunction": {
      return { ...expression, operands: each(expression.operands) };
    }
    case "in-list": {
      const { needle, list } = expression;
      return { ...expression, needle: child(needle), list: each(list) };
    }
    case "between": {
      return rebuildBetween(expression, child);
    }
    case "function": {
      return { ...expression, arguments: each(expression.arguments) };
    }
    case "case": {
      return rebuildCase(expression, rebuild);
    }
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return rebuildSubquery(expression, rebuild);
    }
  }
}

function rebuildBetween<From, To>(
  expression: Between<From>,
  child: (expression: Expression<From>) => Expression<To>,
): Between<To> {
  const { input, lower, upper } = expression;
  return {
    ...expression,
    input: child(input),
    lower: child(lower),
    upper: child(upper),
  };
}

function rebuildSubquery<From, To>(
  expression: SubqueryExpression<From>,
  rebuild: Rebuild<From, To>,
): SubqueryExpression<To> {
  const plan = rebuild.plan(expression.plan);
  if (expression.kind !== "quantified-comparison") {
    return { ...expression, plan };
  }
  return { ...expression, needle: rebuild.expression(expression.needle), plan };
}

function rebuildCase<From, To>(
  expression: Case<From>,
  rebuild: Rebuild<From, To>,
): Case<To> {
  const child = rebuild.expression;
  const { operand, branches, otherwise } = expression;
  return {
    ...expression,
    operand: operand === null ? null : child(operand),
    branches: branches.map(({ when, result }) => ({
      when: child(when),
      result: child(result),
    })),
    otherwise: otherwise === null ? null : child(otherwise),
  };
}

/** True for an expression that holds a subquery's plan. */
export function isSubqueryExpression<Plan>(
  expression: Expression<Plan>,
): expression is SubqueryExpression<Plan> {
  switch (expression.kind) {
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return true;
    }
    case "column":
    case "constant":
    case "parameter":
    case "comparison":
    case "conjunction":
    case "not":
    case "is-null":
    case "arithmetic":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "function":
    case "case":
    case "cast":
    case "json-access": {
      return false;
    }
  }
}

import type { Case, Expression } from "./expression.js";

// ---------------------------------------------------------------------------
// Walking bound expressions: a node's direct children, and every node below
// it. A walk never enters a subquery's plan: the expressions in there belong
// to another query level, and a column at depth 0 there is not this query's.
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

import { InternalError } from "../errors.js";
import { type Expression, nodesOf } from "../sql/plan/index.js";
import { NO_PARAMETERS, type Value } from "../values/index.js";
import { compileExpression } from "./compile.js";
import { createRun, type Scope } from "./scope.js";

/** Where a constant is evaluated: no columns, parameters or subqueries. */
const CONSTANT_SCOPE: Scope = {
  layout: [],
  outerLayouts: [],
  run: createRun(NO_PARAMETERS, {
    compile: () => {
      throw new InternalError("a constant expression compiled a subquery");
    },
  }),
};

/**
 * The value of an expression without columns, parameters, subqueries or
 * volatile functions, computed by the same compiler that runs queries — so
 * folding a constant at plan time cannot disagree with evaluating it per
 * row. Undefined for any other expression, and when evaluating fails: the
 * error is left for run time, where it surfaces only if the expression is
 * reached. Any plan type: a constant holds no subquery.
 */
export function evaluateConstant<Plan>(
  expression: Expression<Plan>,
): Value | undefined {
  if (!isConstant(expression)) return undefined;
  try {
    return compileExpression(expression, CONSTANT_SCOPE)([]);
  } catch {
    return undefined;
  }
}

/** True if nothing in `expression` reads a row, a run or a clock. */
function isConstant<Plan>(
  expression: Expression<Plan>,
): expression is Expression<never> {
  for (const node of nodesOf(expression)) {
    if (!isFixedNode(node)) return false;
  }
  return true;
}

/** True if the node itself gives the same value on every evaluation. */
function isFixedNode<Plan>(node: Expression<Plan>): boolean {
  switch (node.kind) {
    case "column":
    case "parameter":
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return false;
    }
    case "function": {
      return node.function.isVolatile !== true;
    }
    case "constant":
    case "comparison":
    case "conjunction":
    case "not":
    case "is-null":
    case "arithmetic":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "case":
    case "cast":
    case "json-access": {
      return true;
    }
  }
}

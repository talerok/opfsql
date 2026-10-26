import { evaluateConstant } from "../../expressions/index.js";
import type { AggregateCall, logical } from "../plan/index.js";
import {
  conjunctsOf,
  disjunctsOf,
  hasNeverTrue,
  isAlwaysFalse,
  isAlwaysTrue,
  rebuildChildren,
  truth,
} from "./expression-tree.js";
import { withChildren } from "./plan-tree.js";

// ---------------------------------------------------------------------------
// Pass 1, expression simplification (docs/planning.md, rule 4). A
// subexpression without columns, parameters, subqueries or volatile calls is
// evaluated by the expression compiler (evaluateConstant) and replaced by its
// value, typed as the expression was, so the conversions around it do not
// change; one whose evaluation fails is left for run time.
//
// Then only identities that hold in three-valued logic for every operand,
// NULL included:
//   TRUE AND x → x     FALSE AND x → FALSE
//   FALSE OR x → x     TRUE OR x → TRUE
//   NOT NOT x → x
// where the x left standing alone is a condition (a comparison, AND, OR,
// NOT, IS NULL, IN, BETWEEN, EXISTS or a quantified comparison), which
// gives only TRUE, FALSE or NULL: an operand of AND or NOT is checked to be
// a truth, and dropping the operator must not drop that check. Nothing
// else: `x = x` is NULL for a NULL x, and `x * 0` is NULL for a NULL x, so
// neither is touched.
//
// A filter or join whose conditions include one that is never TRUE (a
// constant FALSE or NULL) keeps that one condition, FALSE: lowering turns
// it into a plan that reads nothing.
//
// The pass works on one query level; a subquery's plan is simplified when
// it is planned (optimize.ts).
// ---------------------------------------------------------------------------

export function simplifyPlan(plan: logical.Plan): logical.Plan {
  return simplifyNode(withChildren(plan, simplifyPlan));
}

function simplifyNode(plan: logical.Plan): logical.Plan {
  switch (plan.kind) {
    case "get":
    case "cte-scan":
    case "limit":
    case "distinct":
    case "set-operation":
    case "cte":
    case "recursive-cte": {
      return plan;
    }
    case "filter": {
      const conditions = simplifyConditions(plan.conditions);
      return conditions.length === 0 ? plan.child : { ...plan, conditions };
    }
    case "join": {
      return { ...plan, conditions: simplifyConditions(plan.conditions) };
    }
    case "project": {
      return { ...plan, expressions: simplifyAll(plan.expressions) };
    }
    case "aggregate": {
      const aggregates = plan.aggregates.map((call) => simplifyCall(call));
      return { ...plan, groups: simplifyAll(plan.groups), aggregates };
    }
    case "order": {
      return { ...plan, keys: plan.keys.map((key) => simplifyKey(key)) };
    }
    case "values": {
      return { ...plan, rows: plan.rows.map((row) => simplifyAll(row)) };
    }
  }
}

/** The conjuncts of conditions, simplified: TRUE ones gone, or FALSE alone. */
function simplifyConditions(
  conditions: readonly logical.Expression[],
): logical.Expression[] {
  const conjuncts = conditions
    .map((condition) => simplifyExpression(condition))
    .flatMap((condition) => conjunctsOf(condition));
  if (hasNeverTrue(conjuncts)) {
    return [truth(false)];
  }
  return conjuncts.filter((condition) => !isAlwaysTrue(condition));
}

function simplifyAll(
  expressions: readonly logical.Expression[],
): logical.Expression[] {
  return expressions.map((expression) => simplifyExpression(expression));
}

function simplifyKey(key: logical.OrderKey): logical.OrderKey {
  return { ...key, expression: simplifyExpression(key.expression) };
}

function simplifyCall(
  call: AggregateCall<logical.Plan>,
): AggregateCall<logical.Plan> {
  return { ...call, arguments: simplifyAll(call.arguments) };
}

/** `expression` with constants folded and the identities above applied, bottom up. */
export function simplifyExpression(
  expression: logical.Expression,
): logical.Expression {
  const rebuilt = rebuildChildren(expression, {
    expression: (child) => simplifyExpression(child),
    plan: (plan) => plan,
  });
  return folded(rebuilt) ?? withIdentities(rebuilt);
}

/** The constant `expression` evaluates to, or null if it is not one. */
function folded(expression: logical.Expression): logical.Expression | null {
  if (expression.kind === "constant") return null;
  const value = evaluateConstant(expression);
  if (value === undefined) return null;
  return { kind: "constant", value, type: expression.type };
}

function withIdentities(expression: logical.Expression): logical.Expression {
  if (expression.kind === "conjunction") {
    return expression.operator === "AND"
      ? simplifyAnd(expression)
      : simplifyOr(expression);
  }
  if (expression.kind === "not") {
    const { operand } = expression;
    const isDoubleNegation =
      operand.kind === "not" && isCondition(operand.operand);
    return isDoubleNegation ? operand.operand : expression;
  }
  return expression;
}

type Conjunction = Extract<
  logical.Expression,
  { readonly kind: "conjunction" }
>;

/** FALSE AND x → FALSE; TRUE AND x → x. */
function simplifyAnd(expression: Conjunction): logical.Expression {
  const operands = expression.operands.flatMap((operand) =>
    conjunctsOf(operand),
  );
  if (operands.some((operand) => isAlwaysFalse(operand))) return truth(false);
  return withOperands(expression, operands, (operand) => isAlwaysTrue(operand));
}

/** TRUE OR x → TRUE; FALSE OR x → x. */
function simplifyOr(expression: Conjunction): logical.Expression {
  const operands = expression.operands.flatMap((operand) =>
    disjunctsOf(operand),
  );
  if (operands.some((operand) => isAlwaysTrue(operand))) return truth(true);
  return withOperands(expression, operands, (operand) =>
    isAlwaysFalse(operand),
  );
}

/**
 * The conjunction of `operands` without the neutral ones: the neutral
 * constant if none is left, the one left if it is a condition, else the
 * conjunction as it was.
 */
function withOperands(
  expression: Conjunction,
  operands: readonly logical.Expression[],
  isNeutral: (operand: logical.Expression) => boolean,
): logical.Expression {
  const kept = operands.filter((operand) => !isNeutral(operand));
  const [only] = kept;
  if (only === undefined) return truth(expression.operator === "AND");
  if (kept.length > 1) return { ...expression, operands: kept };
  return isCondition(only) ? only : expression;
}

/** True if `expression` gives only TRUE, FALSE or NULL. */
function isCondition(expression: logical.Expression): boolean {
  switch (expression.kind) {
    case "comparison":
    case "conjunction":
    case "not":
    case "is-null":
    case "in-list":
    case "between":
    case "exists":
    case "quantified-comparison": {
      return true;
    }
    case "constant": {
      return expression.value === null || typeof expression.value === "boolean";
    }
    case "column":
    case "parameter":
    case "arithmetic":
    case "negate":
    case "concatenate":
    case "function":
    case "case":
    case "cast":
    case "json-access":
    case "scalar-subquery": {
      return false;
    }
  }
}

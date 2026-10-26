import { SemanticError } from "../../errors.js";
import {
  ExpressionClass,
  type CaseExpression,
  type ParsedExpression,
} from "../parser/index.js";
import type { logical } from "../plan/index.js";
import { bindCall } from "./calls.js";
import { bindCondition, type AggregateRule, type Clause } from "./clause.js";
import { bindColumn } from "./names.js";
import {
  bindBetween,
  bindComparison,
  bindConjunction,
  bindOperator,
} from "./operators.js";
import type { Scope } from "./scope.js";
import { bindSubquery } from "./subquery.js";
import { checkComparable, declaredType, sharedType } from "./type-rules.js";

// ---------------------------------------------------------------------------
// Expressions: each node bound to the plan's expressions and typed. The
// operators, calls and subqueries have files of their own; they bind their
// operands through the clause, which leads back here.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

/** A clause of `scope` with `aggregates` as its rule for aggregate calls. */
export function clauseOf(scope: Scope, aggregates: AggregateRule): Clause {
  const clause: Clause = {
    scope,
    aggregates,
    bind: (expression) => bindNode(expression, clause),
    withAggregates: (rule) => clauseOf(scope, rule),
  };
  return clause;
}

function bindNode(expression: ParsedExpression, clause: Clause): Expression {
  switch (expression.expressionClass) {
    case ExpressionClass.COLUMN_REF: {
      return bindColumn(expression, clause.scope);
    }
    case ExpressionClass.CONSTANT: {
      return { kind: "constant", value: expression.value, type: expression.type };
    }
    case ExpressionClass.PARAMETER: {
      clause.scope.context.noteParameter(expression.index);
      return { kind: "parameter", index: expression.index, type: "ANY" };
    }
    case ExpressionClass.COMPARISON: {
      return bindComparison(expression, clause);
    }
    case ExpressionClass.CONJUNCTION: {
      return bindConjunction(expression, clause);
    }
    case ExpressionClass.OPERATOR: {
      return bindOperator(expression, clause);
    }
    case ExpressionClass.BETWEEN: {
      return bindBetween(expression, clause);
    }
    case ExpressionClass.FUNCTION: {
      return bindCall(expression, clause);
    }
    case ExpressionClass.SUBQUERY: {
      return bindSubquery(expression, clause);
    }
    case ExpressionClass.CASE: {
      return bindCase(expression, clause);
    }
    case ExpressionClass.CAST: {
      const operand = clause.bind(expression.child);
      return { kind: "cast", operand, type: declaredType(expression.castType) };
    }
    case ExpressionClass.STAR: {
      throw new SemanticError(
        "* is allowed only as a select list item and in count(*)",
      );
    }
  }
}

/**
 * CASE. With an operand, each WHEN value is compared with it as `=` would;
 * without, each WHEN is a condition. The results share one type.
 */
function bindCase(expression: CaseExpression, clause: Clause): Expression {
  const operand =
    expression.operand === null ? null : clause.bind(expression.operand);
  const branches = expression.caseChecks.map((check) => ({
    when: bindWhen(check.whenExpression, operand, clause),
    result: clause.bind(check.thenExpression),
  }));
  const otherwise =
    expression.elseExpression === null
      ? null
      : clause.bind(expression.elseExpression);
  const results = [
    ...branches.map((branch) => branch.result),
    ...(otherwise === null ? [] : [otherwise]),
  ];
  const type = sharedType(results.map((result) => result.type));
  return { kind: "case", operand, branches, otherwise, type };
}

function bindWhen(
  when: ParsedExpression,
  operand: Expression | null,
  clause: Clause,
): Expression {
  if (operand === null) return bindCondition(when, clause, "a CASE WHEN");
  const value = clause.bind(when);
  checkComparable(operand.type, value.type);
  return value;
}

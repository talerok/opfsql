import { SemanticError } from "../../errors.js";
import { commonType } from "../../functions/index.js";
import {
  ExpressionClass,
  type CaseExpression,
  type ParsedExpression,
} from "../parser/index.js";
import type { logical } from "../plan/index.js";
import { comparedWith } from "./affinity.js";
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
import { checkComparable, declaredType } from "./type-rules.js";

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
      return {
        kind: "constant",
        value: expression.value,
        type: expression.type,
      };
    }
    case ExpressionClass.PARAMETER: {
      clause.scope.context.noteParameter(expression.index);
      const { index } = expression;
      return { kind: "parameter", index, affinity: null, type: "ANY" };
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
 * without, each WHEN is a condition. The result has the type the results
 * share, ANY for a mix of kinds (docs/semantics.md, Functions).
 */
function bindCase(expression: CaseExpression, clause: Clause): Expression {
  const compared = bindOptional(expression.operand, clause);
  const branches = expression.caseChecks.map((check) => ({
    when: bindWhen(check.whenExpression, compared, clause),
    result: clause.bind(check.thenExpression),
  }));
  const whenTypes = branches.map((branch) => branch.when.type);
  const operand = compared === null ? null : comparedWith(compared, whenTypes);
  const otherwise = bindOptional(expression.elseExpression, clause);
  const results = [
    ...branches.map((branch) => branch.result),
    ...(otherwise === null ? [] : [otherwise]),
  ];
  const type = commonType(results.map((result) => result.type));
  return { kind: "case", operand, branches, otherwise, type };
}

function bindOptional(
  expression: ParsedExpression | null,
  clause: Clause,
): Expression | null {
  return expression === null ? null : clause.bind(expression);
}

function bindWhen(
  when: ParsedExpression,
  operand: Expression | null,
  clause: Clause,
): Expression {
  if (operand === null) return bindCondition(when, clause, "a CASE WHEN");
  const value = clause.bind(when);
  checkComparable(operand.type, value.type);
  return comparedWith(value, [operand.type]);
}

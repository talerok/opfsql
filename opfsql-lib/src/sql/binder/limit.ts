import { DatatypeMismatchError, SemanticError } from "../../errors.js";
import { assignValue, type Value } from "../../values/index.js";
import {
  ExpressionClass,
  type LimitClause,
  type ParsedExpression,
} from "../parser/index.js";
import type { logical } from "../plan/index.js";
import type { BindContext, BoundQuery } from "./scope.js";

// ---------------------------------------------------------------------------
// LIMIT and OFFSET (docs/semantics.md, LIMIT and OFFSET): integers, by the
// conversion an INTEGER column applies. A constant is converted here, so
// `LIMIT '5'` is 5 and `LIMIT 1.5` fails when the statement is prepared; a
// parameter is converted by the executor when the query starts. A negative
// count stays as written: the executor reads it as no limit, or none
// skipped. Any other expression is refused.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

export function applyLimit(
  query: BoundQuery,
  clause: LimitClause | null,
  context: BindContext,
): BoundQuery {
  if (clause === null) return query;
  const limit = countOf(clause.limit, "LIMIT", context);
  const offset = countOf(clause.offset, "OFFSET", context);
  const plan: logical.Plan = {
    kind: "limit",
    child: query.plan,
    limit,
    offset,
  };
  return { ...query, plan };
}

/** A count as the limit node holds it; null when the clause does not give it. */
function countOf(
  expression: ParsedExpression | null,
  clause: "LIMIT" | "OFFSET",
  context: BindContext,
): Expression | null {
  if (expression === null) return null;
  if (expression.expressionClass === ExpressionClass.PARAMETER) {
    context.noteParameter(expression.index);
    const { index } = expression;
    return { kind: "parameter", index, affinity: null, type: "INTEGER" };
  }
  if (expression.expressionClass !== ExpressionClass.CONSTANT) {
    throw new SemanticError(`${clause} must be a constant or a parameter`);
  }
  const value = integerOf(expression.value, clause);
  return { kind: "constant", value, type: "INTEGER" };
}

/** A constant count converted as an INTEGER column converts it; NULL is refused. */
function integerOf(value: Value, clause: string): number {
  const converted = assignedInteger(value, clause);
  if (typeof converted !== "number") {
    throw new DatatypeMismatchError(`${clause} must be an integer, not NULL`);
  }
  return converted;
}

function assignedInteger(value: Value, clause: string): Value {
  try {
    return assignValue(value, "INTEGER");
  } catch (error) {
    if (!(error instanceof DatatypeMismatchError)) throw error;
    throw new DatatypeMismatchError(`${clause}: ${error.message}`, {
      cause: error,
    });
  }
}

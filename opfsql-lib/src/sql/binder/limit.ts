import { SemanticError } from "../../errors.js";
import {
  ExpressionClass,
  type LimitClause,
  type ParsedExpression,
} from "../parser/index.js";
import type { logical } from "../plan/index.js";
import type { BindContext, BoundQuery } from "./scope.js";

// ---------------------------------------------------------------------------
// LIMIT and OFFSET: an integer constant or a parameter, whose value the run
// supplies. As in SQLite, a negative LIMIT means no limit and a negative
// OFFSET skips none; anything else that is not an integer is an error.
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
  if (limit === null && offset === null) return query;
  const plan: logical.Plan = {
    kind: "limit",
    child: query.plan,
    limit,
    offset,
  };
  return { ...query, plan };
}

/** A count as the limit node holds it: null for no limit, or no offset. */
function countOf(
  expression: ParsedExpression | null,
  clause: "LIMIT" | "OFFSET",
  context: BindContext,
): Expression | null {
  if (expression === null) return null;
  if (expression.expressionClass === ExpressionClass.PARAMETER) {
    context.noteParameter(expression.index);
    return { kind: "parameter", index: expression.index, type: "INTEGER" };
  }
  const value =
    expression.expressionClass === ExpressionClass.CONSTANT
      ? expression.value
      : undefined;
  const isInteger = typeof value === "number" && Number.isSafeInteger(value);
  if (!isInteger) {
    throw new SemanticError(
      `${clause} must be an integer constant or a parameter`,
    );
  }
  const isNone = value < 0 || (clause === "OFFSET" && value === 0);
  if (isNone) return null;
  return { kind: "constant", value, type: "INTEGER" };
}

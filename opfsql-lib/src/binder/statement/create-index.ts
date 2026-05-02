import type { CreateIndexStatement } from "../../parser/types.js";
import { boundToIndexExpression } from "../../store/index-expression.js";
import type { IndexExpression } from "../../types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { requireTable } from "../core/utils/require-table.js";
import { bindExpression } from "../expression/index.js";
import type * as BT from "../types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../types.js";

export function bindCreateIndex(
  ctx: BindContext,
  stmt: CreateIndexStatement,
): BT.LogicalCreateIndex {
  const schema = requireTable(ctx, stmt.table_name);

  if (stmt.index_name.toLowerCase().startsWith("__pk_")) {
    throw new BindError(
      `Index name "${stmt.index_name}" uses reserved prefix "__pk_"`,
    );
  }

  // Create a mini bind scope with just the indexed table
  const scope = ctx.createScope();
  scope.addTable(stmt.table_name, stmt.table_name, schema);

  const indexExpressions: IndexExpression[] = [];

  for (const parsedExpr of stmt.expressions) {
    // Bind via standard expression binder
    const bound = bindExpression(ctx, parsedExpr, scope);

    // Reject BLOB — never valid as index key
    if (bound.returnType === "BLOB") {
      throw new BindError(
        "Index expression must return a scalar type, got BLOB",
      );
    }

    // Reject bare JSON column reference (always an object).
    // JSON path access (data.city) is allowed — may resolve to scalar at runtime.
    if (
      bound.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF &&
      bound.returnType === "JSON"
    ) {
      throw new BindError(
        "Cannot create index on a JSON column directly; use a path expression (e.g. data.field)",
      );
    }

    // Validate: no aggregates, subqueries, or parameters
    validateIndexExpression(bound);

    // Convert to catalog format
    indexExpressions.push(boundToIndexExpression(bound, schema));
  }

  return {
    type: LogicalOperatorType.LOGICAL_CREATE_INDEX,
    index: {
      name: stmt.index_name,
      tableName: stmt.table_name,
      expressions: indexExpressions,
      unique: stmt.is_unique,
    },
    ifNotExists: stmt.if_not_exists,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    columnBindings: [],
  };
}

function validateIndexExpression(expr: BT.BoundExpression): void {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_AGGREGATE:
      throw new BindError(
        "Index expressions cannot contain aggregate functions",
      );
    case BoundExpressionClass.BOUND_SUBQUERY:
      throw new BindError("Index expressions cannot contain subqueries");
    case BoundExpressionClass.BOUND_PARAMETER:
      throw new BindError("Index expressions cannot contain parameters");
    case BoundExpressionClass.BOUND_COLUMN_REF:
    case BoundExpressionClass.BOUND_CONSTANT:
      return;
    case BoundExpressionClass.BOUND_COMPARISON: {
      const cmp = expr as BT.BoundComparisonExpression;
      validateIndexExpression(cmp.left);
      validateIndexExpression(cmp.right);
      return;
    }
    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const conj = expr as BT.BoundConjunctionExpression;
      for (const child of conj.children) validateIndexExpression(child);
      return;
    }
    case BoundExpressionClass.BOUND_BETWEEN: {
      const bet = expr as BT.BoundBetweenExpression;
      validateIndexExpression(bet.input);
      validateIndexExpression(bet.lower);
      validateIndexExpression(bet.upper);
      return;
    }
    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      const ja = expr as BT.BoundJsonAccessExpression;
      validateIndexExpression(ja.child);
      return;
    }
    case BoundExpressionClass.BOUND_FUNCTION: {
      const fn = expr as BT.BoundFunctionExpression;
      for (const child of fn.children) validateIndexExpression(child);
      return;
    }
    case BoundExpressionClass.BOUND_CAST: {
      const cast = expr as BT.BoundCastExpression;
      validateIndexExpression(cast.child);
      return;
    }
    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BT.BoundOperatorExpression;
      for (const child of op.children) validateIndexExpression(child);
      return;
    }
    case BoundExpressionClass.BOUND_CASE: {
      const caseExpr = expr as BT.BoundCaseExpression;
      for (const check of caseExpr.caseChecks) {
        validateIndexExpression(check.when);
        validateIndexExpression(check.then);
      }
      if (caseExpr.elseExpr) validateIndexExpression(caseExpr.elseExpr);
      return;
    }
  }
}

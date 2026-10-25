import type { StarExpression } from "../../../sql/parser/types.js";
import type { BoundColumnRefExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";
import type { AggregateContext } from "../core/context.js";
import type { BindScope } from "../core/scope.js";
import { BindError } from "../core/errors.js";
import { sameExpression } from "./same-expression.js";

export function bindStar(
  expr: StarExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundColumnRefExpression[] {
  const result: BoundColumnRefExpression[] = [];

  const entries = expr.table
    ? ([scope.findByAlias(expr.table)].filter(Boolean) as Array<
        NonNullable<ReturnType<typeof scope.findByAlias>>
      >)
    : scope.getAllBindings();

  if (expr.table && entries.length === 0) {
    throw new BindError(`Unknown table alias "${expr.table}"`);
  }

  for (const entry of entries) {
    for (let i = 0; i < entry.schema.columns.length; i++) {
      const col = entry.schema.columns[i];
      result.push({
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: entry.tableIndex, columnIndex: i },
        tableName: entry.tableName,
        columnName: col.name,
        returnType: col.type,
      });
    }
  }

  if (aggCtx && (aggCtx.groups.length > 0 || aggCtx.aggregates.length > 0)) {
    for (let j = 0; j < result.length; j++) {
      const groupIdx = aggCtx.groups.findIndex((g) =>
        sameExpression(g, result[j]),
      );
      if (groupIdx === -1) {
        throw new BindError(
          `Column "${result[j].columnName}" must appear in the GROUP BY clause or be used in an aggregate function`,
        );
      }
      result[j] = {
        ...result[j],
        binding: { tableIndex: aggCtx.groupIndex, columnIndex: groupIdx },
      };
    }
  }

  return result;
}

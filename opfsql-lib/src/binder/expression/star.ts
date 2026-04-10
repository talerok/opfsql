import type { StarExpression } from '../../parser/types.js';
import type { BoundColumnRefExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { sameExpression } from './same-expression.js';

export function bindStar(
  expr: StarExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundColumnRefExpression[] {
  const result: BoundColumnRefExpression[] = [];

  const entries = expr.table_name
    ? [scope.findByAlias(expr.table_name)].filter(Boolean) as Array<NonNullable<ReturnType<typeof scope.findByAlias>>>
    : scope.getAllBindings();

  if (expr.table_name && entries.length === 0) {
    throw new BindError(`Unknown table alias "${expr.table_name}"`);
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
    for (const col of result) {
      if (!aggCtx.groups.some((g) => sameExpression(g, col))) {
        throw new BindError(
          `Column "${col.columnName}" must appear in the GROUP BY clause or be used in an aggregate function`,
        );
      }
    }
  }

  return result;
}

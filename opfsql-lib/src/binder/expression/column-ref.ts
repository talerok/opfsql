import type { ColumnRefExpression } from '../../parser/types.js';
import type { BoundColumnRefExpression } from '../types.js';
import type { AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { sameExpression } from './same-expression.js';

export function bindColumnRef(
  expr: ColumnRefExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundColumnRefExpression {
  const resolved = expr.column_names.length === 2
    ? scope.resolveColumn(expr.column_names[1], expr.column_names[0])
    : scope.resolveColumn(expr.column_names[0]);

  if (aggCtx && (aggCtx.groups.length > 0 || aggCtx.aggregates.length > 0)) {
    if (!aggCtx.groups.some((g) => sameExpression(g, resolved))) {
      throw new BindError(
        `Column "${resolved.columnName}" must appear in the GROUP BY clause or be used in an aggregate function`,
      );
    }
  }

  return resolved;
}

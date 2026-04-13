import type { ColumnRefExpression, JsonPathSegment } from '../../parser/types.js';
import type { BoundColumnRefExpression, BoundExpression, BoundJsonAccessExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { sameExpression } from './same-expression.js';

export function bindColumnRef(
  expr: ColumnRefExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundExpression {
  const names = expr.column_names;

  // Resolve column: try first 2 as table.column, then first 1 as column
  let resolved: BoundColumnRefExpression;
  let extraNameStart: number;

  if (names.length >= 2) {
    // Try table.column first
    const tableAlias = names[0];
    const entry = scope.findByAlias(tableAlias);
    if (entry) {
      resolved = scope.resolveColumn(names[1], names[0]);
      extraNameStart = 2;
    } else {
      // No table match — resolve names[0] as column
      resolved = scope.resolveColumn(names[0]);
      extraNameStart = 1;
    }
  } else {
    resolved = scope.resolveColumn(names[0]);
    extraNameStart = 1;
  }

  // Build JSON path from remaining column_names + parsed path segments
  const jsonPath: JsonPathSegment[] = [];
  for (let i = extraNameStart; i < names.length; i++) {
    jsonPath.push({ type: 'field', name: names[i] });
  }
  if (expr.path) {
    jsonPath.push(...expr.path);
  }

  // Handle aggregate context for the base column ref
  if (aggCtx && (aggCtx.groups.length > 0 || aggCtx.aggregates.length > 0)) {
    const groupIdx = aggCtx.groups.findIndex((g) => sameExpression(g, resolved));
    if (groupIdx === -1) {
      throw new BindError(
        `Column "${resolved.columnName}" must appear in the GROUP BY clause or be used in an aggregate function`,
      );
    }
    resolved = {
      ...resolved,
      binding: { tableIndex: aggCtx.groupIndex, columnIndex: groupIdx },
    };
  }

  // If there's a JSON path, wrap in BoundJsonAccessExpression
  if (jsonPath.length > 0) {
    if (resolved.returnType !== 'JSON') {
      throw new BindError(
        `Cannot access field on non-JSON column "${resolved.columnName}" (type: ${resolved.returnType})`,
      );
    }
    return {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: resolved,
      path: jsonPath,
      returnType: 'JSON',
    } satisfies BoundJsonAccessExpression;
  }

  return resolved;
}

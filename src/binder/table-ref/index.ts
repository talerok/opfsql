import type { TableRef, BaseTableRef, JoinRef, SubqueryRef } from '../../parser/types.js';
import { TableRefType } from '../../parser/types.js';
import type { LogicalOperator } from '../types.js';
import type { BindContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { bindBaseTableRef } from './base-table.js';
import { bindJoinRef } from './join.js';
import { bindSubqueryRef } from './subquery.js';

export function bindTableRef(
  ctx: BindContext,
  ref: TableRef,
  scope: BindScope,
): LogicalOperator {
  switch (ref.type) {
    case TableRefType.BASE_TABLE:
      return bindBaseTableRef(ctx, ref as BaseTableRef, scope);
    case TableRefType.JOIN:
      return bindJoinRef(ctx, ref as JoinRef, scope);
    case TableRefType.SUBQUERY:
      return bindSubqueryRef(ctx, ref as SubqueryRef, scope);
    default:
      throw new BindError('Unknown table reference type');
  }
}

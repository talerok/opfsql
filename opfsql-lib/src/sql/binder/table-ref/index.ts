import type {
  TableReference,
  BaseTableReference,
  JoinReference,
  SubqueryReference,
} from "../../parser/types.js";
import { TableReferenceType } from "../../parser/types.js";
import type { LogicalOperator } from "../types.js";
import type { BindContext } from "../core/context.js";
import type { BindScope } from "../core/scope.js";
import { BindError } from "../core/errors.js";
import { bindBaseTableRef } from "./base-table.js";
import { bindJoinRef } from "./join.js";
import { bindSubqueryRef } from "./subquery.js";

export function bindTableRef(
  ctx: BindContext,
  ref: TableReference,
  scope: BindScope,
): LogicalOperator {
  switch (ref.type) {
    case TableReferenceType.BASE_TABLE:
      return bindBaseTableRef(ctx, ref as BaseTableReference, scope);
    case TableReferenceType.JOIN:
      return bindJoinRef(ctx, ref as JoinReference, scope);
    case TableReferenceType.SUBQUERY:
      return bindSubqueryRef(ctx, ref as SubqueryReference, scope);
    default:
      throw new BindError("Unknown table reference type");
  }
}

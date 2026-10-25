import type {
  SelectNode,
  SelectStatement,
  SetOperationNode,
} from "../../parser/types.js";
import type { BindContext } from "../core/context.js";
import type { BindScope } from "../core/scope.js";
import type { LogicalOperator } from "../types.js";
import { NO_CLAUSES, type QueryClauses } from "./query-clauses.js";
import { bindSelect } from "./select.js";
import { bindSetOperation } from "./set-operation.js";

export function bindQueryNode(
  context: BindContext,
  query: SelectStatement,
  scope: BindScope,
): LogicalOperator {
  return bindQueryBody(context, query.body, scope, query);
}

export function bindQueryBody(
  context: BindContext,
  body: SelectNode | SetOperationNode,
  scope: BindScope,
  clauses: QueryClauses = NO_CLAUSES,
): LogicalOperator {
  if (body.type === "SELECT_NODE") {
    return bindSelect(context, body, scope, clauses);
  }
  return bindSetOperation(context, body, scope, clauses);
}

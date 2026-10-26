import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import {
  SetOperationType,
  type OrderByNode,
  type ParsedExpression,
  type SelectNode,
  type SetOperationNode,
} from "../parser/index.js";
import type { logical, OutputColumn } from "../plan/index.js";
import { readColumns } from "./columns.js";
import { bareName, keyOrder, literalPosition } from "./order-by.js";
import type { BoundQuery, Scope } from "./scope.js";
import { bindSelect, order } from "./select.js";
import { commonType } from "../../functions/index.js";

// ---------------------------------------------------------------------------
// UNION, UNION ALL, INTERSECT and EXCEPT. Each member is a SELECT of its own
// beside the others; their column counts must agree, and each column's type
// is the type its values share, ANY for a mix of kinds. The result has a
// table index of its own,
// and ORDER BY can only name its columns: by position or by name.
// ---------------------------------------------------------------------------

/** SQLite's SQLITE_MAX_COMPOUND_SELECT (docs/semantics.md, Limits). */
const MAX_COMPOUND_SELECTS = 500;

export function bindCompound(
  node: SetOperationNode,
  scope: Scope,
  orderBy: readonly OrderByNode[],
): BoundQuery {
  checkSelectCount(node);
  const query = bindMembers(node, scope);
  const columns = readColumns(query.plan, query.columns);
  const keys = orderBy.map((item) => ({
    position: outputPosition(item.expression, query.columns),
    ...keyOrder(item),
  }));
  return { ...query, plan: order(query.plan, keys, columns) };
}

function checkSelectCount(node: SetOperationNode): void {
  let count = 1;
  let member: SelectNode | SetOperationNode = node;
  for (; member.type === "SET_OPERATION_NODE"; member = member.left) count++;
  if (count > MAX_COMPOUND_SELECTS) {
    throw new SemanticError(
      `a compound SELECT joins at most ${String(MAX_COMPOUND_SELECTS)} SELECTs, not ${String(count)}`,
    );
  }
}

function bindMembers(node: SetOperationNode, scope: Scope): BoundQuery {
  const left = bindMember(node.left, scope);
  const right = bindSelect(node.right, scope.sibling(), []);
  const operator = operatorOf(node.setOperationType);
  if (left.columns.length !== right.columns.length) {
    throw new SemanticError(
      `each side of ${operator} must have as many columns: ${String(left.columns.length)} and ${String(right.columns.length)}`,
    );
  }
  const columns = left.columns.map((column, position): OutputColumn => {
    const other = right.columns[position]?.type ?? column.type;
    return { name: column.name, type: commonType([column.type, other]) };
  });
  const plan: logical.Plan = {
    kind: "set-operation",
    operator,
    tableIndex: scope.context.newIndex(),
    left: left.plan,
    right: right.plan,
    types: columns.map((column) => column.type),
  };
  return { plan, columns };
}

function bindMember(
  member: SelectNode | SetOperationNode,
  scope: Scope,
): BoundQuery {
  if (member.type === "SELECT_NODE") {
    return bindSelect(member, scope.sibling(), []);
  }
  return bindMembers(member, scope);
}

export function operatorOf(
  type: SetOperationType,
): logical.SetOperation["operator"] {
  switch (type) {
    case SetOperationType.UNION: {
      return "UNION";
    }
    case SetOperationType.UNION_ALL: {
      return "UNION ALL";
    }
    case SetOperationType.INTERSECT: {
      return "INTERSECT";
    }
    case SetOperationType.EXCEPT: {
      return "EXCEPT";
    }
  }
}

/** The result column an ORDER BY key of a compound query names. */
function outputPosition(
  expression: ParsedExpression,
  columns: readonly OutputColumn[],
): number {
  const position = literalPosition(expression, columns.length, "ORDER BY");
  if (position !== null) return position;
  const name = bareName(expression);
  const key = name === null ? null : nameKey(name);
  const found = columns.findIndex((column) => nameKey(column.name) === key);
  if (found === -1) {
    throw new SemanticError(
      "an ORDER BY key of a compound query must be one of its result columns",
    );
  }
  return found;
}

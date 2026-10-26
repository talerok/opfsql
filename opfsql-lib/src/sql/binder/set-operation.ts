import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import {
  compoundMembers,
  SetOperationType,
  type CompoundLink,
  type OrderByNode,
  type ParsedExpression,
  type SetOperationNode,
} from "../parser/index.js";
import type { logical, OutputColumn } from "../plan/index.js";
import { readColumns } from "./columns.js";
import { bareName, keyOrder, literalPosition } from "./order-by.js";
import type { BoundQuery, Scope } from "./scope.js";
import { bindSelect, order } from "./select.js";
import { commonType } from "../../functions/index.js";
import { itemAt } from "./one.js";

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
  const { first, links } = compoundMembers(node);
  const count = links.length + 1;
  if (count > MAX_COMPOUND_SELECTS) {
    throw new SemanticError(
      `a compound SELECT joins at most ${String(MAX_COMPOUND_SELECTS)} SELECTs, not ${String(count)}`,
    );
  }
  let query = bindSelect(first, scope.sibling(), []);
  for (const link of links) query = joinOn(query, link, scope);
  const columns = readColumns(query.plan, query.columns);
  const keys = orderBy.map((item) => ({
    position: outputPosition(item.expression, query.columns),
    ...keyOrder(item),
  }));
  return { ...query, plan: order(query.plan, keys, columns) };
}

/** The compound so far, `left`, with the next SELECT joined on by its operator. */
function joinOn(
  left: BoundQuery,
  link: CompoundLink,
  scope: Scope,
): BoundQuery {
  const right = bindSelect(link.select, scope.sibling(), []);
  const operator = operatorOf(link.operator);
  if (left.columns.length !== right.columns.length) {
    throw new SemanticError(
      `each side of ${operator} must have as many columns: ${String(left.columns.length)} and ${String(right.columns.length)}`,
    );
  }
  const columns = left.columns.map((column, position): OutputColumn => {
    const other = itemAt(right.columns, position, `${operator}'s right side`);
    return { name: column.name, type: commonType([column.type, other.type]) };
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

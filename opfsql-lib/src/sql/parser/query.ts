// ---------------------------------------------------------------------------
// Queries: WITH, SELECT, compound operators, ORDER BY and LIMIT.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import { parseFrom } from "./from-clause.js";
import { parseSubquery } from "./primary.js";
import {
  ExpressionClass,
  OrderByNullType,
  OrderType,
  ResultModifierType,
  SetOperationType,
  StatementType,
  type CTEMap,
  type CTENode,
  type LimitModifier,
  type OrderByNode,
  type ParsedExpression,
  type ResultModifier,
  type SelectNode,
  type SelectStatement,
  type SetOperationNode,
  type StarExpression,
} from "./types.js";

const NO_CTES: CTEMap = { ctes: [], recursive: false };

/** A whole query; WITH, ORDER BY and LIMIT attach to its outermost node. */
export function parseQuery(cursor: Cursor): SelectStatement {
  cursor.descend();
  const cteMap = cursor.isKeyword("WITH") ? parseWith(cursor) : NO_CTES;
  const node = parseCompound(cursor);
  const modifiers = [...node.modifiers, ...parseTrailingModifiers(cursor)];
  cursor.ascend();
  return {
    type: StatementType.SELECT_STATEMENT,
    node: { ...node, modifiers, cteMap },
  };
}

// --- WITH ----------------------------------------------------------------------

function parseWith(cursor: Cursor): CTEMap {
  cursor.next();
  // `WITH recursive AS (…)` names a CTE "recursive".
  const isRecursive =
    cursor.isKeyword("RECURSIVE") &&
    !cursor.isKeyword("AS", 1) &&
    !cursor.isSymbol("(", 1);
  if (isRecursive) cursor.next();
  // The binder nests one plan node per CTE, so each counts as a level.
  let levels = 0;
  const ctes = cursor.list(() => {
    cursor.descend();
    levels++;
    return parseCte(cursor);
  });
  cursor.ascend(levels);
  const next = cursor.peek();
  if (next.kind === "keyword" && DML_KEYWORDS.has(next.value)) {
    throw new NotSupportedError(`WITH before ${next.value}`);
  }
  return { ctes, recursive: isRecursive };
}

const DML_KEYWORDS: ReadonlySet<string> = new Set([
  "INSERT",
  "UPDATE",
  "DELETE",
]);

function parseCte(cursor: Cursor): CTENode {
  const name = cursor.name("a name for the WITH query");
  const aliases = cursor.isSymbol("(") ? cursor.nameList("a column name") : [];
  cursor.expectKeyword("AS");
  if (cursor.isWord("MATERIALIZED") || cursor.isKeyword("NOT")) {
    throw new NotSupportedError("[NOT] MATERIALIZED");
  }
  return { name, query: parseSubquery(cursor), aliases };
}

// --- Compound queries ------------------------------------------------------------

function parseCompound(cursor: Cursor): SelectNode | SetOperationNode {
  let node: SelectNode | SetOperationNode = parseSelectCore(cursor);
  let levels = 0;
  for (let type = parseSetOperator(cursor); type !== undefined;) {
    // A chain of compound operators makes a tree as deep as a nesting would.
    cursor.descend();
    levels++;
    node = {
      type: "SET_OPERATION_NODE",
      setOperationType: type,
      left: node,
      right: parseSelectCore(cursor),
      modifiers: [],
      cteMap: NO_CTES,
    };
    type = parseSetOperator(cursor);
  }
  cursor.ascend(levels);
  return node;
}

function parseSetOperator(cursor: Cursor): SetOperationType | undefined {
  if (cursor.matchKeyword("UNION")) {
    return cursor.matchKeyword("ALL")
      ? SetOperationType.UNION_ALL
      : SetOperationType.UNION;
  }
  let type: SetOperationType | undefined;
  if (cursor.matchKeyword("INTERSECT")) {
    type = SetOperationType.INTERSECT;
  } else if (cursor.matchKeyword("EXCEPT")) {
    type = SetOperationType.EXCEPT;
  }
  if (type !== undefined && cursor.isKeyword("ALL")) {
    throw new NotSupportedError(`${type} ALL`);
  }
  return type;
}

// --- SELECT ----------------------------------------------------------------------

function parseSelectCore(cursor: Cursor): SelectNode {
  if (cursor.isKeyword("VALUES"))
    throw new NotSupportedError("VALUES as a query");
  cursor.expectKeyword("SELECT");
  const isDistinct = cursor.matchKeyword("DISTINCT");
  if (!isDistinct) cursor.matchKeyword("ALL");
  const selectList = cursor.list(() => parseSelectItem(cursor));
  const fromTable = cursor.matchKeyword("FROM") ? parseFrom(cursor) : null;
  const whereClause = parseOptional(cursor, "WHERE");
  const groupExpressions = parseGroupBy(cursor);
  const having = parseOptional(cursor, "HAVING");
  if (cursor.isKeyword("WINDOW")) throw new NotSupportedError("WINDOW");
  const modifiers: ResultModifier[] = isDistinct
    ? [{ type: ResultModifierType.DISTINCT_MODIFIER }]
    : [];
  return {
    type: "SELECT_NODE",
    selectList,
    fromTable,
    whereClause,
    groups: { groupExpressions },
    having,
    modifiers,
    cteMap: NO_CTES,
  };
}

function parseOptional(
  cursor: Cursor,
  keyword: "WHERE" | "HAVING",
): ParsedExpression | null {
  return cursor.matchKeyword(keyword) ? cursor.expression() : null;
}

/** `*`, `t.*`, or an expression with an optional alias. */
function parseSelectItem(cursor: Cursor): ParsedExpression {
  if (cursor.matchSymbol("*")) return star(null);
  if (cursor.isName() && cursor.isSymbol(".", 1) && cursor.isSymbol("*", 2)) {
    const table = cursor.name("a table name");
    cursor.next();
    cursor.next();
    return star(table);
  }
  const expression = cursor.expression();
  const alias = cursor.matchKeyword("AS")
    ? cursor.name("an alias")
    : cursor.matchAlias();
  return alias === null ? expression : { ...expression, alias };
}

function star(tableName: string | null): StarExpression {
  return { expressionClass: ExpressionClass.STAR, alias: null, tableName };
}

function parseGroupBy(cursor: Cursor): readonly ParsedExpression[] {
  if (!cursor.matchKeyword("GROUP")) return [];
  cursor.expectKeyword("BY");
  return cursor.list(() => cursor.expression());
}

// --- ORDER BY and LIMIT --------------------------------------------------------------

function parseTrailingModifiers(cursor: Cursor): ResultModifier[] {
  const modifiers: ResultModifier[] = [];
  if (cursor.matchKeyword("ORDER")) {
    cursor.expectKeyword("BY");
    const orders = cursor.list(() => parseOrderItem(cursor));
    modifiers.push({ type: ResultModifierType.ORDER_MODIFIER, orders });
  }
  const limit = parseLimit(cursor);
  if (limit !== undefined) modifiers.push(limit);
  return modifiers;
}

function parseOrderItem(cursor: Cursor): OrderByNode {
  const expression = cursor.expression();
  const isDescending = cursor.matchKeyword("DESC");
  if (!isDescending) cursor.matchKeyword("ASC");
  let nullOrder: OrderByNullType | null = null;
  if (cursor.matchKeyword("NULLS")) {
    if (cursor.matchKeyword("FIRST")) {
      nullOrder = OrderByNullType.NULLS_FIRST;
    } else if (cursor.matchKeyword("LAST")) {
      nullOrder = OrderByNullType.NULLS_LAST;
    } else {
      cursor.fail("FIRST or LAST");
    }
  }
  const type = isDescending ? OrderType.DESCENDING : OrderType.ASCENDING;
  return { type, nullOrder, expression };
}

/**
 * `LIMIT n [OFFSET m]`, SQLite's `LIMIT m, n`, and PostgreSQL's `OFFSET m`
 * with or without a LIMIT after it.
 */
function parseLimit(cursor: Cursor): LimitModifier | undefined {
  let limit: ParsedExpression | null = null;
  let offset: ParsedExpression | null = null;
  if (cursor.matchKeyword("LIMIT")) {
    limit = cursor.expression();
    if (cursor.matchSymbol(",")) {
      offset = limit;
      limit = cursor.expression();
    }
  }
  if (offset === null && cursor.matchKeyword("OFFSET")) {
    offset = cursor.expression();
    if (limit === null && cursor.matchKeyword("LIMIT")) {
      limit = cursor.expression();
    }
  }
  return limit === null && offset === null
    ? undefined
    : { type: ResultModifierType.LIMIT_MODIFIER, limit, offset };
}

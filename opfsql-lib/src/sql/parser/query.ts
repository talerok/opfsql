// ---------------------------------------------------------------------------
// Queries: WITH, SELECT, compound operators, ORDER BY and LIMIT.
// ---------------------------------------------------------------------------

import type { Cursor } from "./cursor.js";
import { parseFrom } from "./from-clause.js";
import {
  ExpressionClass,
  OrderByNullType,
  SetOperationType,
  StatementType,
  type CTENode,
  type LimitClause,
  type OrderByNode,
  type ParsedExpression,
  type SelectNode,
  type SelectStatement,
  type SetOperationNode,
  type WithClause,
} from "./types.js";

/** A whole query: WITH, a body, and the ORDER BY and LIMIT of the body. */
export function parseQuery(cursor: Cursor): SelectStatement {
  cursor.descend();
  const withClause = cursor.isKeyword("WITH") ? parseWith(cursor) : null;
  const body = parseCompound(cursor);
  const orderBy = parseOrderBy(cursor);
  const limit = parseLimit(cursor);
  cursor.ascend();
  return {
    type: StatementType.SELECT_STATEMENT,
    with: withClause,
    body,
    orderBy,
    limit,
  };
}

// --- WITH ----------------------------------------------------------------------

function parseWith(cursor: Cursor): WithClause {
  cursor.next();
  // `WITH recursive AS (…)` names a CTE "recursive".
  const isRecursive =
    cursor.isKeyword("RECURSIVE") &&
    !cursor.isKeyword("AS", 1) &&
    !cursor.isSymbol("(", 1);
  if (isRecursive) cursor.next();
  // The binder nests a plan node per CTE, so each is a level.
  const ctes = cursor.list(() => {
    cursor.descend();
    return parseCte(cursor);
  });
  cursor.ascend(ctes.length);
  cursor.rejectKeyword("INSERT", "WITH before INSERT");
  cursor.rejectKeyword("UPDATE", "WITH before UPDATE");
  cursor.rejectKeyword("DELETE", "WITH before DELETE");
  return { isRecursive, ctes };
}

function parseCte(cursor: Cursor): CTENode {
  const name = cursor.name("a name for the WITH query");
  const columnNames = cursor.isSymbol("(")
    ? cursor.nameList("a column name")
    : [];
  cursor.expectKeyword("AS");
  cursor.rejectIdentifierWord("MATERIALIZED", "[NOT] MATERIALIZED");
  cursor.rejectKeyword("NOT", "[NOT] MATERIALIZED");
  return { name, columnNames, query: cursor.subquery() };
}

// --- Compound queries ------------------------------------------------------------

/**
 * SELECTs joined by compound operators. A compound is a sequence, not
 * nesting, as in SQLite: its links do not count toward MAX_DEPTH, and the
 * binder limits it to 500 SELECTs (docs/semantics.md, Limits).
 */
function parseCompound(cursor: Cursor): SelectNode | SetOperationNode {
  let body: SelectNode | SetOperationNode = parseSelectCore(cursor);
  for (
    let operator = parseSetOperator(cursor);
    operator !== undefined;
    operator = parseSetOperator(cursor)
  ) {
    const right = parseSelectCore(cursor);
    body = {
      type: "SET_OPERATION_NODE",
      setOperationType: operator,
      left: body,
      right,
    };
  }
  return body;
}

/** UNION [ALL], INTERSECT or EXCEPT; the last two without ALL. */
function parseSetOperator(cursor: Cursor): SetOperationType | undefined {
  if (cursor.matchKeyword("UNION")) {
    return cursor.matchKeyword("ALL")
      ? SetOperationType.UNION_ALL
      : SetOperationType.UNION;
  }
  if (cursor.matchKeyword("INTERSECT")) {
    cursor.rejectKeyword("ALL", "INTERSECT ALL");
    return SetOperationType.INTERSECT;
  }
  if (cursor.matchKeyword("EXCEPT")) {
    cursor.rejectKeyword("ALL", "EXCEPT ALL");
    return SetOperationType.EXCEPT;
  }
  return undefined;
}

// --- SELECT ----------------------------------------------------------------------

function parseSelectCore(cursor: Cursor): SelectNode {
  cursor.rejectKeyword("VALUES", "VALUES as a query");
  cursor.expectKeyword("SELECT");
  const isDistinct = cursor.matchKeyword("DISTINCT");
  if (!isDistinct) cursor.matchKeyword("ALL");
  const selectList = cursor.list(() => parseSelectItem(cursor));
  const from = cursor.matchKeyword("FROM") ? parseFrom(cursor) : null;
  const where = cursor.expressionAfter("WHERE");
  const groupBy = parseGroupBy(cursor);
  const having = cursor.expressionAfter("HAVING");
  cursor.rejectKeyword("WINDOW", "WINDOW");
  return {
    type: "SELECT_NODE",
    isDistinct,
    selectList,
    from,
    where,
    groupBy,
    having,
  };
}

/** `*`, `t.*`, or an expression with an optional alias. */
function parseSelectItem(cursor: Cursor): ParsedExpression {
  if (cursor.matchSymbol("*")) return star(null);
  const isTableStar =
    cursor.isName() && cursor.isSymbol(".", 1) && cursor.isSymbol("*", 2);
  if (isTableStar) {
    const table = cursor.name("a table name");
    cursor.expectSymbol(".");
    cursor.expectSymbol("*");
    return star(table);
  }
  const expression = cursor.expression();
  const alias = cursor.optionalAlias();
  return alias === null ? expression : { ...expression, alias };
}

function star(table: string | null): ParsedExpression {
  return { expressionClass: ExpressionClass.STAR, alias: null, table };
}

function parseGroupBy(cursor: Cursor): readonly ParsedExpression[] {
  if (!cursor.matchKeyword("GROUP")) return [];
  cursor.expectKeyword("BY");
  return cursor.list(() => cursor.expression());
}

// --- ORDER BY and LIMIT --------------------------------------------------------------

function parseOrderBy(cursor: Cursor): readonly OrderByNode[] {
  if (!cursor.matchKeyword("ORDER")) return [];
  cursor.expectKeyword("BY");
  return cursor.list(() => parseOrderItem(cursor));
}

function parseOrderItem(cursor: Cursor): OrderByNode {
  const expression = cursor.expression();
  const isDescending = cursor.matchKeyword("DESC");
  if (!isDescending) cursor.matchKeyword("ASC");
  const direction = isDescending ? "descending" : "ascending";
  return { direction, nullOrder: parseNullOrder(cursor), expression };
}

/** `NULLS FIRST`, `NULLS LAST`, or null when the item does not say. */
function parseNullOrder(cursor: Cursor): OrderByNullType | null {
  if (!cursor.matchKeyword("NULLS")) return null;
  if (cursor.matchKeyword("FIRST")) return OrderByNullType.NULLS_FIRST;
  if (cursor.matchKeyword("LAST")) return OrderByNullType.NULLS_LAST;
  return cursor.fail("FIRST or LAST");
}

/**
 * `LIMIT n [OFFSET m]`, SQLite's `LIMIT m, n`, and PostgreSQL's `OFFSET m`
 * with or without a LIMIT after it.
 */
function parseLimit(cursor: Cursor): LimitClause | null {
  if (cursor.matchKeyword("LIMIT")) return parseLimitFirst(cursor);
  if (cursor.matchKeyword("OFFSET")) return parseOffsetFirst(cursor);
  return null;
}

/** After LIMIT: `n`, `n OFFSET m`, or `m, n`, where m is the offset. */
function parseLimitFirst(cursor: Cursor): LimitClause {
  const first = cursor.expression();
  if (cursor.matchSymbol(",")) return limitClause(cursor.expression(), first);
  return limitClause(first, cursor.expressionAfter("OFFSET"));
}

/** After OFFSET: `m`, or `m LIMIT n`. */
function parseOffsetFirst(cursor: Cursor): LimitClause {
  const offset = cursor.expression();
  return limitClause(cursor.expressionAfter("LIMIT"), offset);
}

function limitClause(
  limit: ParsedExpression | null,
  offset: ParsedExpression | null,
): LimitClause {
  return { limit, offset };
}

// ---------------------------------------------------------------------------
// FROM: tables, subqueries and joins. Commas and JOINs chain left to right,
// so `FROM a, b JOIN c ON …` is `(a CROSS JOIN b) JOIN c ON …`.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import type { Keyword } from "./keywords.js";
import {
  JoinType,
  TableReferenceType,
  type JoinConstraint,
  type JoinReference,
  type TableReference,
} from "./types.js";

export function parseFrom(cursor: Cursor): TableReference {
  return cursor.chain(
    parseTableOrSubquery(cursor),
    () => parseJoinOperator(cursor),
    (left, joinType) => parseJoin(cursor, left, joinType),
  );
}

function parseTableOrSubquery(cursor: Cursor): TableReference {
  if (cursor.isSymbol("(")) return parseSubqueryReference(cursor);
  const table = cursor.objectName("a table name");
  cursor.rejectSymbol("(", "a table-valued function");
  const alias = cursor.optionalAlias();
  const isIndexed =
    cursor.isKeyword("INDEXED") ||
    (cursor.isKeyword("NOT") && cursor.isKeyword("INDEXED", 1));
  if (isIndexed) throw new NotSupportedError("INDEXED BY");
  return { type: TableReferenceType.BASE_TABLE, table, alias };
}

/** `( SELECT … ) [AS] alias`; other parentheses here are not supported. */
function parseSubqueryReference(cursor: Cursor): TableReference {
  const isParenthesizedItem = cursor.isName(1) || cursor.isSymbol("(", 1);
  if (isParenthesizedItem) {
    throw new NotSupportedError("parentheses around a FROM item");
  }
  const subquery = cursor.subquery();
  const alias = cursor.optionalAlias();
  return { type: TableReferenceType.SUBQUERY, subquery, alias };
}

const JOIN_KEYWORDS: ReadonlyMap<Keyword, JoinType> = new Map<
  Keyword,
  JoinType
>([
  ["INNER", JoinType.INNER],
  ["CROSS", JoinType.CROSS],
  ["LEFT", JoinType.LEFT],
  ["RIGHT", JoinType.RIGHT],
]);

/** `,` or `[INNER | CROSS | LEFT [OUTER] | RIGHT [OUTER]] JOIN`. */
function parseJoinOperator(cursor: Cursor): JoinType | undefined {
  if (cursor.matchSymbol(",")) return JoinType.CROSS;
  cursor.rejectKeyword("NATURAL", "NATURAL JOIN");
  cursor.rejectKeyword("FULL", "FULL JOIN");
  if (cursor.matchKeyword("JOIN")) return JoinType.INNER;
  const type = cursor.lookupKeyword(JOIN_KEYWORDS);
  if (type === undefined) return undefined;
  cursor.next();
  const isOuter = type === JoinType.LEFT || type === JoinType.RIGHT;
  if (isOuter) cursor.matchKeyword("OUTER");
  cursor.expectKeyword("JOIN");
  return type;
}

/**
 * The right side of a join and its constraint. As in SQLite, a CROSS join
 * or a comma with ON or USING is an inner join.
 */
function parseJoin(
  cursor: Cursor,
  left: TableReference,
  joinType: JoinType,
): JoinReference {
  const right = parseTableOrSubquery(cursor);
  const constraint = parseJoinConstraint(cursor);
  const isInner = joinType === JoinType.CROSS && constraint !== null;
  return {
    type: TableReferenceType.JOIN,
    left,
    right,
    joinType: isInner ? JoinType.INNER : joinType,
    constraint,
  };
}

/** `ON condition`, `USING (columns)`, or null for neither. */
function parseJoinConstraint(cursor: Cursor): JoinConstraint | null {
  if (cursor.matchKeyword("ON")) {
    return { type: "ON", condition: cursor.expression() };
  }
  if (cursor.matchKeyword("USING")) {
    return { type: "USING", columns: cursor.nameList("a column name") };
  }
  return null;
}

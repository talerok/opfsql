// ---------------------------------------------------------------------------
// FROM: tables, subqueries and joins. Commas and JOINs chain left to right,
// so `FROM a, b JOIN c ON …` is `(a CROSS JOIN b) JOIN c ON …`.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import type { Keyword } from "./keywords.js";
import { parseSubquery } from "./primary.js";
import {
  JoinType,
  TableReferenceType,
  type JoinReference,
  type ParsedExpression,
  type TableReference,
} from "./types.js";

export function parseFrom(cursor: Cursor): TableReference {
  let table = parseTableOrSubquery(cursor);
  let levels = 0;
  for (let type = parseJoinOperator(cursor); type !== undefined;) {
    // A chain of joins makes a tree as deep as a nesting would.
    cursor.descend();
    levels++;
    table = parseJoin(cursor, table, type);
    type = parseJoinOperator(cursor);
  }
  cursor.ascend(levels);
  return table;
}

/** A table name, which cannot have a schema: `main.t` is not supported. */
export function parseTableName(cursor: Cursor, what = "a table name"): string {
  const name = cursor.name(what);
  if (cursor.isSymbol(".")) throw new NotSupportedError("a schema name");
  return name;
}

function parseTableOrSubquery(cursor: Cursor): TableReference {
  if (cursor.isSymbol("(")) {
    if (cursor.isName(1)) throw new NotSupportedError("a parenthesized join");
    if (cursor.isKeyword("VALUES", 1)) {
      throw new NotSupportedError("VALUES as a query");
    }
    const subquery = parseSubquery(cursor);
    const alias = parseAlias(cursor);
    return { type: TableReferenceType.SUBQUERY, subquery, alias };
  }
  const tableName = parseTableName(cursor);
  if (cursor.isSymbol("(")) {
    throw new NotSupportedError("a table-valued function");
  }
  const alias = parseAlias(cursor);
  const isIndexed =
    cursor.isKeyword("INDEXED") ||
    (cursor.isKeyword("NOT") && cursor.isKeyword("INDEXED", 1));
  if (isIndexed) {
    throw new NotSupportedError("INDEXED BY");
  }
  return { type: TableReferenceType.BASE_TABLE, tableName, alias };
}

/** `AS name`, a bare identifier, or nothing. */
function parseAlias(cursor: Cursor): string | null {
  return cursor.matchKeyword("AS")
    ? cursor.name("an alias")
    : cursor.matchAlias();
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
  const token = cursor.peek();
  if (token.kind !== "keyword") return undefined;
  if (token.value === "NATURAL" || token.value === "FULL") {
    throw new NotSupportedError(`${token.value} JOIN`);
  }
  if (cursor.matchKeyword("JOIN")) return JoinType.INNER;
  const type = JOIN_KEYWORDS.get(token.value);
  if (type === undefined) return undefined;
  cursor.next();
  if (type === JoinType.LEFT || type === JoinType.RIGHT) {
    cursor.matchKeyword("OUTER");
  }
  cursor.expectKeyword("JOIN");
  return type;
}

/** The right side of a join and its ON or USING; a CROSS join has neither. */
function parseJoin(
  cursor: Cursor,
  left: TableReference,
  joinType: JoinType,
): JoinReference {
  const right = parseTableOrSubquery(cursor);
  let condition: ParsedExpression | null = null;
  let usingColumns: readonly string[] = [];
  if (joinType !== JoinType.CROSS) {
    if (cursor.matchKeyword("ON")) {
      condition = cursor.expression();
    } else if (cursor.matchKeyword("USING")) {
      usingColumns = cursor.nameList("a column name");
    }
  }
  return {
    type: TableReferenceType.JOIN,
    left,
    right,
    condition,
    joinType,
    usingColumns,
  };
}

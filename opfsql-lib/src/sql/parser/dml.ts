// INSERT, UPDATE and DELETE.

import type { Cursor } from "./cursor.js";
import {
  StatementType,
  type Assignment,
  type DeleteStatement,
  type InsertSource,
  type InsertStatement,
  type OnConflictAction,
  type OnConflictClause,
  type ParsedExpression,
  type UpdateStatement,
} from "./types.js";

export function parseInsert(cursor: Cursor): InsertStatement {
  cursor.next();
  cursor.rejectKeyword("OR", "INSERT OR …");
  cursor.expectKeyword("INTO");
  const table = cursor.objectName("a table name");
  cursor.rejectKeyword("AS", "a table alias in INSERT");
  const columns = cursor.isSymbol("(") ? cursor.nameList("a column name") : [];
  const source = parseInsertSource(cursor);
  const onConflict = cursor.isKeyword("ON") ? parseOnConflict(cursor) : null;
  cursor.rejectKeyword("RETURNING", "RETURNING");
  return {
    type: StatementType.INSERT_STATEMENT,
    table,
    columns,
    source,
    onConflict,
  };
}

/** `VALUES (…), …` or a query. */
function parseInsertSource(cursor: Cursor): InsertSource {
  if (cursor.matchKeyword("VALUES")) {
    return { type: "VALUES", rows: cursor.list(() => parseRow(cursor)) };
  }
  cursor.rejectKeyword("DEFAULT", "DEFAULT VALUES");
  if (!cursor.startsQuery()) cursor.fail("VALUES or SELECT");
  return { type: "SELECT", query: cursor.query() };
}

/** `( value, … )` */
function parseRow(cursor: Cursor): readonly ParsedExpression[] {
  cursor.expectSymbol("(");
  const row = cursor.list(() => cursor.expression());
  cursor.expectSymbol(")");
  return row;
}

/** `ON CONFLICT [(columns)] DO NOTHING | DO UPDATE SET … [WHERE …]`. */
function parseOnConflict(cursor: Cursor): OnConflictClause {
  cursor.next();
  cursor.expectKeyword("CONFLICT");
  const target = cursor.isSymbol("(") ? cursor.nameList("a column name") : null;
  cursor.rejectKeyword("WHERE", "ON CONFLICT … WHERE");
  cursor.expectKeyword("DO");
  return { target, action: parseConflictAction(cursor) };
}

function parseConflictAction(cursor: Cursor): OnConflictAction {
  if (cursor.matchKeyword("NOTHING")) return { type: "NOTHING" };
  if (!cursor.matchKeyword("UPDATE")) cursor.fail("NOTHING or UPDATE");
  cursor.expectKeyword("SET");
  const assignments = parseAssignments(cursor);
  return {
    type: "UPDATE",
    assignments,
    where: cursor.expressionAfter("WHERE"),
  };
}

export function parseUpdate(cursor: Cursor): UpdateStatement {
  cursor.next();
  cursor.rejectKeyword("OR", "UPDATE OR …");
  const table = cursor.objectName("a table name");
  cursor.rejectKeyword("AS", "a table alias in UPDATE");
  cursor.expectKeyword("SET");
  const assignments = parseAssignments(cursor);
  cursor.rejectKeyword("FROM", "UPDATE … FROM");
  const where = cursor.expressionAfter("WHERE");
  cursor.rejectKeyword("RETURNING", "RETURNING");
  return { type: StatementType.UPDATE_STATEMENT, table, assignments, where };
}

export function parseDelete(cursor: Cursor): DeleteStatement {
  cursor.next();
  cursor.expectKeyword("FROM");
  const table = cursor.objectName("a table name");
  cursor.rejectKeyword("AS", "a table alias in DELETE");
  const where = cursor.expressionAfter("WHERE");
  cursor.rejectKeyword("RETURNING", "RETURNING");
  return { type: StatementType.DELETE_STATEMENT, table, where };
}

/** `column = value, …` */
function parseAssignments(cursor: Cursor): readonly Assignment[] {
  return cursor.list(() => {
    cursor.rejectSymbol("(", "assigning to a list of columns");
    const column = cursor.name("a column name");
    cursor.expectSymbol("=");
    return { column, value: cursor.expression() };
  });
}

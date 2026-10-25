// INSERT, UPDATE and DELETE.

import { NotSupportedError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import { parseTableName } from "./from-clause.js";
import { startsQuery } from "./primary.js";
import {
  StatementType,
  type DeleteStatement,
  type InsertStatement,
  type OnConflictClause,
  type ParsedExpression,
  type SelectStatement,
  type UpdateSetClause,
  type UpdateStatement,
} from "./types.js";

export function parseInsert(cursor: Cursor): InsertStatement {
  cursor.next();
  if (cursor.isKeyword("OR")) throw new NotSupportedError("INSERT OR …");
  cursor.expectKeyword("INTO");
  const table = parseTableName(cursor);
  const columns = cursor.isSymbol("(") ? cursor.nameList("a column name") : [];
  const { values, selectStatement } = parseInsertSource(cursor);
  const onConflict = cursor.isKeyword("ON") ? parseOnConflict(cursor) : null;
  rejectReturning(cursor);
  return {
    type: StatementType.INSERT_STATEMENT,
    table,
    columns,
    values,
    selectStatement,
    onConflict,
  };
}

function parseInsertSource(cursor: Cursor): {
  values: readonly (readonly ParsedExpression[])[];
  selectStatement: SelectStatement | null;
} {
  if (startsQuery(cursor))
    return { values: [], selectStatement: cursor.query() };
  if (cursor.isKeyword("DEFAULT")) {
    throw new NotSupportedError("DEFAULT VALUES");
  }
  if (!cursor.matchKeyword("VALUES")) cursor.fail("VALUES or SELECT");
  const values = cursor.list(() => {
    cursor.expectSymbol("(");
    const row = cursor.list(() => cursor.expression());
    cursor.expectSymbol(")");
    return row;
  });
  return { values, selectStatement: null };
}

/** `ON CONFLICT [(columns)] DO NOTHING | DO UPDATE SET … [WHERE …]`. */
function parseOnConflict(cursor: Cursor): OnConflictClause {
  cursor.next();
  cursor.expectKeyword("CONFLICT");
  const conflictTarget = cursor.isSymbol("(")
    ? cursor.nameList("a column name")
    : null;
  cursor.expectKeyword("DO");
  if (cursor.matchKeyword("NOTHING"))
    return { conflictTarget, action: "NOTHING" };
  if (!cursor.matchKeyword("UPDATE")) cursor.fail("NOTHING or UPDATE");
  cursor.expectKeyword("SET");
  const assignments = parseAssignments(cursor);
  const whereClause = cursor.matchKeyword("WHERE") ? cursor.expression() : null;
  return {
    conflictTarget,
    action: { type: "UPDATE", setClauses: assignments, whereClause },
  };
}

export function parseUpdate(cursor: Cursor): UpdateStatement {
  cursor.next();
  if (cursor.isKeyword("OR")) throw new NotSupportedError("UPDATE OR …");
  const table = parseTableName(cursor);
  cursor.expectKeyword("SET");
  const assignments = parseAssignments(cursor);
  if (cursor.isKeyword("FROM")) throw new NotSupportedError("UPDATE … FROM");
  const whereClause = cursor.matchKeyword("WHERE") ? cursor.expression() : null;
  rejectReturning(cursor);
  return {
    type: StatementType.UPDATE_STATEMENT,
    table,
    setClauses: assignments,
    whereClause,
  };
}

export function parseDelete(cursor: Cursor): DeleteStatement {
  cursor.next();
  cursor.expectKeyword("FROM");
  const table = parseTableName(cursor);
  const whereClause = cursor.matchKeyword("WHERE") ? cursor.expression() : null;
  rejectReturning(cursor);
  return { type: StatementType.DELETE_STATEMENT, table, whereClause };
}

/** `column = value, …` */
function parseAssignments(cursor: Cursor): readonly UpdateSetClause[] {
  return cursor.list(() => {
    if (cursor.isSymbol("(")) {
      throw new NotSupportedError("assigning to a list of columns");
    }
    const column = cursor.name("a column name");
    cursor.expectSymbol("=");
    return { column, value: cursor.expression() };
  });
}

function rejectReturning(cursor: Cursor): void {
  if (cursor.isKeyword("RETURNING")) throw new NotSupportedError("RETURNING");
}

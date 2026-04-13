import { BaseParser } from './base.js';
import { parseExpression } from './expressions.js';
import {
  TokenType, ParsedExpression,
  StatementType, SelectStatement,
  InsertStatement, UpdateStatement, DeleteStatement,
  UpdateSetClause, OnConflictClause,
} from '../types.js';

export function parseInsert(p: BaseParser): InsertStatement {
  p.expect(TokenType.INSERT);
  p.expect(TokenType.INTO, `Expected INTO after INSERT`);
  const table = p.expect(TokenType.IDENTIFIER, `Expected table name after INSERT INTO`).value;

  const columns: string[] = [];
  if (p.match(TokenType.LEFT_PAREN)) {
    do {
      columns.push(p.expect(TokenType.IDENTIFIER, `Expected column name`).value);
    } while (p.match(TokenType.COMMA));
    p.expect(TokenType.RIGHT_PAREN);
  }

  let values: ParsedExpression[][] = [];
  let select_statement: SelectStatement | null = null;

  // INSERT INTO ... SELECT ...
  if (p.check(TokenType.SELECT) || p.check(TokenType.WITH)) {
    select_statement = p.parseSelectStatement() as SelectStatement;
  } else {
    // INSERT INTO ... VALUES (...)
    p.expect(TokenType.VALUES, `Expected VALUES or SELECT after INSERT INTO ${table}`);
    do {
      p.expect(TokenType.LEFT_PAREN, `Expected '(' before values`);
      const row: ParsedExpression[] = [];
      do {
        row.push(parseExpression(p));
      } while (p.match(TokenType.COMMA));
      p.expect(TokenType.RIGHT_PAREN, `Expected ')' after values`);
      values.push(row);
    } while (p.match(TokenType.COMMA));
  }

  const onConflict = parseOnConflict(p);

  return {
    type: StatementType.INSERT_STATEMENT,
    table,
    columns,
    values,
    select_statement,
    onConflict,
  };
}

function parseOnConflict(p: BaseParser): OnConflictClause | null {
  if (!p.match(TokenType.ON)) return null;
  p.expect(TokenType.CONFLICT, `Expected CONFLICT after ON`);

  // Optional conflict target: (col1, col2, ...)
  let conflictTarget: string[] | null = null;
  if (p.match(TokenType.LEFT_PAREN)) {
    conflictTarget = [];
    do {
      conflictTarget.push(p.expect(TokenType.IDENTIFIER, `Expected column name in conflict target`).value);
    } while (p.match(TokenType.COMMA));
    p.expect(TokenType.RIGHT_PAREN);
  }

  p.expect(TokenType.DO, `Expected DO after ON CONFLICT`);

  // DO NOTHING
  if (p.match(TokenType.NOTHING)) {
    return { conflictTarget, action: 'NOTHING' };
  }

  // DO UPDATE SET ...
  p.expect(TokenType.UPDATE, `Expected NOTHING or UPDATE after DO`);
  p.expect(TokenType.SET, `Expected SET after DO UPDATE`);

  const setClauses: UpdateSetClause[] = [];
  do {
    const column = p.expect(TokenType.IDENTIFIER, `Expected column name in SET clause`).value;
    p.expect(TokenType.EQUALS, `Expected '=' after column name in SET clause`);
    const value = parseExpression(p);
    setClauses.push({ column, value });
  } while (p.match(TokenType.COMMA));

  let whereClause: ParsedExpression | null = null;
  if (p.match(TokenType.WHERE)) {
    whereClause = parseExpression(p);
  }

  return {
    conflictTarget,
    action: { type: 'UPDATE', setClauses, whereClause },
  };
}

export function parseUpdate(p: BaseParser): UpdateStatement {
  p.expect(TokenType.UPDATE);
  const table = p.expect(TokenType.IDENTIFIER, `Expected table name after UPDATE`).value;
  p.expect(TokenType.SET, `Expected SET after UPDATE ${table}`);

  const set_clauses: UpdateSetClause[] = [];
  do {
    const column = p.expect(TokenType.IDENTIFIER, `Expected column name in SET clause`).value;
    p.expect(TokenType.EQUALS, `Expected '=' after column name in SET clause`);
    const value = parseExpression(p);
    set_clauses.push({ column, value });
  } while (p.match(TokenType.COMMA));

  let where_clause: ParsedExpression | null = null;
  if (p.match(TokenType.WHERE)) {
    where_clause = parseExpression(p);
  }

  return {
    type: StatementType.UPDATE_STATEMENT,
    table,
    set_clauses,
    where_clause,
  };
}

export function parseDelete(p: BaseParser): DeleteStatement {
  p.expect(TokenType.DELETE);
  p.expect(TokenType.FROM, `Expected FROM after DELETE`);
  const table = p.expect(TokenType.IDENTIFIER, `Expected table name after DELETE FROM`).value;

  let where_clause: ParsedExpression | null = null;
  if (p.match(TokenType.WHERE)) {
    where_clause = parseExpression(p);
  }

  return {
    type: StatementType.DELETE_STATEMENT,
    table,
    where_clause,
  };
}

import { BaseParser } from './base.js';
import { parseExpression } from './expressions.js';
import {
  TokenType, ParsedExpression,
  StatementType, SelectStatement,
  InsertStatement, UpdateStatement, DeleteStatement,
  UpdateSetClause,
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

  // INSERT INTO ... SELECT ...
  if (p.check(TokenType.SELECT) || p.check(TokenType.WITH)) {
    const select_statement = p.parseSelectStatement() as SelectStatement;
    return {
      type: StatementType.INSERT_STATEMENT,
      table,
      columns,
      values: [],
      select_statement,
    };
  }

  // INSERT INTO ... VALUES (...)
  p.expect(TokenType.VALUES, `Expected VALUES or SELECT after INSERT INTO ${table}`);
  const values: ParsedExpression[][] = [];

  do {
    p.expect(TokenType.LEFT_PAREN, `Expected '(' before values`);
    const row: ParsedExpression[] = [];
    do {
      row.push(parseExpression(p));
    } while (p.match(TokenType.COMMA));
    p.expect(TokenType.RIGHT_PAREN, `Expected ')' after values`);
    values.push(row);
  } while (p.match(TokenType.COMMA));

  return {
    type: StatementType.INSERT_STATEMENT,
    table,
    columns,
    values,
    select_statement: null,
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

import { Lexer } from '../lexer.js';
import {
  TokenType, Statement, SelectStatement, SetOperationStatement,
  StatementType, TransactionType,
  type ExplainStatement,
} from '../types.js';
import { BaseParser } from './base.js';
import { parseSelectStatement } from './select.js';
import { parseInsert, parseUpdate, parseDelete } from './dml.js';
import { parseCreate, parseAlterTable, parseDrop } from './ddl.js';
import { parseTransaction } from './tcl.js';

export class Parser extends BaseParser {
  parse(sql: string): Statement[] {
    const lexer = new Lexer(sql);
    this.tokens = lexer.tokenize();
    this.current = 0;

    const statements: Statement[] = [];

    while (!this.isAtEnd()) {
      while (this.match(TokenType.SEMICOLON)) { /* skip empty semicolons */ }
      if (this.isAtEnd()) break;
      statements.push(this.parseStatement());
      this.match(TokenType.SEMICOLON);
    }

    return statements;
  }

  override parseSelectStatement(): SelectStatement | SetOperationStatement {
    return parseSelectStatement(this);
  }

  private parseStatement(): Statement {
    const token = this.peek();

    switch (token.type) {
      case TokenType.SELECT:
      case TokenType.WITH:
        return parseSelectStatement(this);
      case TokenType.INSERT:
        return parseInsert(this);
      case TokenType.UPDATE:
        return parseUpdate(this);
      case TokenType.DELETE:
        return parseDelete(this);
      case TokenType.CREATE:
        return parseCreate(this);
      case TokenType.ALTER:
        return parseAlterTable(this);
      case TokenType.DROP:
        return parseDrop(this);
      case TokenType.BEGIN:
        return parseTransaction(this, TransactionType.BEGIN);
      case TokenType.COMMIT:
        return parseTransaction(this, TransactionType.COMMIT);
      case TokenType.ROLLBACK:
        return parseTransaction(this, TransactionType.ROLLBACK);
      case TokenType.EXPLAIN: {
        this.advance(); // consume EXPLAIN
        const inner = this.parseStatement();
        return { type: StatementType.EXPLAIN_STATEMENT, statement: inner } as ExplainStatement;
      }
      default:
        this.error(`Unexpected token '${token.value || 'EOF'}' at start of statement`);
    }
  }
}

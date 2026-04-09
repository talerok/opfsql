import {
  Token, TokenType, ParseError,
  SelectStatement, SetOperationStatement,
} from '../types.js';

export class BaseParser {
  tokens: Token[] = [];
  current: number = 0;

  peek(): Token {
    return this.tokens[this.current];
  }

  peekAt(offset: number): Token {
    const idx = this.current + offset;
    if (idx >= this.tokens.length) return this.tokens[this.tokens.length - 1];
    return this.tokens[idx];
  }

  advance(): Token {
    const token = this.tokens[this.current];
    if (!this.isAtEnd()) this.current++;
    return token;
  }

  check(type: TokenType): boolean {
    return this.peek().type === type;
  }

  match(...types: TokenType[]): boolean {
    for (const type of types) {
      if (this.check(type)) {
        this.advance();
        return true;
      }
    }
    return false;
  }

  expect(type: TokenType, message?: string): Token {
    if (this.check(type)) return this.advance();
    const token = this.peek();
    const msg = message ?? `Expected ${TokenType[type]}, got '${token.value || 'EOF'}'`;
    throw new ParseError(msg, token.line, token.column, token);
  }

  isAtEnd(): boolean {
    return this.peek().type === TokenType.EOF;
  }

  error(message: string): never {
    const token = this.peek();
    throw new ParseError(message, token.line, token.column, token);
  }

  checkIdentifier(): boolean {
    return this.check(TokenType.IDENTIFIER) || this.check(TokenType.QUOTED_IDENTIFIER);
  }

  expectIdentifier(message?: string): Token {
    if (this.check(TokenType.IDENTIFIER) || this.check(TokenType.QUOTED_IDENTIFIER)) {
      return this.advance();
    }
    const token = this.peek();
    const msg = message ?? `Expected identifier, got '${token.value || 'EOF'}'`;
    throw new ParseError(msg, token.line, token.column, token);
  }

  isClauseKeyword(): boolean {
    const t = this.peek().type;
    return t === TokenType.FROM || t === TokenType.WHERE || t === TokenType.GROUP ||
           t === TokenType.HAVING || t === TokenType.ORDER || t === TokenType.LIMIT ||
           t === TokenType.OFFSET || t === TokenType.UNION || t === TokenType.SEMICOLON ||
           t === TokenType.EOF || t === TokenType.RIGHT_PAREN;
  }

  // Overridden by Parser — needed for subquery parsing in expressions
  parseSelectStatement(): SelectStatement | SetOperationStatement {
    this.error('Unexpected subquery');
  }
}

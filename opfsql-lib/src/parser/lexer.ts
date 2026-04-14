import { Token, TokenType, ParseError } from './types.js';

const KEYWORDS: Record<string, TokenType> = {
  select: TokenType.SELECT,
  from: TokenType.FROM,
  where: TokenType.WHERE,
  join: TokenType.JOIN,
  left: TokenType.LEFT,
  right: TokenType.RIGHT,
  inner: TokenType.INNER,
  outer: TokenType.OUTER,
  cross: TokenType.CROSS,
  on: TokenType.ON,
  using: TokenType.USING,
  as: TokenType.AS,
  distinct: TokenType.DISTINCT,
  all: TokenType.ALL,
  group: TokenType.GROUP,
  by: TokenType.BY,
  having: TokenType.HAVING,
  order: TokenType.ORDER,
  asc: TokenType.ASC,
  desc: TokenType.DESC,
  nulls: TokenType.NULLS,
  first: TokenType.FIRST,
  last: TokenType.LAST,
  limit: TokenType.LIMIT,
  offset: TokenType.OFFSET,
  union: TokenType.UNION,
  insert: TokenType.INSERT,
  into: TokenType.INTO,
  values: TokenType.VALUES,
  update: TokenType.UPDATE,
  set: TokenType.SET,
  delete: TokenType.DELETE,
  create: TokenType.CREATE,
  table: TokenType.TABLE,
  index: TokenType.INDEX,
  unique: TokenType.UNIQUE,
  if: TokenType.IF,
  not: TokenType.NOT,
  exists: TokenType.EXISTS,
  alter: TokenType.ALTER,
  add: TokenType.ADD,
  drop: TokenType.DROP,
  column: TokenType.COLUMN,
  begin: TokenType.BEGIN,
  commit: TokenType.COMMIT,
  rollback: TokenType.ROLLBACK,
  transaction: TokenType.TRANSACTION,
  with: TokenType.WITH,
  and: TokenType.AND,
  or: TokenType.OR,
  in: TokenType.IN,
  between: TokenType.BETWEEN,
  like: TokenType.LIKE,
  is: TokenType.IS,
  null: TokenType.NULL_KW,
  primary: TokenType.PRIMARY,
  key: TokenType.KEY,
  foreign: TokenType.FOREIGN,
  references: TokenType.REFERENCES,
  default: TokenType.DEFAULT,
  case: TokenType.CASE,
  when: TokenType.WHEN,
  then: TokenType.THEN,
  else: TokenType.ELSE,
  end: TokenType.END,
  cast: TokenType.CAST,
  true: TokenType.TRUE_KW,
  false: TokenType.FALSE_KW,
  conflict: TokenType.CONFLICT,
  do: TokenType.DO,
  nothing: TokenType.NOTHING,
  excluded: TokenType.EXCLUDED,
  autoincrement: TokenType.AUTOINCREMENT,
  // Type keywords
  integer: TokenType.INTEGER_KW,
  int: TokenType.INT_KW,
  bigint: TokenType.BIGINT_KW,
  smallint: TokenType.SMALLINT_KW,
  real: TokenType.REAL_KW,
  float: TokenType.FLOAT_KW,
  double: TokenType.DOUBLE_KW,
  text: TokenType.TEXT_KW,
  varchar: TokenType.VARCHAR_KW,
  char: TokenType.CHAR_KW,
  blob: TokenType.BLOB_KW,
  boolean: TokenType.BOOLEAN_KW,
  bool: TokenType.BOOL_KW,
  json: TokenType.JSON_KW,
};

// Unsupported keywords that produce clear errors
const UNSUPPORTED_KEYWORDS = new Set([
  'window', 'over', 'partition',
  'full', 'truncate', 'grant', 'revoke',
  'procedure', 'trigger', 'pivot', 'unpivot',
]);

export class Lexer {
  private source: string;
  private pos: number = 0;
  private line: number = 1;
  private column: number = 1;
  private tokens: Token[] = [];

  constructor(source: string) {
    this.source = source;
  }

  tokenize(): Token[] {
    this.tokens = [];
    this.pos = 0;
    this.line = 1;
    this.column = 1;

    while (this.pos < this.source.length) {
      this.skipWhitespace();
      if (this.pos >= this.source.length) break;

      const ch = this.source[this.pos];

      // Single-line comment
      if (ch === '-' && this.peek(1) === '-') {
        this.skipLineComment();
        continue;
      }

      // Block comment
      if (ch === '/' && this.peek(1) === '*') {
        this.skipBlockComment();
        continue;
      }

      // String literal
      if (ch === "'") {
        this.readString();
        continue;
      }

      // Quoted identifier
      if (ch === '"') {
        this.readQuotedIdentifier();
        continue;
      }

      // Number
      if (this.isDigit(ch)) {
        this.readNumber();
        continue;
      }

      // Identifier or keyword
      if (this.isAlpha(ch) || ch === '_') {
        this.readIdentifierOrKeyword();
        continue;
      }

      // Operators and punctuation
      const startLine = this.line;
      const startCol = this.column;

      switch (ch) {
        case '(':
          this.addToken(TokenType.LEFT_PAREN, '(', startLine, startCol);
          this.advance();
          break;
        case ')':
          this.addToken(TokenType.RIGHT_PAREN, ')', startLine, startCol);
          this.advance();
          break;
        case ',':
          this.addToken(TokenType.COMMA, ',', startLine, startCol);
          this.advance();
          break;
        case ';':
          this.addToken(TokenType.SEMICOLON, ';', startLine, startCol);
          this.advance();
          break;
        case '.':
          this.addToken(TokenType.DOT, '.', startLine, startCol);
          this.advance();
          break;
        case '[':
          this.addToken(TokenType.LEFT_BRACKET, '[', startLine, startCol);
          this.advance();
          break;
        case ']':
          this.addToken(TokenType.RIGHT_BRACKET, ']', startLine, startCol);
          this.advance();
          break;
        case '+':
          this.addToken(TokenType.PLUS, '+', startLine, startCol);
          this.advance();
          break;
        case '-':
          this.addToken(TokenType.MINUS, '-', startLine, startCol);
          this.advance();
          break;
        case '*':
          this.addToken(TokenType.STAR, '*', startLine, startCol);
          this.advance();
          break;
        case '/':
          this.addToken(TokenType.SLASH, '/', startLine, startCol);
          this.advance();
          break;
        case '%':
          this.addToken(TokenType.PERCENT, '%', startLine, startCol);
          this.advance();
          break;
        case '=':
          this.addToken(TokenType.EQUALS, '=', startLine, startCol);
          this.advance();
          break;
        case '<':
          this.advance();
          if (this.pos < this.source.length && this.source[this.pos] === '=') {
            this.addToken(TokenType.LESS_THAN_EQUAL, '<=', startLine, startCol);
            this.advance();
          } else if (this.pos < this.source.length && this.source[this.pos] === '>') {
            this.addToken(TokenType.NOT_EQUALS, '<>', startLine, startCol);
            this.advance();
          } else {
            this.addToken(TokenType.LESS_THAN, '<', startLine, startCol);
          }
          break;
        case '>':
          this.advance();
          if (this.pos < this.source.length && this.source[this.pos] === '=') {
            this.addToken(TokenType.GREATER_THAN_EQUAL, '>=', startLine, startCol);
            this.advance();
          } else {
            this.addToken(TokenType.GREATER_THAN, '>', startLine, startCol);
          }
          break;
        case '|':
          this.advance();
          if (this.pos < this.source.length && this.source[this.pos] === '|') {
            this.addToken(TokenType.PIPE_PIPE, '||', startLine, startCol);
            this.advance();
          } else {
            this.error(`Unexpected character '|'`, startLine, startCol);
          }
          break;
        case '!':
          this.advance();
          if (this.pos < this.source.length && this.source[this.pos] === '=') {
            this.addToken(TokenType.NOT_EQUALS, '!=', startLine, startCol);
            this.advance();
          } else {
            this.error(`Unexpected character '!'`, startLine, startCol);
          }
          break;
        case '$': {
          this.advance();
          let digits = '';
          while (this.pos < this.source.length && this.isDigit(this.source[this.pos])) {
            digits += this.source[this.pos];
            this.advance();
          }
          if (digits.length === 0) {
            this.error("Expected parameter index after '$'", startLine, startCol);
          }
          this.addToken(TokenType.PARAMETER, digits, startLine, startCol);
          break;
        }
        default:
          this.error(`Unexpected character '${ch}'`, startLine, startCol);
      }
    }

    this.addToken(TokenType.EOF, '', this.line, this.column);
    return this.tokens;
  }

  private advance(): void {
    if (this.source[this.pos] === '\n') {
      this.line++;
      this.column = 1;
    } else {
      this.column++;
    }
    this.pos++;
  }

  private peek(offset: number): string | undefined {
    return this.source[this.pos + offset];
  }

  private skipWhitespace(): void {
    while (this.pos < this.source.length) {
      const ch = this.source[this.pos];
      if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r') {
        this.advance();
      } else {
        break;
      }
    }
  }

  private skipLineComment(): void {
    // Skip --
    this.advance();
    this.advance();
    while (this.pos < this.source.length && this.source[this.pos] !== '\n') {
      this.advance();
    }
  }

  private skipBlockComment(): void {
    const startLine = this.line;
    const startCol = this.column;
    // Skip /*
    this.advance();
    this.advance();
    while (this.pos < this.source.length) {
      if (this.source[this.pos] === '*' && this.peek(1) === '/') {
        this.advance();
        this.advance();
        return;
      }
      this.advance();
    }
    this.error('Unterminated block comment', startLine, startCol);
  }

  private readString(): void {
    const startLine = this.line;
    const startCol = this.column;
    this.advance(); // skip opening '
    let value = '';

    while (this.pos < this.source.length) {
      const ch = this.source[this.pos];
      if (ch === "'") {
        // Check for escaped ''
        if (this.peek(1) === "'") {
          value += "'";
          this.advance();
          this.advance();
        } else {
          this.advance(); // skip closing '
          this.addToken(TokenType.STRING_LITERAL, value, startLine, startCol);
          return;
        }
      } else {
        value += ch;
        this.advance();
      }
    }

    this.error('Unterminated string literal', startLine, startCol);
  }

  private readQuotedIdentifier(): void {
    const startLine = this.line;
    const startCol = this.column;
    this.advance(); // skip opening "
    let value = '';

    while (this.pos < this.source.length) {
      const ch = this.source[this.pos];
      if (ch === '"') {
        this.advance(); // skip closing "
        this.addToken(TokenType.QUOTED_IDENTIFIER, value, startLine, startCol);
        return;
      }
      value += ch;
      this.advance();
    }

    this.error('Unterminated quoted identifier', startLine, startCol);
  }

  private readNumber(): void {
    const startLine = this.line;
    const startCol = this.column;
    let value = '';
    let isFloat = false;

    while (this.pos < this.source.length && this.isDigit(this.source[this.pos])) {
      value += this.source[this.pos];
      this.advance();
    }

    if (this.pos < this.source.length && this.source[this.pos] === '.' && this.peek(1) !== undefined && this.isDigit(this.peek(1)!)) {
      isFloat = true;
      value += '.';
      this.advance();
      while (this.pos < this.source.length && this.isDigit(this.source[this.pos])) {
        value += this.source[this.pos];
        this.advance();
      }
    }

    this.addToken(isFloat ? TokenType.FLOAT_LITERAL : TokenType.INTEGER_LITERAL, value, startLine, startCol);
  }

  private readIdentifierOrKeyword(): void {
    const startLine = this.line;
    const startCol = this.column;
    let value = '';

    while (this.pos < this.source.length && this.isAlphaNumeric(this.source[this.pos])) {
      value += this.source[this.pos];
      this.advance();
    }

    const lower = value.toLowerCase();

    // Blob literal: x'DEADBEEF' or X'DEADBEEF'
    if (lower === 'x' && this.pos < this.source.length && this.source[this.pos] === "'") {
      this.advance(); // skip opening '
      let hex = '';
      while (this.pos < this.source.length && this.source[this.pos] !== "'") {
        hex += this.source[this.pos];
        this.advance();
      }
      if (this.pos >= this.source.length) {
        this.error('Unterminated blob literal', startLine, startCol);
      }
      this.advance(); // skip closing '
      if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) {
        this.error('Invalid blob literal: hex string must have even length and contain only hex digits', startLine, startCol);
      }
      this.addToken(TokenType.BLOB_LITERAL, hex, startLine, startCol);
      return;
    }

    // Check unsupported keywords
    if (UNSUPPORTED_KEYWORDS.has(lower)) {
      this.error(
        `Keyword '${value.toUpperCase()}' is not supported`,
        startLine,
        startCol
      );
    }

    const keywordType = KEYWORDS[lower];
    if (keywordType !== undefined) {
      this.addToken(keywordType, value, startLine, startCol);
    } else {
      this.addToken(TokenType.IDENTIFIER, value, startLine, startCol);
    }
  }

  private isDigit(ch: string): boolean {
    return ch >= '0' && ch <= '9';
  }

  private isAlpha(ch: string): boolean {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch === '_';
  }

  private isAlphaNumeric(ch: string): boolean {
    return this.isAlpha(ch) || this.isDigit(ch);
  }

  private addToken(type: TokenType, value: string, line: number, column: number): void {
    this.tokens.push({ type, value, line, column });
  }

  private error(message: string, line: number, column: number): never {
    const token: Token = { type: TokenType.EOF, value: '', line, column };
    throw new ParseError(message, line, column, token);
  }
}

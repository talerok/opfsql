// ---------------------------------------------------------------------------
// The parser's position in the token list, and everything the grammar files
// share: matching keywords and symbols, reading names, errors, the nesting
// limit and parameter numbers. The grammar's two recursive entry points,
// expressions and queries, are reached through the cursor so that the files
// implementing them need not import each other.
// ---------------------------------------------------------------------------

import { InternalError, SqlSyntaxError } from "../../errors.js";
import { isReserved, type Keyword } from "./keywords.js";
import { power } from "./precedence.js";
import { describe, type SymbolText, type Token } from "./tokens.js";
import type { ParsedExpression, SelectStatement } from "./types.js";

/**
 * How deeply expressions and queries may nest, counting each parenthesis,
 * subquery and prefix operator, and each operator in a chain like
 * `1 + 2 + 3`. SQLite allows 1000; a worker's stack is smaller than the main
 * thread's, and the binder and executor walk the tree recursively too, so
 * opfsql stops at 200 with a syntax error instead of a stack overflow.
 */
export const MAX_DEPTH = 200;

/** SQLite's largest parameter number (SQLITE_MAX_VARIABLE_NUMBER). */
const MAX_PARAMETER = 32_766;

export interface Grammar {
  expression(cursor: Cursor, minimumPower: number): ParsedExpression;
  query(cursor: Cursor): SelectStatement;
}

export class Cursor {
  private position = 0;
  private depth = 0;
  private highestParameter = 0;

  constructor(
    private readonly sql: string,
    private readonly tokens: readonly Token[],
    private readonly grammar: Grammar,
  ) {}

  // --- The grammar's entry points --------------------------------------------

  expression(minimumPower: number = power.lowest): ParsedExpression {
    return this.grammar.expression(this, minimumPower);
  }

  query(): SelectStatement {
    return this.grammar.query(this);
  }

  // --- Tokens ----------------------------------------------------------------

  peek(offset = 0): Token {
    const last = this.tokens.length - 1;
    const token = this.tokens[Math.min(this.position + offset, last)];
    if (token === undefined) throw new InternalError("no tokens to parse");
    return token;
  }

  next(): Token {
    const token = this.peek();
    if (token.kind !== "end") this.position++;
    return token;
  }

  atEnd(): boolean {
    return this.peek().kind === "end";
  }

  isKeyword(keyword: Keyword, offset = 0): boolean {
    const token = this.peek(offset);
    return token.kind === "keyword" && token.value === keyword;
  }

  matchKeyword(keyword: Keyword): boolean {
    if (!this.isKeyword(keyword)) return false;
    this.position++;
    return true;
  }

  expectKeyword(keyword: Keyword): void {
    if (!this.matchKeyword(keyword)) this.fail(keyword);
  }

  isSymbol(symbol: SymbolText, offset = 0): boolean {
    const token = this.peek(offset);
    return token.kind === "symbol" && token.value === symbol;
  }

  matchSymbol(symbol: SymbolText): boolean {
    if (!this.isSymbol(symbol)) return false;
    this.position++;
    return true;
  }

  expectSymbol(symbol: SymbolText): Token {
    if (!this.isSymbol(symbol)) this.fail(`'${symbol}'`);
    return this.next();
  }

  /** One or more items separated by commas. */
  list<T>(parse: () => T): T[] {
    const items = [parse()];
    while (this.matchSymbol(",")) items.push(parse());
    return items;
  }

  /** The SQL text between two offsets, as written. */
  text(start: number, end: number): string {
    return this.sql.slice(start, end);
  }

  // --- Names -----------------------------------------------------------------

  /** True if the token can be a name: a word that is not reserved. */
  isName(offset = 0): boolean {
    const token = this.peek(offset);
    switch (token.kind) {
      case "identifier":
      case "quoted": {
        return true;
      }
      case "keyword": {
        return !isReserved(token.value);
      }
      case "integer":
      case "real":
      case "string":
      case "blob":
      case "parameter":
      case "symbol":
      case "end": {
        return false;
      }
    }
  }

  /** A name: an identifier, a quoted name or a non-reserved keyword. */
  name(what: string): string {
    if (!this.isName()) this.fail(what);
    return nameOf(this.next());
  }

  /** A name after a dot, where even a reserved keyword is a name. */
  label(what: string): string {
    const kind = this.peek().kind;
    if (kind !== "keyword" && !this.isName()) this.fail(what);
    return nameOf(this.next());
  }

  /** `( name, … )` */
  nameList(what: string): string[] {
    this.expectSymbol("(");
    const names = this.list(() => this.name(what));
    this.expectSymbol(")");
    return names;
  }

  /** True if the token is an identifier spelling `word`, in any case. */
  isWord(word: string, offset = 0): boolean {
    const token = this.peek(offset);
    return token.kind === "identifier" && token.value.toUpperCase() === word;
  }

  /** A bare alias: an identifier or a quoted name, never a keyword. */
  matchAlias(): string | null {
    const kind = this.peek().kind;
    return kind === "identifier" || kind === "quoted"
      ? nameOf(this.next())
      : null;
  }

  // --- Errors ----------------------------------------------------------------

  /** Fails at the current token, which is not what the grammar expects. */
  fail(expected: string): never {
    const token = this.peek();
    this.failAt(token, `expected ${expected}, got ${describe(token)}`);
  }

  failAt(token: Token, message: string): never {
    throw new SqlSyntaxError(message, token.start, token.end - token.start);
  }

  // --- Nesting ---------------------------------------------------------------

  /** One level deeper; fails past MAX_DEPTH. */
  descend(): void {
    this.depth++;
    if (this.depth > MAX_DEPTH) {
      this.failAt(
        this.peek(),
        `the statement nests more than ${String(MAX_DEPTH)} levels deep`,
      );
    }
  }

  ascend(levels = 1): void {
    this.depth -= levels;
  }

  // --- Parameters ------------------------------------------------------------

  /** Parameters are numbered per statement. */
  startStatement(): void {
    this.highestParameter = 0;
  }

  /** The 0-based index of a parameter token: `$1` is 0, `?` the next one. */
  parameterIndex(token: Token): number {
    const number =
      token.value === "" ? this.highestParameter + 1 : Number(token.value);
    if (number < 1 || number > MAX_PARAMETER) {
      this.failAt(
        token,
        `parameter numbers run from 1 to ${String(MAX_PARAMETER)}, got ${token.text}`,
      );
    }
    this.highestParameter = Math.max(this.highestParameter, number);
    return number - 1;
  }
}

function nameOf(token: Token): string {
  return token.kind === "keyword" ? token.text : token.value;
}

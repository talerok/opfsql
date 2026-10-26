// ---------------------------------------------------------------------------
// The parser's position in the token list, and everything the grammar files
// share: matching keywords and symbols, names, optional clauses, subqueries,
// errors, unsupported syntax, the nesting limit and parameter numbers. The
// grammar's two recursive entry points, expressions and queries, are reached
// through the cursor so that the files implementing them need not import
// each other.
// ---------------------------------------------------------------------------

import {
  InternalError,
  NotSupportedError,
  SqlSyntaxError,
} from "../../errors.js";
import { isReserved, upperAscii, type Keyword } from "./keywords.js";
import { power } from "../precedence.js";
import { describe, type SymbolText, type Token } from "./tokens.js";
import type { ParsedExpression, SelectStatement } from "./types.js";

/**
 * How deeply expressions and queries may nest, counting each parenthesis,
 * subquery and prefix operator, and each link of a chain (`1 + 2 + 3`, a
 * join, a CTE list). A compound query is a sequence, not nesting, and does
 * not count (query.ts). SQLite allows 1000; a worker's stack
 * is smaller than the main thread's, and the binder and executor walk the
 * tree recursively too, so opfsql stops at 200 with a syntax error instead
 * of a stack overflow. The whole pipeline runs at this depth in 180 KB of
 * stack.
 */
export const MAX_DEPTH = 200;

/**
 * The keywords a query starts with. VALUES is among them so that a query
 * starting with it reaches parseSelectCore, which reports it as not
 * supported.
 */
const QUERY_KEYWORDS: ReadonlySet<Keyword> = new Set<Keyword>([
  "SELECT",
  "WITH",
  "VALUES",
]);

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

  /** True if a query starts here: SELECT, WITH or VALUES. */
  startsQuery(offset = 0): boolean {
    const keyword = this.keywordAt(offset);
    return keyword !== undefined && QUERY_KEYWORDS.has(keyword);
  }

  /** `( SELECT … )` */
  subquery(): SelectStatement {
    this.expectSymbol("(");
    if (!this.startsQuery()) this.fail("a subquery");
    const query = this.query();
    this.expectSymbol(")");
    return query;
  }

  /** The expression after `keyword`, or null when the keyword is absent. */
  expressionAfter(keyword: Keyword): ParsedExpression | null {
    return this.matchKeyword(keyword) ? this.expression() : null;
  }

  /**
   * A left-deep chain: `first`, then for each operator `operator` finds,
   * `combine` parses the right side and joins it on. Each link nests the
   * tree one level deeper, so each counts toward MAX_DEPTH.
   */
  chain<Node, Operator>(
    first: Node,
    operator: () => Operator | undefined,
    combine: (left: Node, operator: Operator) => Node,
  ): Node {
    let node = first;
    let links = 0;
    for (let next = operator(); next !== undefined; next = operator()) {
      this.descend();
      links++;
      node = combine(node, next);
    }
    this.ascend(links);
    return node;
  }

  /** One or more items separated by commas. */
  list<T>(parse: () => T): T[] {
    const items = [parse()];
    while (this.matchSymbol(",")) items.push(parse());
    return items;
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

  /** The keyword at the token, or undefined if it is not one. */
  keywordAt(offset = 0): Keyword | undefined {
    const token = this.peek(offset);
    return token.kind === "keyword" ? token.value : undefined;
  }

  /** What `table` holds for the keyword at the token, if anything. */
  lookupKeyword<T>(table: ReadonlyMap<Keyword, T>, offset = 0): T | undefined {
    const keyword = this.keywordAt(offset);
    return keyword === undefined ? undefined : table.get(keyword);
  }

  isKeyword(keyword: Keyword, offset = 0): boolean {
    return this.keywordAt(offset) === keyword;
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

  /** An identifier's word in upper case, if it is ASCII; for non-keywords. */
  wordAt(offset = 0): string | undefined {
    const token = this.peek(offset);
    return token.kind === "identifier" ? upperAscii(token.value) : undefined;
  }

  /** True if the token is an identifier spelling `word` (in upper case). */
  isWord(word: string, offset = 0): boolean {
    return this.wordAt(offset) === word;
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
    const isLabel = this.peek().kind === "keyword" || this.isName();
    if (!isLabel) this.fail(what);
    return nameOf(this.next());
  }

  /** `( name, … )` */
  nameList(what: string): string[] {
    this.expectSymbol("(");
    const names = this.list(() => this.name(what));
    this.expectSymbol(")");
    return names;
  }

  /** The name of a table or index, which cannot have a schema: `main.t`. */
  objectName(what: string): string {
    const name = this.name(what);
    this.rejectSymbol(".", "a schema name");
    return name;
  }

  /**
   * `AS name`, or a bare alias, which is an identifier or a quoted name and
   * never a keyword; null when there is neither.
   */
  optionalAlias(): string | null {
    if (this.matchKeyword("AS")) return this.name("an alias");
    const kind = this.peek().kind;
    const isBareAlias = kind === "identifier" || kind === "quoted";
    return isBareAlias ? nameOf(this.next()) : null;
  }

  // --- Errors ----------------------------------------------------------------

  /** Fails at the current token, which is not what the grammar expects. */
  fail(expected: string): never {
    const token = this.peek();
    this.failAt(token, `expected ${expected}, got ${describe(token)}`);
  }

  /** Fails at a token, or from `start` (a sign before it, say) to its end. */
  failAt(token: Token, message: string, start = token.start): never {
    throw new SqlSyntaxError(message, start, token.end - start);
  }

  /** Valid SQL that opfsql does not implement starts with `keyword` here. */
  rejectKeyword(keyword: Keyword, feature: string, offset = 0): void {
    if (this.isKeyword(keyword, offset)) throw new NotSupportedError(feature);
  }

  rejectSymbol(symbol: SymbolText, feature: string, offset = 0): void {
    if (this.isSymbol(symbol, offset)) throw new NotSupportedError(feature);
  }

  /** As rejectKeyword, for any name. */
  rejectName(feature: string): void {
    if (this.isName()) throw new NotSupportedError(feature);
  }

  /** As rejectKeyword, for a word that is not a keyword (`VIEW`, `PRAGMA`). */
  rejectWord(word: string, feature: string): void {
    if (this.isWord(word)) throw new NotSupportedError(feature);
  }

  /** As rejectWord, for any of `words`; `feature` names the one found. */
  rejectWordIn(
    words: ReadonlySet<string>,
    feature: (word: string) => string,
  ): void {
    const word = this.wordAt();
    const isRejected = word !== undefined && words.has(word);
    if (isRejected) throw new NotSupportedError(feature(word));
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
    const isInRange = number >= 1 && number <= MAX_PARAMETER;
    if (!isInRange) {
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

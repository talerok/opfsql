// ---------------------------------------------------------------------------
// The lexer: SQL text to tokens, as SQLite's tokenizer reads it. Every token
// keeps its offsets and its text as written. A number that runs into a
// letter (`1e3x`, `0x1g`, `1_000`) is an error, never two tokens.
// ---------------------------------------------------------------------------

import { NotSupportedError, SqlSyntaxError } from "../../errors.js";
import { keywordOf } from "./keywords.js";
import type { SymbolText, Token, TokenValue } from "./tokens.js";

/** A sticky regular expression, matched at a given offset. */
class Pattern {
  constructor(private readonly expression: RegExp) {}

  /** The match starting exactly at `offset`, with its groups. */
  execAt(text: string, offset: number): RegExpExecArray | null {
    this.expression.lastIndex = offset;
    return this.expression.exec(text);
  }

  at(text: string, offset: number): string | undefined {
    return this.execAt(text, offset)?.[0];
  }
}

const SPACE = new Pattern(/[\t\n\f\r ]+/y);
const LINE_COMMENT = new Pattern(/--[^\n]*/y);
const BLOCK_COMMENT = new Pattern(/\/\*[\s\S]*?\*\//y);
const HEX_NUMBER = new Pattern(/0x[\da-f]+/iy);
const DECIMAL_NUMBER = new Pattern(/(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?/iy);
const WORD = new Pattern(/[a-z_\u{80}-\u{10FFFF}][\w$\u{80}-\u{10FFFF}]*/iuy);
const WORD_CHARACTERS = new Pattern(/[\w$\u{80}-\u{10FFFF}]*/uy);
const STRING = new Pattern(/'(?:[^']|'')*'/y);
const QUOTED = new Pattern(/"(?:[^"]|"")*"/y);
const BLOB = new Pattern(/x'([^']*)'/iy);
const PARAMETER = new Pattern(/\$\d+|\?\d*/y);
const NAMED_PARAMETER = new Pattern(/[$:@][a-z_]/iy);
const HEX_DIGITS = /^(?:[\da-f]{2})*$/i;

// Longest first, so that `<=` is not read as `<` and `=`.
const SYMBOLS: readonly SymbolText[] = [
  "->>",
  "==",
  "!=",
  "<>",
  "<=",
  ">=",
  "||",
  "<<",
  ">>",
  "->",
  "(",
  ")",
  ",",
  ";",
  ".",
  "[",
  "]",
  "+",
  "-",
  "*",
  "/",
  "%",
  "=",
  "<",
  ">",
  "|",
  "&",
  "~",
];

/** The tokens of `sql`, ending with one of kind "end". */
export function tokenize(sql: string): readonly Token[] {
  const lexer = new Lexer(sql);
  const tokens: Token[] = [];
  for (;;) {
    const token = lexer.read();
    tokens.push(token);
    if (token.kind === "end") return tokens;
  }
}

class Lexer {
  private position = 0;

  constructor(private readonly sql: string) {}

  private skipSpace(): void {
    for (;;) {
      const skipped =
        this.match(SPACE) ?? this.match(LINE_COMMENT) ?? this.blockComment();
      if (skipped === undefined) return;
      this.position += skipped.length;
    }
  }

  private blockComment(): string | undefined {
    if (!this.sql.startsWith("/*", this.position)) return undefined;
    const comment = this.match(BLOCK_COMMENT);
    if (comment === undefined) {
      this.fail("unterminated comment", this.sql.length);
    }
    return comment;
  }

  private number(): Token {
    const text = this.match(HEX_NUMBER) ?? this.match(DECIMAL_NUMBER) ?? "";
    const end = this.position + text.length;
    const trailing = WORD_CHARACTERS.at(this.sql, end) ?? "";
    if (trailing !== "") {
      this.fail(`invalid number '${text}${trailing}'`, end + trailing.length);
    }
    const isReal = !/^0x/i.test(text) && /[.e]/i.test(text);
    return this.token({ kind: isReal ? "real" : "integer", value: text }, end);
  }

  private string(): Token {
    const text = this.match(STRING);
    if (text === undefined) {
      this.fail("unterminated string", this.sql.length);
    }
    const value = text.slice(1, -1).replaceAll("''", "'");
    return this.token({ kind: "string", value }, this.position + text.length);
  }

  private quoted(): Token {
    const text = this.match(QUOTED);
    if (text === undefined) {
      this.fail("unterminated quoted name", this.sql.length);
    }
    const end = this.position + text.length;
    if (text === '""') this.fail("a quoted name cannot be empty", end);
    const value = text.slice(1, -1).replaceAll('""', '"');
    return this.token({ kind: "quoted", value }, end);
  }

  private blob(): Token | undefined {
    const found = BLOB.execAt(this.sql, this.position);
    if (found === null) return undefined;
    const digits = found[1] ?? "";
    const end = this.position + found[0].length;
    if (!HEX_DIGITS.test(digits)) {
      this.fail("a blob literal needs an even number of hex digits", end);
    }
    return this.token({ kind: "blob", value: digits }, end);
  }

  private word(): Token | undefined {
    const text = this.match(WORD);
    if (text === undefined) return undefined;
    const end = this.position + text.length;
    const keyword = keywordOf(text);
    return keyword === undefined
      ? this.token({ kind: "identifier", value: text }, end)
      : this.token({ kind: "keyword", value: keyword }, end);
  }

  private parameter(): Token {
    const text = this.match(PARAMETER);
    if (text !== undefined) {
      const end = this.position + text.length;
      return this.token({ kind: "parameter", value: text.slice(1) }, end);
    }
    if (this.match(NAMED_PARAMETER) !== undefined) {
      throw new NotSupportedError("a named parameter");
    }
    return this.symbol();
  }

  private symbol(): Token {
    const symbol = SYMBOLS.find((candidate) =>
      this.sql.startsWith(candidate, this.position),
    );
    if (symbol === undefined) {
      const character = String.fromCodePoint(
        this.sql.codePointAt(this.position) ?? 0,
      );
      this.fail(
        `unexpected character '${character}'`,
        this.position + character.length,
      );
    }
    const end = this.position + symbol.length;
    return this.token({ kind: "symbol", value: symbol }, end);
  }

  private ahead(length: number): string {
    return this.sql.slice(this.position, this.position + length);
  }

  private match(pattern: Pattern): string | undefined {
    return pattern.at(this.sql, this.position);
  }

  private token(value: TokenValue, end: number): Token {
    const start = this.position;
    this.position = end;
    return { ...value, text: this.sql.slice(start, end), start, end };
  }

  private fail(message: string, end: number): never {
    throw new SqlSyntaxError(message, this.position, end - this.position);
  }

  /** The next token, after any spaces and comments. */
  read(): Token {
    this.skipSpace();
    if (this.position >= this.sql.length) {
      return this.token({ kind: "end", value: "" }, this.position);
    }
    const character = this.sql.charAt(this.position);
    if (/\d/.test(character) || /^\.\d/.test(this.ahead(2))) {
      return this.number();
    }
    switch (character) {
      case "'": {
        return this.string();
      }
      case '"': {
        return this.quoted();
      }
      case "$":
      case "?":
      case ":":
      case "@": {
        return this.parameter();
      }
      default: {
        return this.blob() ?? this.word() ?? this.symbol();
      }
    }
  }
}

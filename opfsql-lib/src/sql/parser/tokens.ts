// Tokens: what the lexer reads and the parser consumes.

import type { Keyword } from "./keywords.js";

export type TokenKind =
  /** `42`, `0x2A` */
  | "integer"
  /** `1.5`, `.5`, `1e3` */
  | "real"
  /** `'text'`; the value is the text, `''` unescaped */
  | "string"
  /** `x'CAFE'`; the value is the hex digits */
  | "blob"
  /** `$1`, `?1` or `?`; the value is the number, empty for a bare `?` */
  | "parameter"
  /** `name`; the value is the name as written */
  | "identifier"
  /** `"name"`; the value is the name, `""` unescaped */
  | "quoted"
  /** the value is the keyword in upper case */
  | "keyword"
  /** an operator or punctuation; the value is the symbol */
  | "symbol"
  /** the end of the text; it has no characters */
  | "end";

export type SymbolText =
  | "("
  | ")"
  | ","
  | ";"
  | "."
  | "["
  | "]"
  | "+"
  | "-"
  | "*"
  | "/"
  | "%"
  | "="
  | "=="
  | "!="
  | "<>"
  | "<"
  | "<="
  | ">"
  | ">="
  | "||"
  | "|"
  | "&"
  | "~"
  | "<<"
  | ">>"
  | "->"
  | "->>";

/** What a token is, without where it is. */
export type TokenValue =
  | { readonly kind: "keyword"; readonly value: Keyword }
  | { readonly kind: "symbol"; readonly value: SymbolText }
  | {
      readonly kind: Exclude<TokenKind, "keyword" | "symbol">;
      readonly value: string;
    };

export type Token = TokenValue & {
  /** The token as written, so a literal keeps its exact digits. */
  readonly text: string;
  /** Offset of the first character in the SQL text (UTF-16 code units). */
  readonly start: number;
  /** Offset just past the last character. */
  readonly end: number;
};

const LONGEST_SHOWN = 32;

/** How an error message names a token: `'FROM'`, `')'`, `end of input`. */
export function describe(token: Token): string {
  if (token.kind === "end") return "end of input";
  const text =
    token.text.length > LONGEST_SHOWN
      ? `${token.text.slice(0, LONGEST_SHOWN)}…`
      : token.text;
  return `'${text}'`;
}

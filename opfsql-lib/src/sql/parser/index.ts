// The SQL parser: text to syntax trees (types.ts). It depends on nothing but
// `values` and `errors`.

import { parse } from "./statement.js";
import type { Statement } from "./types.js";

export { childExpressions } from "./children.js";
export { MAX_DEPTH } from "./cursor.js";
export { formatExpression, formatName, formatQuery } from "./format.js";
export { tokenize } from "./lexer.js";
export { parse, parseExpression } from "./statement.js";
export type { Token, TokenKind } from "./tokens.js";
export * from "./types.js";

/** For callers that hold a parser; parsing keeps no state between calls. */
export class Parser {
  parse(sql: string): readonly Statement[] {
    return parse(sql);
  }
}

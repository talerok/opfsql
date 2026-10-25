// Declared types: `INTEGER`, `varchar(255)`, `DECIMAL(10, 2)`. The parser
// keeps the text as written; what it means is the binder's business
// (docs/semantics.md, Column types).

import type { Cursor } from "./cursor.js";
import type { Token } from "./tokens.js";

/** True if a type name starts here: types are plain identifiers. */
export function startsTypeName(cursor: Cursor): boolean {
  return cursor.peek().kind === "identifier";
}

/** A type name: one or more words, then up to two numbers in parentheses. */
export function parseTypeName(cursor: Cursor): string {
  if (!startsTypeName(cursor)) cursor.fail("a type name");
  const first = cursor.next();
  let last: Token = first;
  while (startsTypeName(cursor)) last = cursor.next();
  if (cursor.matchSymbol("(")) {
    parseSignedNumber(cursor);
    if (cursor.matchSymbol(",")) parseSignedNumber(cursor);
    last = cursor.expectSymbol(")");
  }
  return cursor.text(first.start, last.end);
}

function parseSignedNumber(cursor: Cursor): void {
  if (!cursor.matchSymbol("-")) cursor.matchSymbol("+");
  const kind = cursor.peek().kind;
  if (kind !== "integer" && kind !== "real") cursor.fail("a number");
  cursor.next();
}

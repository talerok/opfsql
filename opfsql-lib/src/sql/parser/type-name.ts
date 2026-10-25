// Declared types: `INTEGER`, `varchar(255)`, `DECIMAL(10, 2)`. The parser
// keeps the text as written; what it means is the binder's business
// (docs/semantics.md, Column types).

import { NotSupportedError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import type { Token } from "./tokens.js";

/** True if a type name starts here: types are identifiers. */
export function startsTypeName(cursor: Cursor): boolean {
  const kind = cursor.peek().kind;
  return kind === "identifier" || kind === "quoted";
}

/** A type name: one or more words, then up to two numbers in parentheses. */
export function parseTypeName(cursor: Cursor): string {
  if (!startsTypeName(cursor)) cursor.fail("a type name");
  const first = cursor.peek();
  let last: Token = first;
  while (startsTypeName(cursor)) last = parseTypeWord(cursor);
  if (cursor.matchSymbol("(")) {
    parseSignedNumber(cursor);
    if (cursor.matchSymbol(",")) parseSignedNumber(cursor);
    last = cursor.expectSymbol(")");
  }
  return cursor.text(first.start, last.end);
}

function parseTypeWord(cursor: Cursor): Token {
  if (cursor.peek().kind === "quoted") {
    throw new NotSupportedError("a quoted type name");
  }
  return cursor.next();
}

function parseSignedNumber(cursor: Cursor): void {
  if (!cursor.matchSymbol("-")) cursor.matchSymbol("+");
  const kind = cursor.peek().kind;
  if (kind !== "integer" && kind !== "real") cursor.fail("a number");
  cursor.next();
}

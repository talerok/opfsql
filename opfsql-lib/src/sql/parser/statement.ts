// ---------------------------------------------------------------------------
// The entry point: SQL text to statements. Statements are separated by
// semicolons; anything after a statement other than `;` or the end of the
// text is an error, so leftovers never parse as another statement.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import { Cursor, type Grammar } from "./cursor.js";
import { parseAlterTable, parseCreate, parseDrop } from "./ddl.js";
import { parseDelete, parseInsert, parseUpdate } from "./dml.js";
import { parseExpression } from "./expression.js";
import type { Keyword } from "./keywords.js";
import { tokenize } from "./lexer.js";
import { parseQuery } from "./query.js";
import {
  StatementType,
  TransactionType,
  type Statement,
  type TransactionStatement,
} from "./types.js";

const GRAMMAR: Grammar = { expression: parseExpression, query: parseQuery };

/** The statements in `sql`; empty statements (`;;`) are skipped. */
export function parse(sql: string): readonly Statement[] {
  const cursor = new Cursor(sql, tokenize(sql), GRAMMAR);
  const statements: Statement[] = [];
  for (;;) {
    while (cursor.matchSymbol(";")) {
      // An empty statement.
    }
    if (cursor.atEnd()) return statements;
    cursor.startStatement();
    statements.push(parseStatement(cursor));
    if (!cursor.atEnd() && !cursor.isSymbol(";")) {
      cursor.fail("';' or end of input");
    }
  }
}

const STATEMENTS: ReadonlyMap<Keyword, (cursor: Cursor) => Statement> = new Map<
  Keyword,
  (cursor: Cursor) => Statement
>([
  ["SELECT", parseQuery],
  ["WITH", parseQuery],
  ["INSERT", parseInsert],
  ["UPDATE", parseUpdate],
  ["DELETE", parseDelete],
  ["CREATE", parseCreate],
  ["ALTER", parseAlterTable],
  ["DROP", parseDrop],
  ["BEGIN", parseBegin],
  ["COMMIT", (cursor) => parseEnd(cursor, TransactionType.COMMIT)],
  ["END", (cursor) => parseEnd(cursor, TransactionType.COMMIT)],
  ["ROLLBACK", parseRollback],
  ["EXPLAIN", parseExplain],
]);

/** Statements SQLite has and opfsql does not. */
const OTHER_STATEMENTS: ReadonlySet<string> = new Set([
  "ANALYZE",
  "ATTACH",
  "DETACH",
  "PRAGMA",
  "REINDEX",
  "RELEASE",
  "REPLACE",
  "SAVEPOINT",
  "VACUUM",
  "VALUES",
]);

function parseStatement(cursor: Cursor): Statement {
  const token = cursor.peek();
  const parser =
    token.kind === "keyword" ? STATEMENTS.get(token.value) : undefined;
  if (parser !== undefined) return parser(cursor);
  const word = token.value.toUpperCase();
  if (
    (token.kind === "identifier" || token.kind === "keyword") &&
    OTHER_STATEMENTS.has(word)
  ) {
    throw new NotSupportedError(`the ${word} statement`);
  }
  return cursor.fail("a statement");
}

// --- Transactions and EXPLAIN ---------------------------------------------------------

function transaction(transactionType: TransactionType): TransactionStatement {
  return { type: StatementType.TRANSACTION_STATEMENT, transactionType };
}

/** `BEGIN [TRANSACTION]`; DEFERRED, IMMEDIATE and EXCLUSIVE are not supported. */
function parseBegin(cursor: Cursor): TransactionStatement {
  cursor.next();
  for (const mode of ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]) {
    if (cursor.isWord(mode)) throw new NotSupportedError(`BEGIN ${mode}`);
  }
  cursor.matchKeyword("TRANSACTION");
  return transaction(TransactionType.BEGIN);
}

/** `COMMIT [TRANSACTION]` or `END [TRANSACTION]`. */
function parseEnd(cursor: Cursor, type: TransactionType): TransactionStatement {
  cursor.next();
  cursor.matchKeyword("TRANSACTION");
  return transaction(type);
}

/** `ROLLBACK [TRANSACTION]`; ROLLBACK TO a savepoint is not supported. */
function parseRollback(cursor: Cursor): TransactionStatement {
  const statement = parseEnd(cursor, TransactionType.ROLLBACK);
  if (cursor.isWord("TO")) throw new NotSupportedError("ROLLBACK TO");
  return statement;
}

function parseExplain(cursor: Cursor): Statement {
  cursor.next();
  if (cursor.isWord("QUERY")) {
    throw new NotSupportedError("EXPLAIN QUERY PLAN");
  }
  if (cursor.isKeyword("EXPLAIN")) cursor.fail("a statement to explain");
  return {
    type: StatementType.EXPLAIN_STATEMENT,
    statement: parseStatement(cursor),
  };
}

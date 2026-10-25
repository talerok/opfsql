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
  type BeginMode,
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
  const word = token.kind === "keyword" ? token.value : cursor.wordAt();
  if (word !== undefined && OTHER_STATEMENTS.has(word)) {
    throw new NotSupportedError(`the ${word} statement`);
  }
  return cursor.fail("a statement");
}

// --- Transactions and EXPLAIN ---------------------------------------------------------

type EndType = TransactionType.COMMIT | TransactionType.ROLLBACK;

/** `BEGIN [DEFERRED | IMMEDIATE] [TRANSACTION]`. */
function parseBegin(cursor: Cursor): TransactionStatement {
  cursor.next();
  const mode = parseBeginMode(cursor);
  parseTransactionKeyword(cursor);
  return {
    type: StatementType.TRANSACTION_STATEMENT,
    transactionType: TransactionType.BEGIN,
    mode,
  };
}

/** DEFERRED, the default, or IMMEDIATE; EXCLUSIVE is not supported. */
function parseBeginMode(cursor: Cursor): BeginMode {
  if (cursor.isWord("EXCLUSIVE"))
    throw new NotSupportedError("BEGIN EXCLUSIVE");
  if (cursor.isWord("IMMEDIATE")) {
    cursor.next();
    return "IMMEDIATE";
  }
  if (cursor.isWord("DEFERRED")) cursor.next();
  return "DEFERRED";
}

/** `COMMIT [TRANSACTION]` or `END [TRANSACTION]`. */
function parseEnd(cursor: Cursor, type: EndType): TransactionStatement {
  cursor.next();
  parseTransactionKeyword(cursor);
  return { type: StatementType.TRANSACTION_STATEMENT, transactionType: type };
}

/** `[TRANSACTION]`; SQLite allows a name after it, which it ignores. */
function parseTransactionKeyword(cursor: Cursor): void {
  if (!cursor.matchKeyword("TRANSACTION")) return;
  if (cursor.isName() && !cursor.isWord("TO")) {
    throw new NotSupportedError("a transaction name");
  }
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

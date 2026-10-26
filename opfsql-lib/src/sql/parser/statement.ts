import { Cursor, type Grammar } from "./cursor.js";
import { parseAlterTable, parseCreate, parseDrop } from "./ddl.js";
import { parseDelete, parseInsert, parseUpdate } from "./dml.js";
import { parseExpressionFrom as parseExpressionAt } from "./expression.js";
import type { Keyword } from "./keywords.js";
import { tokenize } from "./lexer.js";
import { parseQuery } from "./query.js";
import {
  StatementType,
  TransactionType,
  type BeginMode,
  type BeginStatement,
  type ParsedExpression,
  type Statement,
  type TransactionStatement,
} from "./types.js";

// ---------------------------------------------------------------------------
// The entry point: SQL text to statements. Statements are separated by
// semicolons; anything after a statement other than `;` or the end of the
// text is an error, so leftovers never parse as another statement.
// ---------------------------------------------------------------------------

const GRAMMAR: Grammar = { expression: parseExpressionAt, query: parseQuery };

/** The statements in `sql`; empty statements (`;;`) are skipped. */
export function parse(sql: string): readonly Statement[] {
  const cursor = new Cursor(sql, tokenize(sql), GRAMMAR);
  const statements: Statement[] = [];
  skipEmptyStatements(cursor);
  while (!cursor.atEnd()) {
    statements.push(parseTerminatedStatement(cursor));
    skipEmptyStatements(cursor);
  }
  return statements;
}

/**
 * One expression and nothing after it: an index key as CREATE INDEX stores
 * it (formatExpression prints it back).
 */
export function parseExpression(sql: string): ParsedExpression {
  const cursor = new Cursor(sql, tokenize(sql), GRAMMAR);
  const expression = cursor.expression();
  if (!cursor.atEnd()) cursor.fail("end of input");
  return expression;
}

function skipEmptyStatements(cursor: Cursor): void {
  while (cursor.isSymbol(";")) cursor.next();
}

/** A statement, which `;` or the end of the input must follow. */
function parseTerminatedStatement(cursor: Cursor): Statement {
  cursor.startStatement();
  const statement = parseStatement(cursor);
  const isTerminated = cursor.atEnd() || cursor.isSymbol(";");
  if (!isTerminated) cursor.fail("';' or end of input");
  return statement;
}

type StatementParser = (cursor: Cursor) => Statement;

const STATEMENTS: ReadonlyMap<Keyword, StatementParser> = new Map<
  Keyword,
  StatementParser
>([
  ["SELECT", parseQuery],
  ["WITH", parseQuery],
  ["VALUES", parseQuery],
  ["INSERT", parseInsert],
  ["UPDATE", parseUpdate],
  ["DELETE", parseDelete],
  ["CREATE", parseCreate],
  ["ALTER", parseAlterTable],
  ["DROP", parseDrop],
  ["BEGIN", parseBegin],
  ["COMMIT", parseCommit],
  ["END", parseCommit],
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
]);

function parseStatement(cursor: Cursor): Statement {
  const parser = cursor.lookupKeyword(STATEMENTS);
  if (parser !== undefined) return parser(cursor);
  cursor.rejectIdentifierWordIn(
    OTHER_STATEMENTS,
    (word) => `the ${word} statement`,
  );
  return cursor.fail("a statement");
}

// --- Transactions and EXPLAIN ---------------------------------------------------------

/** `BEGIN [DEFERRED | IMMEDIATE] [TRANSACTION]`. */
function parseBegin(cursor: Cursor): BeginStatement {
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
  cursor.rejectIdentifierWord("EXCLUSIVE", "BEGIN EXCLUSIVE");
  if (cursor.isIdentifierWord("IMMEDIATE")) {
    cursor.next();
    return "IMMEDIATE";
  }
  if (cursor.isIdentifierWord("DEFERRED")) cursor.next();
  return "DEFERRED";
}

/** `COMMIT [TRANSACTION]` or `END [TRANSACTION]`. */
function parseCommit(cursor: Cursor): TransactionStatement {
  cursor.next();
  parseTransactionKeyword(cursor);
  return transactionEnd(TransactionType.COMMIT);
}

/** `ROLLBACK [TRANSACTION]`; ROLLBACK TO a savepoint is not supported. */
function parseRollback(cursor: Cursor): TransactionStatement {
  cursor.next();
  const hasKeyword = cursor.matchKeyword("TRANSACTION");
  cursor.rejectIdentifierWord("TO", "ROLLBACK TO");
  if (hasKeyword) rejectTransactionName(cursor);
  return transactionEnd(TransactionType.ROLLBACK);
}

type EndType = TransactionType.COMMIT | TransactionType.ROLLBACK;

function transactionEnd(transactionType: EndType): TransactionStatement {
  return { type: StatementType.TRANSACTION_STATEMENT, transactionType };
}

/** `[TRANSACTION]` */
function parseTransactionKeyword(cursor: Cursor): void {
  if (cursor.matchKeyword("TRANSACTION")) rejectTransactionName(cursor);
}

/** SQLite allows a name after TRANSACTION, which it ignores. */
function rejectTransactionName(cursor: Cursor): void {
  cursor.rejectName("a transaction name");
}

function parseExplain(cursor: Cursor): Statement {
  cursor.next();
  cursor.rejectIdentifierWord("QUERY", "EXPLAIN QUERY PLAN");
  if (cursor.isKeyword("EXPLAIN")) cursor.fail("a statement to explain");
  return {
    type: StatementType.EXPLAIN_STATEMENT,
    statement: parseStatement(cursor),
  };
}

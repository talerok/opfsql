import type { Session } from "../../src/worker-entry.js";
import type { Record } from "./records.js";
import { mismatchOf, resultValues } from "./results.js";

// ---------------------------------------------------------------------------
// Running a file's records against one session, in order, each to an
// outcome: what it did against what the file expects. A failing record
// does not stop the file: the next ones run, as sqllogictest runs them.
// ---------------------------------------------------------------------------

/** What one record did: null when it did what the file expects. */
export interface Outcome {
  readonly line: number;
  readonly sql: string;
  readonly failure: string | null;
}

/** Each statement and query of `records` run, with its outcome. */
export async function runRecords(
  session: Session,
  records: readonly Record[],
): Promise<Outcome[]> {
  const outcomes: Outcome[] = [];
  for (const record of records) {
    const failure = await failureOf(session, record);
    if (failure !== undefined) {
      outcomes.push({ line: record.line, sql: sqlOf(record), failure });
    }
  }
  return outcomes;
}

/** How a record failed, null if it did not; undefined for a directive. */
async function failureOf(
  session: Session,
  record: Record,
): Promise<string | null | undefined> {
  switch (record.kind) {
    case "statement": {
      return statementFailure(session, record.sql, record.isErrorExpected);
    }
    case "query": {
      return queryFailure(session, record);
    }
    case "hash-threshold":
    case "halt": {
      // Hashing is the file's to choose; parsing stopped at a halt.
      return undefined;
    }
  }
}

function sqlOf(record: Record): string {
  return "sql" in record ? record.sql : "";
}

async function statementFailure(
  session: Session,
  sql: string,
  isErrorExpected: boolean,
): Promise<string | null> {
  const error = await errorOf(session.execute(sql));
  if (isErrorExpected) return error === null ? "expected an error" : null;
  return error;
}

type Query = Extract<Record, { readonly kind: "query" }>;

async function queryFailure(
  session: Session,
  query: Query,
): Promise<string | null> {
  try {
    const result = await session.query(query.sql);
    const values = resultValues(result.rows, query.types, query.sort);
    return mismatchOf(values, query.expected);
  } catch (error) {
    return describe(error);
  }
}

/** The error a call fails with, described; null if it succeeds. */
async function errorOf(call: Promise<unknown>): Promise<string | null> {
  try {
    await call;
    return null;
  } catch (error) {
    return describe(error);
  }
}

function describe(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const code = "code" in error ? String(error.code) : error.name;
  return `${code}: ${error.message}`;
}

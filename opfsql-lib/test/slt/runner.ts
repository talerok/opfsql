import type { Session } from "../../src/worker-entry.js";
import {
  bytesToHex,
  canonicalJson,
  type Value,
} from "../../src/values/index.js";

// ---------------------------------------------------------------------------
// A runner for sqllogictest files, the subset these tests use: `statement
// ok|error`, `query <types> [nosort|rowsort|valuesort]` with its expected
// lines after `----`, `skipif opfsql` before a block, `halt`, and `#`
// comments. Blank lines end a block.
// ---------------------------------------------------------------------------

export type SortMode = "nosort" | "rowsort" | "valuesort";

export type SltBlock =
  | {
      readonly kind: "statement";
      readonly sql: string;
      readonly isErrorExpected: boolean;
      readonly isSkipped: boolean;
    }
  | {
      readonly kind: "query";
      readonly sql: string;
      readonly sort: SortMode;
      readonly expected: readonly string[];
      readonly isSkipped: boolean;
    };

export interface SltFailure {
  readonly blockIndex: number;
  readonly sql: string;
  readonly message: string;
}

export interface SltResult {
  readonly total: number;
  readonly passed: number;
  readonly skipped: number;
  readonly failures: readonly SltFailure[];
}

// --- Parsing -----------------------------------------------------------------

/** Lines read one at a time, trimmed; null past the end. */
class Lines {
  private position = 0;

  constructor(private readonly lines: readonly string[]) {}

  peek(): string | null {
    return this.lines[this.position]?.trim() ?? null;
  }

  next(): string | null {
    const line = this.peek();
    this.position++;
    return line;
  }

  skipBlank(): void {
    while (this.peek() === "") this.position++;
  }

  /** Lines up to a blank one, the end, or one `isEnd` accepts. */
  takeUntil(isEnd: (line: string) => boolean = () => false): string[] {
    const taken: string[] = [];
    for (let line = this.peek(); line !== null; line = this.peek()) {
      if (line === "" || isEnd(line)) break;
      taken.push(line);
      this.position++;
    }
    return taken;
  }
}

export function parseSlt(source: string): SltBlock[] {
  const lines = new Lines(source.split("\n"));
  const blocks: SltBlock[] = [];
  for (let line = lines.next(); line !== null; line = lines.next()) {
    if (line === "halt") break;
    const block = blockAt(line, lines);
    if (block !== null) blocks.push(block);
  }
  return blocks;
}

/** The block that `line` starts, if it starts one; skips anything else. */
function blockAt(line: string, lines: Lines): SltBlock | null {
  if (line.startsWith("skipif")) return skippedBlock(line, lines);
  if (line.startsWith("statement")) return statementBlock(line, lines, false);
  if (line.startsWith("query")) return queryBlock(line, lines, false);
  return null;
}

/** `skipif <engine>`: the next block, skipped if the engine is opfsql. */
function skippedBlock(line: string, lines: Lines): SltBlock | null {
  const isSkipped = line.split(/\s+/u, 2)[1] === "opfsql";
  lines.skipBlank();
  const header = lines.next();
  if (header === null) return null;
  if (header.startsWith("statement")) {
    return statementBlock(header, lines, isSkipped);
  }
  if (header.startsWith("query")) return queryBlock(header, lines, isSkipped);
  return null;
}

function statementBlock(
  header: string,
  lines: Lines,
  isSkipped: boolean,
): SltBlock {
  const sql = lines.takeUntil().join("\n");
  const isErrorExpected = header.includes("error");
  return { kind: "statement", sql, isErrorExpected, isSkipped };
}

function queryBlock(
  header: string,
  lines: Lines,
  isSkipped: boolean,
): SltBlock {
  const sql = lines.takeUntil((line) => line === "----").join("\n");
  const hasExpected = lines.peek() === "----";
  if (hasExpected) lines.next();
  const expected = hasExpected ? lines.takeUntil() : [];
  return { kind: "query", sql, sort: sortOf(header), expected, isSkipped };
}

function sortOf(header: string): SortMode {
  const mode = header.split(/\s+/u, 3)[2];
  return mode === "rowsort" || mode === "valuesort" ? mode : "nosort";
}

// --- Running -----------------------------------------------------------------

export async function runSlt(
  session: Session,
  blocks: readonly SltBlock[],
): Promise<SltResult> {
  const failures: SltFailure[] = [];
  let skipped = 0;
  for (const [blockIndex, block] of blocks.entries()) {
    if (block.isSkipped) {
      skipped++;
      continue;
    }
    const problem = await problemOf(session, block);
    if (problem !== null) {
      failures.push({ blockIndex, sql: block.sql, message: problem });
    }
  }
  const total = blocks.length;
  return {
    total,
    passed: total - skipped - failures.length,
    skipped,
    failures,
  };
}

/** What went wrong with a block; null if it did what it expected. */
async function problemOf(
  session: Session,
  block: SltBlock,
): Promise<string | null> {
  switch (block.kind) {
    case "statement": {
      return statementProblem(session, block.sql, block.isErrorExpected);
    }
    case "query": {
      return queryProblem(session, block.sql, block.sort, block.expected);
    }
  }
}

async function statementProblem(
  session: Session,
  sql: string,
  isErrorExpected: boolean,
): Promise<string | null> {
  const failure = await failureOf(session.execute(sql));
  if (failure === null) {
    return isErrorExpected ? "Expected error but statement succeeded" : null;
  }
  if (isErrorExpected) return null;
  return `Expected success but got error: ${failure}`;
}

async function queryProblem(
  session: Session,
  sql: string,
  sort: SortMode,
  expected: readonly string[],
): Promise<string | null> {
  let rows: readonly (readonly Value[])[];
  try {
    const [result] = await session.execute(sql);
    if (result === undefined) return "Expected rows result but got nothing";
    rows = result.rows;
  } catch (error) {
    return `Query threw error: ${messageOf(error)}`;
  }
  const actual = sorted(linesOf(rows, sort), sort);
  const wanted = sorted(expected, sort);
  if (actual.join("\n") === wanted.join("\n")) return null;
  const label = sort === "valuesort" ? "Values" : "Row";
  return `${label} mismatch (${sort})\nExpected:\n  ${wanted.join("\n  ")}\nActual:\n  ${actual.join("\n  ")}`;
}

/** The rows as lines: one per row, or one per value for valuesort. */
function linesOf(
  rows: readonly (readonly Value[])[],
  sort: SortMode,
): string[] {
  if (sort === "valuesort") {
    return rows.flatMap((row) => row.map((value) => formatValue(value)));
  }
  return rows.map((row) => row.map((value) => formatValue(value)).join("|"));
}

function sorted(lines: readonly string[], sort: SortMode): readonly string[] {
  if (sort === "nosort") return lines;
  return lines.toSorted((a, b) => (a < b ? -1 : Number(a > b)));
}

function formatValue(value: Value): string {
  if (value === null) return "NULL";
  if (value instanceof Uint8Array) return bytesToHex(value).toUpperCase();
  if (typeof value === "object") return canonicalJson(value);
  return String(value);
}

/** The message of the error `work` fails with; null if it succeeds. */
async function failureOf(work: Promise<unknown>): Promise<string | null> {
  try {
    await work;
    return null;
  } catch (error) {
    return messageOf(error);
  }
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

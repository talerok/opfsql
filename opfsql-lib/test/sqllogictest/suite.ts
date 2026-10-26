import { expect } from "vitest";
import { openTestEngine } from "../../src/engine/tests/test-engine.js";
import { corpusFiles, readCorpusFile } from "./corpus.js";
import { parseRecords } from "./records.js";
import { type Outcome, runRecords } from "./run.js";
import { readSkips, recordKey } from "./skips.js";

// Running corpus files as tests: each file in a fresh database, a record
// failing only if skips.txt lists it, and a listed one failing. The files
// are spread over several test files, so that vitest runs them side by
// side.

/** Long enough for a file of ten thousand records on a loaded machine. */
export const FILE_TIMEOUT = 120_000;

/** The corpus files whose path `isWanted` takes. */
export function corpusFilesWhere(
  isWanted: (file: string) => boolean,
): string[] {
  return corpusFiles().filter((file) => isWanted(file));
}

/** A test of one corpus file: every record does what the file and skips.txt say. */
export function runsAsListed(file: string): () => Promise<void> {
  return async () => {
    const skips = readSkips();
    const outcomes = await runFile(file);
    const isListed = (outcome: Outcome): boolean =>
      skips.records.has(recordKey(file, outcome.line));
    const unexpected = outcomes
      .filter((outcome) => (outcome.failure !== null) !== isListed(outcome))
      .map((outcome) => describeOutcome(file, outcome));
    expect(unexpected).toEqual([]);
  };
}

async function runFile(file: string): Promise<Outcome[]> {
  const engine = await openTestEngine(`sqllogictest-${file}`);
  const session = engine.connect();
  try {
    return await runRecords(session, parseRecords(readCorpusFile(file)));
  } finally {
    await session.close();
    await engine.close();
  }
}

function describeOutcome(
  file: string,
  { line, sql, failure }: Outcome,
): string {
  const key = recordKey(file, line);
  if (failure === null) return `${key} passes, but is listed in skips.txt`;
  return `${key} ${failure}\n  ${sql}`;
}

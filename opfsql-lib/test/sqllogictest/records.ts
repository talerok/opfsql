// ---------------------------------------------------------------------------
// The sqllogictest file format, as SQLite's sqllogictest reads it: records
// separated by blank lines, `#` lines being comments.
//
//   statement ok | statement error           then the SQL
//   query <types> [<sort> [<label>]]         then the SQL, `----`, the result
//   hash-threshold <n>                       results of more values are hashed
//   halt                                     the rest of the file is skipped
//
// `skipif <engine>` and `onlyif <engine>` lines before a record say which
// engines run it. We run what SQLite runs: a record is ours unless a skipif
// names sqlite or an onlyif names another engine.
// ---------------------------------------------------------------------------

/** The engine whose records we run: opfsql's reference. */
const OUR_ENGINE = "sqlite";

export type SortMode = "nosort" | "rowsort" | "valuesort";

/** A query's expected result: its values one per line, or their count and MD5. */
export type Expected =
  | { readonly kind: "values"; readonly values: readonly string[] }
  | { readonly kind: "hash"; readonly count: number; readonly hash: string };

export type Record =
  | {
      readonly kind: "statement";
      readonly line: number;
      readonly sql: string;
      readonly isErrorExpected: boolean;
    }
  | {
      readonly kind: "query";
      readonly line: number;
      readonly sql: string;
      /** One letter per column: I integer, R real, T text. */
      readonly types: string;
      readonly sort: SortMode;
      readonly expected: Expected;
    }
  | {
      readonly kind: "hash-threshold";
      readonly line: number;
      readonly threshold: number;
    }
  | { readonly kind: "halt"; readonly line: number };

/** A record's lines: where it starts, and its text. */
interface Block {
  readonly line: number;
  readonly lines: readonly string[];
}

const HASHED = /^(\d+) values hashing to ([0-9a-f]{32})$/u;
const SORT_MODES: ReadonlySet<string> = new Set([
  "nosort",
  "rowsort",
  "valuesort",
]);

function isSortMode(word: string): word is SortMode {
  return SORT_MODES.has(word);
}

/** The records of a file that SQLite runs, in order; nothing after a `halt`. */
export function parseRecords(text: string): Record[] {
  const records: Record[] = [];
  for (const block of blocksOf(text)) {
    const record = recordOf(block);
    if (record === null) continue;
    records.push(record);
    if (record.kind === "halt") break;
  }
  return records;
}

/** The file's blocks: runs of lines between blank lines, comments left out. */
function blocksOf(text: string): Block[] {
  const blocks: Block[] = [];
  let current: { line: number; lines: string[] } | null = null;
  for (const [index, raw] of text.split("\n").entries()) {
    const line = raw.trimEnd();
    if (line.trim() === "") {
      if (current !== null) blocks.push(current);
      current = null;
      continue;
    }
    if (line.startsWith("#")) continue;
    current ??= { line: index + 1, lines: [] };
    current.lines.push(line);
  }
  if (current !== null) blocks.push(current);
  return blocks;
}

/** A block as a record, or null if its conditions keep it from SQLite. */
function recordOf(block: Block): Record | null {
  let start = 0;
  let isOurs = true;
  for (const line of block.lines) {
    const [word = "", engine = ""] = line.split(/\s+/u, 2);
    if (word === "skipif") isOurs &&= engine !== OUR_ENGINE;
    else if (word === "onlyif") isOurs &&= engine === OUR_ENGINE;
    else break;
    start++;
  }
  if (!isOurs) return null;
  const lines = block.lines.slice(start);
  return headedRecord(block.line + start, lines);
}

function headedRecord(line: number, lines: readonly string[]): Record | null {
  const [header = "", ...body] = lines;
  const words = header.split(/\s+/u);
  switch (words[0]) {
    case "statement": {
      const isErrorExpected = words[1] === "error";
      return { kind: "statement", line, sql: body.join("\n"), isErrorExpected };
    }
    case "query": {
      return queryRecord(line, words, body);
    }
    case "hash-threshold": {
      return { kind: "hash-threshold", line, threshold: Number(words[1]) };
    }
    case "halt": {
      return { kind: "halt", line };
    }
    default: {
      return null;
    }
  }
}

function queryRecord(
  line: number,
  words: readonly string[],
  body: readonly string[],
): Record {
  const divider = body.indexOf("----");
  const sqlLines = divider === -1 ? body : body.slice(0, divider);
  const resultLines = divider === -1 ? [] : body.slice(divider + 1);
  const sort = words[2] ?? "nosort";
  return {
    kind: "query",
    line,
    sql: sqlLines.join("\n"),
    types: words[1] ?? "",
    sort: isSortMode(sort) ? sort : "nosort",
    expected: expectedOf(resultLines),
  };
}

function expectedOf(lines: readonly string[]): Expected {
  const [only = ""] = lines;
  const hashed = lines.length === 1 ? HASHED.exec(only) : null;
  if (hashed === null) return { kind: "values", values: lines };
  return { kind: "hash", count: Number(hashed[1]), hash: hashed[2] };
}

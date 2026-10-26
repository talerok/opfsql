import { readFileSync } from "node:fs";

// ---------------------------------------------------------------------------
// The records opfsql is expected to answer differently from SQLite, by file
// and line, each with a reason (README.md):
//
//   reason <key> <text>         a reason: a docs/semantics.md section, or
//                               "not supported: <feature>"
//   <path>:<line> <key>         a record of the corpus, and its reason
//
// `#` lines are comments.
// ---------------------------------------------------------------------------

export interface Skips {
  /** Each reason's text, by its key. */
  readonly reasons: ReadonlyMap<string, string>;
  /** Each skipped record's reason key, by `path:line`. */
  readonly records: ReadonlyMap<string, string>;
}

const SKIPS = new URL("skips.txt", import.meta.url);

export function readSkips(): Skips {
  const reasons = new Map<string, string>();
  const records = new Map<string, string>();
  for (const line of readFileSync(SKIPS, "utf8").split("\n")) {
    const [first = "", second = "", ...rest] = line.trim().split(/\s+/u);
    if (first === "" || first.startsWith("#")) continue;
    if (first === "reason") reasons.set(second, rest.join(" "));
    else records.set(first, second);
  }
  for (const [record, reason] of records) {
    if (!reasons.has(reason)) {
      throw new RangeError(`${record}: no reason ${reason} in skips.txt`);
    }
  }
  return { reasons, records };
}

/** The key a record has in the list. */
export function recordKey(path: string, line: number): string {
  return `${path}:${String(line)}`;
}

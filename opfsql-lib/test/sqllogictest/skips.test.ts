import { describe, expect, it } from "vitest";
import { corpusFiles, readCorpusFile } from "./corpus.js";
import { parseRecords } from "./records.js";
import { readSkips, recordKey } from "./skips.js";

describe("skips.txt", () => {
  it("lists only records the corpus has, each with a known reason", () => {
    const records = new Set<string>();
    for (const file of corpusFiles()) {
      const parsed = parseRecords(readCorpusFile(file));
      for (const record of parsed) records.add(recordKey(file, record.line));
    }
    const { records: listed } = readSkips();
    const unknown = [...listed.keys()].filter((key) => !records.has(key));
    expect(unknown).toEqual([]);
  });
});

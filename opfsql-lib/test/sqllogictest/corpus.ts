import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

// The vendored corpus (README.md): gzipped files under corpus/, named by
// their path in sqllogictest's repository.

const CORPUS = fileURLToPath(new URL("corpus/", import.meta.url));
const GZIP = ".gz";

/** Every corpus file, by its path in the repository: `test/select1.test`. */
export function corpusFiles(): string[] {
  const entries = readdirSync(CORPUS, { recursive: true, encoding: "utf8" });
  return entries
    .filter((entry) => entry.endsWith(GZIP))
    .map((entry) => entry.slice(0, -GZIP.length).replaceAll("\\", "/"))
    .toSorted((a, b) => a.localeCompare(b));
}

/** A corpus file's text, as the repository has it. */
export function readCorpusFile(file: string): string {
  const gzipped = readFileSync(path.join(CORPUS, `${file}${GZIP}`));
  return gunzipSync(gzipped).toString("utf8");
}

import "opfs-mock";
import "../setup/web-locks.js";
import { Engine } from "../../src/worker-entry.js";
import { corpusFiles, readCorpusFile } from "./corpus.js";
import { parseRecords } from "./records.js";
import { runRecords } from "./run.js";
import { readSkips, recordKey } from "./skips.js";

// Every record that fails, and every skip that no longer does, one per
// line, for bringing skips.txt up to date:
//
//   npx vite-node test/sqllogictest/triage.ts [path …]
//
// Each line is `path:line<TAB>failure<TAB>first line of the SQL`, or
// `path:line<TAB>passes, but is listed in skips.txt`.

const skips = readSkips();
const wanted = process.argv.slice(2);
const files = wanted.length > 0 ? wanted : corpusFiles();
let failures = 0;
for (const path of files) {
  const engine = await Engine.open(`triage-${path}`);
  const session = engine.connect();
  const outcomes = await runRecords(
    session,
    parseRecords(readCorpusFile(path)),
  );
  await session.close();
  await engine.close();
  for (const { line, sql, failure } of outcomes) {
    const key = recordKey(path, line);
    const isListed = skips.records.has(key);
    if (failure !== null && !isListed) {
      failures++;
      const [first = ""] = sql.split("\n", 1);
      const why = failure.replaceAll("\n", " ");
      process.stdout.write(`${key}\t${why}\t${first}\n`);
    }
    if (failure === null && isListed) {
      process.stdout.write(`${key}\tpasses, but is listed in skips.txt\n`);
    }
  }
  console.error(`${path}: ${String(outcomes.length)} records`);
}
console.error(`${String(failures)} failures not in skips.txt`);

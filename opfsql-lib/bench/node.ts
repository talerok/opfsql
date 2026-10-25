// Benchmarks in Node. opfs-mock keeps files in memory, so this measures the
// engine's CPU path; `npm run bench:browser` adds real disk I/O.
import "opfs-mock";
import "../test/setup/web-locks.js";
import { parseCli, report } from "./cli.js";
import { opfsHost, runScenarios } from "./harness.js";
import { SCENARIOS } from "./scenarios.js";

const options = parseCli(process.argv.slice(2));
report(await runScenarios(SCENARIOS, opfsHost, options), options);

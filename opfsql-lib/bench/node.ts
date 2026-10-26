// Benchmarks in Node. `npm run bench` bundles this file and the engine with
// esbuild, minified as the package build does, and runs the bundle: what a
// user runs, not vite-node's transformed modules, whose imported functions
// are getter calls V8 does not inline. opfs-mock keeps files in memory, so
// this measures the engine's CPU path; `npm run bench:browser` adds real
// disk I/O.
import "opfs-mock";
import "../test/setup/web-locks.js";
import { parseCli, report } from "./cli.js";
import { opfsHost, runScenarios } from "./harness.js";
import { SCENARIOS } from "./scenarios.js";

const options = parseCli(process.argv.slice(2));
if (options !== undefined) {
  report(await runScenarios(SCENARIOS, opfsHost, options), options);
}

import { readFileSync, writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import {
  formatResults,
  type BenchOptions,
  type ScenarioResult,
} from "./harness.js";

export interface CliOptions extends BenchOptions {
  /** Write the raw results here, to compare against later. */
  json?: string;
  /** Results file of an earlier run to compare medians against. */
  compare?: string;
  browser: "chromium" | "firefox" | "webkit";
}

const USAGE = `options:
  --repeats <n>     timed runs per scenario, reported as the median (default 5)
  --warmup <n>      untimed runs before them (default 2)
  --filter <text>   only scenarios whose name contains <text>
  --json <file>     save results, e.g. as a baseline
  --compare <file>  compare against saved results
  --browser <name>  chromium | firefox | webkit (bench:browser only)`;

export function parseCli(args: string[]): CliOptions {
  const { values } = parseArgs({
    args: args.filter((arg) => arg !== "--"),
    options: {
      repeats: { type: "string", default: "5" },
      warmup: { type: "string", default: "2" },
      filter: { type: "string" },
      json: { type: "string" },
      compare: { type: "string" },
      browser: { type: "string", default: "chromium" },
      help: { type: "boolean" },
    },
  });
  if (values.help) {
    console.log(USAGE);
    process.exit(0);
  }
  const browser = values.browser;
  if (browser !== "chromium" && browser !== "firefox" && browser !== "webkit") {
    throw new Error(`unknown browser "${browser}"\n${USAGE}`);
  }
  return {
    repeats: Number(values.repeats),
    warmup: Number(values.warmup),
    filter: values.filter,
    json: values.json,
    compare: values.compare,
    browser,
  };
}

export function report(results: ScenarioResult[], options: CliOptions): void {
  const baseline = options.compare
    ? (JSON.parse(readFileSync(options.compare, "utf8")) as ScenarioResult[])
    : undefined;
  console.log(formatResults(results, baseline));
  if (options.json) {
    writeFileSync(options.json, JSON.stringify(results, null, 2) + "\n");
  }
}

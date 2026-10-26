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

/** The options on the command line; undefined after printing --help. */
export function parseCli(argv: string[]): CliOptions | undefined {
  const { values } = parseArgs({
    args: argv.filter((argument) => argument !== "--"),
    options: {
      repeats: { type: "string", default: "5" },
      warmup: { type: "string", default: "2" },
      filter: { type: "string" },
      json: { type: "string" },
      compare: { type: "string" },
      browser: { type: "string", default: "chromium" },
      help: { type: "boolean", default: false },
    },
  });
  if (values.help) {
    print(USAGE);
    return undefined;
  }
  const { browser } = values;
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
  const { compare, json } = options;
  const baseline = compare === undefined ? undefined : readResults(compare);
  print(formatResults(results, baseline));
  if (json !== undefined) {
    writeFileSync(json, `${JSON.stringify(results, null, 2)}\n`);
  }
}

function readResults(file: string): ScenarioResult[] {
  return JSON.parse(readFileSync(file, "utf8")) as ScenarioResult[];
}

function print(text: string): void {
  process.stdout.write(`${text}\n`);
}

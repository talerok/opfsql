import { Engine } from "../src/worker-entry.js";
import type { Scenario } from "./scenarios.js";

export interface BenchHost {
  open(databaseName: string): Promise<Engine>;
  remove(databaseName: string): Promise<void>;
}

/**
 * Each database goes through Engine.open — the production stack — on
 * whatever OPFS the environment provides: opfs-mock in Node, the real one in
 * a browser worker.
 */
export const opfsHost: BenchHost = {
  open: async (databaseName) => Engine.open(databaseName),
  async remove(databaseName) {
    const root = await navigator.storage.getDirectory();
    for (const suffix of [".opfsql", ".opfsql-wal"]) {
      await root.removeEntry(`${databaseName}${suffix}`);
    }
  },
};

export interface BenchOptions {
  repeats: number;
  /** Untimed runs before the measured ones, so the JIT settles first. */
  warmup: number;
  /** Runs only scenarios whose name contains this text. */
  filter?: string;
}

/** Collects garbage before a timed run when the host allows it (node --expose-gc). */
function collectGarbage(): void {
  globalThis.gc?.();
}

interface OpenDatabase {
  engine: Engine;
  databaseName: string;
}

export interface ScenarioResult {
  scenario: string;
  ops: number;
  /** Wall time of each run, in milliseconds. */
  runs: number[];
}

export async function runScenarios(
  scenarios: Scenario[],
  host: BenchHost,
  options: BenchOptions,
): Promise<ScenarioResult[]> {
  const { filter = "" } = options;
  const results: ScenarioResult[] = [];
  for (const scenario of scenarios) {
    if (!scenario.name.includes(filter)) continue;
    results.push({
      scenario: scenario.name,
      ops: scenario.ops,
      runs: await measure(scenario, host, options),
    });
  }
  return results;
}

/**
 * The times of the scenario's runs after the warmup ones. Read-only
 * scenarios share one database; the others get a fresh one each run.
 */
async function measure(
  scenario: Scenario,
  host: BenchHost,
  { warmup, repeats }: BenchOptions,
): Promise<number[]> {
  const runs: number[] = [];
  const isShared = scenario.readOnly === true;
  let shared: OpenDatabase | null = null;
  for (let index = 0; index < warmup + repeats; index++) {
    const database: OpenDatabase =
      shared ?? (await prepare(scenario, host, index));
    if (isShared) shared = database;
    const time = await timeRun(scenario, database.engine);
    if (index >= warmup) runs.push(time);
    if (!isShared) await dispose(database, host);
  }
  if (shared !== null) await dispose(shared, host);
  return runs;
}

/** A new database, set up for the scenario. */
async function prepare(
  scenario: Scenario,
  host: BenchHost,
  index: number,
): Promise<OpenDatabase> {
  const databaseName = `bench-${String(Date.now())}-${String(index)}`;
  const engine = await host.open(databaseName);
  const session = engine.connect();
  await scenario.setup(session);
  await session.close();
  return { engine, databaseName };
}

/** Milliseconds one run of the scenario takes, on a session of its own. */
async function timeRun(scenario: Scenario, engine: Engine): Promise<number> {
  const session = engine.connect();
  collectGarbage();
  const start = performance.now();
  await scenario.run(session);
  const time = performance.now() - start;
  await session.close();
  return time;
}

async function dispose(database: OpenDatabase, host: BenchHost): Promise<void> {
  await database.engine.close();
  await host.remove(database.databaseName);
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

function median(values: readonly number[]): number {
  const sorted = values.toSorted((a, b) => a - b);
  const middle = sorted.length >> 1;
  if (sorted.length % 2 === 1) return sorted[middle];
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

/** Plain-text table; with a baseline, adds the change of each median. */
export function formatResults(
  results: ScenarioResult[],
  baseline?: ScenarioResult[],
): string {
  const header = ["scenario", "median ms", "min ms", "µs/op"];
  if (baseline !== undefined) header.push("vs baseline");
  const rows = results.map((result) => rowOf(result, baseline));
  const widths = header.map((title, column) =>
    Math.max(title.length, ...rows.map((row) => row[column].length)),
  );
  const line = (cells: string[]): string =>
    cells
      .map((cell, column) =>
        column === 0
          ? cell.padEnd(widths[column])
          : cell.padStart(widths[column]),
      )
      .join("  ");
  return [line(header), ...rows.map((row) => line(row))].join("\n");
}

function rowOf(
  result: ScenarioResult,
  baseline: ScenarioResult[] | undefined,
): string[] {
  const medianMs = median(result.runs);
  const row = [
    result.scenario,
    medianMs.toFixed(1),
    Math.min(...result.runs).toFixed(1),
    ((medianMs * 1000) / result.ops).toFixed(1),
  ];
  if (baseline !== undefined) {
    row.push(changeFrom(baseline, result.scenario, medianMs));
  }
  return row;
}

/** The change from the baseline's median, or "new" for a scenario it lacks. */
function changeFrom(
  baseline: readonly ScenarioResult[],
  scenario: string,
  medianMs: number,
): string {
  const base = baseline.find((candidate) => candidate.scenario === scenario);
  if (base === undefined) return "new";
  const change = (medianMs / median(base.runs) - 1) * 100;
  const sign = change >= 0 ? "+" : "";
  return `${sign}${change.toFixed(0)}%`;
}

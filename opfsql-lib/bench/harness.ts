import { Engine } from "../src/worker-entry.js";
import type { Scenario } from "./scenarios.js";

export interface BenchHost {
  open(dbName: string): Promise<Engine>;
  remove(dbName: string): Promise<void>;
}

/**
 * Each database goes through Engine.open — the production stack — on
 * whatever OPFS the environment provides: opfs-mock in Node, the real one in
 * a browser worker.
 */
export const opfsHost: BenchHost = {
  open: (dbName) => Engine.open(dbName),
  async remove(dbName) {
    const root = await navigator.storage.getDirectory();
    for (const file of [`${dbName}.opfsql`, `${dbName}.opfsql-wal`]) {
      await root.removeEntry(file);
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
const collectGarbage = (globalThis as { gc?: () => void }).gc ?? (() => {});

interface OpenDatabase {
  engine: Engine;
  dbName: string;
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
  const results: ScenarioResult[] = [];
  for (const scenario of scenarios) {
    if (options.filter && !scenario.name.includes(options.filter)) continue;
    results.push({
      scenario: scenario.name,
      ops: scenario.ops,
      runs: await measure(scenario, host, options),
    });
  }
  return results;
}

async function measure(
  scenario: Scenario,
  host: BenchHost,
  { warmup, repeats }: BenchOptions,
): Promise<number[]> {
  const runs: number[] = [];
  let shared: OpenDatabase | null = null;

  for (let i = 0; i < warmup + repeats; i++) {
    let db: OpenDatabase | null = shared;
    if (!db) {
      const dbName = `bench-${Date.now()}-${i}`;
      const engine = await host.open(dbName);
      const session = engine.connect();
      await scenario.setup(session);
      await session.close();
      db = { engine, dbName };
      if (scenario.readOnly) shared = db;
    }

    const session = db.engine.connect();
    collectGarbage();
    const start = performance.now();
    await scenario.run(session);
    if (i >= warmup) runs.push(performance.now() - start);
    await session.close();

    if (!scenario.readOnly) await dispose(db, host);
  }
  if (shared) await dispose(shared, host);
  return runs;
}

async function dispose(db: OpenDatabase, host: BenchHost): Promise<void> {
  await db.engine.close();
  await host.remove(db.dbName);
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

/** Plain-text table; with a baseline, adds the change of each median. */
export function formatResults(
  results: ScenarioResult[],
  baseline?: ScenarioResult[],
): string {
  const header = ["scenario", "median ms", "min ms", "µs/op"];
  if (baseline) header.push("vs baseline");

  const rows = results.map((r) => {
    const med = median(r.runs);
    const row = [
      r.scenario,
      med.toFixed(1),
      Math.min(...r.runs).toFixed(1),
      ((med * 1000) / r.ops).toFixed(1),
    ];
    if (baseline) {
      const base = baseline.find((b) => b.scenario === r.scenario);
      if (!base) {
        row.push("new");
      } else {
        const change = (med / median(base.runs) - 1) * 100;
        row.push(`${change >= 0 ? "+" : ""}${change.toFixed(0)}%`);
      }
    }
    return row;
  });

  const widths = header.map((h, i) =>
    Math.max(h.length, ...rows.map((row) => row[i].length)),
  );
  const line = (cells: string[]) =>
    cells
      .map((c, i) => (i === 0 ? c.padEnd(widths[i]) : c.padStart(widths[i])))
      .join("  ");
  return [line(header), ...rows.map(line)].join("\n");
}

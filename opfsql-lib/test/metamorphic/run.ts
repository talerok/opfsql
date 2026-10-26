import { closeCopies, type Copies, openCopies } from "./database.js";
import { checksOf } from "./oracles.js";
import { generateCase, type QueryCase } from "./query.js";
import { caseSeed, Random } from "./random.js";
import { reduceFailure } from "./reduce.js";
import { describeFailure } from "./report.js";
import { type Outcome, runCheck } from "./run-check.js";
import { type DatabaseSpec, generateDatabase } from "./schema.js";
import type { Check } from "./check.js";

// ---------------------------------------------------------------------------
// Running the oracles over one random database, case after case. The first
// failure of a database is reduced before it is reported; any later one is
// reported as generated.
// ---------------------------------------------------------------------------

export interface SeedReport {
  readonly checks: number;
  /** Checks skipped, by the error code a form failed with. */
  readonly skipped: ReadonlyMap<string, number>;
  readonly failures: readonly string[];
}

export async function runSeed(
  seed: number,
  caseCount: number,
): Promise<SeedReport> {
  const random = new Random(seed);
  const spec = generateDatabase(random);
  const copies = await openCopies(random, spec);
  const tally = new Tally();
  try {
    for (let index = 0; index < caseCount; index++) {
      await runCase({ seed, index, spec, copies }, tally);
    }
  } finally {
    await closeCopies(copies);
  }
  return tally.report();
}

/** Where a case runs: its seed and number, and its database. */
interface CaseSite {
  readonly seed: number;
  readonly index: number;
  readonly spec: DatabaseSpec;
  readonly copies: Copies;
}

async function runCase(site: CaseSite, tally: Tally): Promise<void> {
  const random = new Random(caseSeed(site.seed, site.index));
  const queryCase = generateCase(random, site.spec);
  for (const check of checksOf(queryCase)) {
    const outcome = await runCheck(check, site.copies);
    tally.count(outcome);
    if (outcome.kind !== "failed") continue;
    const shouldReduce = tally.failureCount() === 0;
    const failing = { site, queryCase, check, reason: outcome.reason };
    tally.fail(await describe(failing, shouldReduce));
  }
}

interface Failing {
  readonly site: CaseSite;
  readonly queryCase: QueryCase;
  readonly check: Check;
  readonly reason: string;
}

async function describe(
  failing: Failing,
  shouldReduce: boolean,
): Promise<string> {
  const { site, check, reason } = failing;
  const { seed, index, spec, copies } = site;
  const generated = {
    seed,
    index,
    spec,
    rows: copies.rows,
    pageSize: copies.pageSize,
  };
  const reduction = shouldReduce
    ? await reduceFailure(spec, copies, failing.queryCase, check.oracle)
    : null;
  if (reduction === null) {
    return describeFailure({ ...generated, check, reason, isReduced: false });
  }
  return describeFailure({ ...generated, ...reduction, isReduced: true });
}

class Tally {
  private checks = 0;
  private readonly skipped = new Map<string, number>();
  private readonly failures: string[] = [];

  count(outcome: Outcome): void {
    this.checks++;
    if (outcome.kind !== "skipped") return;
    const count = this.skipped.get(outcome.code) ?? 0;
    this.skipped.set(outcome.code, count + 1);
  }

  fail(description: string): void {
    this.failures.push(description);
  }

  failureCount(): number {
    return this.failures.length;
  }

  report(): SeedReport {
    const { checks, skipped, failures } = this;
    return { checks, skipped, failures };
  }
}

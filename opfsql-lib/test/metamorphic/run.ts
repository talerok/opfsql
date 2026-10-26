import { OpfsqlError } from "../../src/errors.js";
import type { Rows } from "./compare.js";
import { closeCopies, type Copies, type Copy, openCopies } from "./database.js";
import { type Check, copyDifference } from "./check.js";
import { checksOf } from "./oracles.js";
import { generateCase } from "./query.js";
import { caseSeed, Random } from "./random.js";
import {
  createIndexSql,
  createTableSql,
  type DatabaseSpec,
  generateDatabase,
} from "./schema.js";

// ---------------------------------------------------------------------------
// Running the oracles over one random database. Errors are not results
// (docs/planning.md, rule 4): a check where any form fails at run time is
// skipped and counted. Any other error — one binding should have caught,
// or an internal one — is a failure, as a disagreement is.
// ---------------------------------------------------------------------------

/** What generated SQL may fail with at run time, whatever the plan. */
const RUNTIME_ERRORS: ReadonlySet<string> = new Set([
  "DATATYPE_MISMATCH",
  "INTEGER_OVERFLOW",
]);

export type Outcome =
  | { readonly kind: "agreed" }
  | { readonly kind: "skipped"; readonly code: string }
  | { readonly kind: "failed"; readonly reason: string };

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
    tally.add(outcome, () =>
      describeFailure({ ...site, check, reason: reasonOf(outcome) }),
    );
  }
}

function reasonOf(outcome: Outcome): string {
  return outcome.kind === "failed" ? outcome.reason : "";
}

class Tally {
  private checks = 0;
  private readonly skipped = new Map<string, number>();
  private readonly failures: string[] = [];

  /** Counts `outcome`; `describe` says what failed, when it did. */
  add(outcome: Outcome, describe: () => string): void {
    this.checks++;
    switch (outcome.kind) {
      case "agreed": {
        return;
      }
      case "skipped": {
        const count = this.skipped.get(outcome.code) ?? 0;
        this.skipped.set(outcome.code, count + 1);
        return;
      }
      case "failed": {
        this.failures.push(describe());
      }
    }
  }

  report(): SeedReport {
    const { checks, skipped, failures } = this;
    return { checks, skipped, failures };
  }
}

/** Runs every form on both copies, and holds them to the check and to each other. */
export async function runCheck(check: Check, copies: Copies): Promise<Outcome> {
  const plain = await runForms(check, copies.plain);
  const indexed = await runForms(check, copies.indexed);
  if (!Array.isArray(plain)) return plain;
  if (!Array.isArray(indexed)) return indexed;
  const reasons = [
    labelled("plain copy", check.verdict(plain)),
    labelled("indexed copy", check.verdict(indexed)),
    ...check.forms.map((form, index) =>
      labelled(
        `form ${String(index + 1)} on the two copies`,
        copyDifference(form, plain[index] ?? [], indexed[index] ?? []),
      ),
    ),
  ].filter((reason) => reason !== null);
  return reasons.length === 0
    ? { kind: "agreed" }
    : { kind: "failed", reason: reasons.join("\n") };
}

function labelled(label: string, difference: string | null): string | null {
  return difference === null ? null : `${label}: ${difference}`;
}

/** Every form's rows, or how a form failed. */
async function runForms(check: Check, copy: Copy): Promise<Rows[] | Outcome> {
  const results: Rows[] = [];
  for (const form of check.forms) {
    try {
      const result = await copy.session.query(form.sql, form.parameters);
      results.push(result.rows);
    } catch (error) {
      return outcomeOfError(error, form.sql);
    }
  }
  return results;
}

function outcomeOfError(error: unknown, sql: string): Outcome {
  const isRuntime =
    error instanceof OpfsqlError && RUNTIME_ERRORS.has(error.code);
  if (isRuntime) return { kind: "skipped", code: error.code };
  const code = error instanceof OpfsqlError ? error.code : "not an OpfsqlError";
  return { kind: "failed", reason: `${code}: ${String(error)}\n  in: ${sql}` };
}

interface FailureContext {
  readonly seed: number;
  readonly index: number;
  readonly spec: DatabaseSpec;
  readonly copies: Copies;
  readonly check: Check;
  readonly reason: string;
}

/** Everything needed to see the failure again, by hand or by its seed. */
export function describeFailure(context: FailureContext): string {
  const { spec, copies, check } = context;
  const sizes = spec.tables.map(
    (table) =>
      `${table.name}: ${String(copies.rows.get(table.name)?.length ?? 0)} rows`,
  );
  return [
    `seed ${String(context.seed)}, case ${String(context.index)}: ${check.oracle} disagrees`,
    context.reason,
    "schema:",
    ...spec.tables.map((table) => `  ${createTableSql(table)};`),
    "indexed copy only:",
    ...spec.indexes.map(
      (index) =>
        `  ${createIndexSql(index)};${index.isEarly ? " -- before the rows" : ""}`,
    ),
    `data: ${sizes.join(", ")}; page size ${String(copies.pageSize)}`,
    "forms:",
    ...check.forms.map((form) => `  ${form.sql};`),
  ].join("\n");
}

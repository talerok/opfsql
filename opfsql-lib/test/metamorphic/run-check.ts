import { OpfsqlError } from "../../src/errors.js";
import { type Check, copyDifference } from "./check.js";
import type { Rows } from "./compare.js";
import type { Copies, Copy } from "./database.js";

// ---------------------------------------------------------------------------
// Running one check. Errors are not results (docs/planning.md, rule 4): a
// check where any form fails at run time is skipped and counted. Any other
// error — one binding should have caught, or an internal one — is a
// failure, as a disagreement is.
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

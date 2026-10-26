import { OpfsqlError } from "../../src/errors.js";
import type { Result } from "../../src/engine/api/index.js";
import type { PreparedStatement } from "../../src/engine/session.js";
import { type Check, copyDifference, type Form } from "./check.js";
import type { Rows } from "./compare.js";
import type { Copies, Copy } from "./database.js";

// ---------------------------------------------------------------------------
// Running one check. Errors are not results (docs/planning.md, rule 4): a
// check where any form fails at run time is skipped and counted. Any other
// error — one binding should have caught, or an internal one — is a
// failure, as a disagreement is, and a failure on either copy is reported
// before a skip on the other: a crash only an index path causes must show.
// ---------------------------------------------------------------------------

/** What generated SQL may fail with at run time, whatever the plan. */
export const RUNTIME_ERRORS: ReadonlySet<string> = new Set([
  "DATATYPE_MISMATCH",
  "INTEGER_OVERFLOW",
]);

export type Outcome =
  | { readonly kind: "agreed" }
  | { readonly kind: "skipped"; readonly code: string }
  | { readonly kind: "failed"; readonly reason: string };

/** Runs every form on both copies, and holds them to the check and to each other. */
export async function runCheck(check: Check, copies: Copies): Promise<Outcome> {
  const plain = await runForms(check, copies.plain, "plain copy");
  const indexed = await runForms(check, copies.indexed, "indexed copy");
  for (const outcome of [plain, indexed]) {
    if (!Array.isArray(outcome) && outcome.kind === "failed") return outcome;
  }
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

/**
 * Every form's rows, or how a form failed. Forms with parameters run as
 * prepared statements, one for each SQL text, so a statement's second run
 * reuses its plan with other values.
 */
async function runForms(
  check: Check,
  copy: Copy,
  label: string,
): Promise<Rows[] | Outcome> {
  const statements = new Map<string, PreparedStatement>();
  const results: Rows[] = [];
  let running = "";
  try {
    for (const form of check.forms) {
      running = form.sql;
      const result = await runForm(form, copy, statements);
      results.push(result.rows);
    }
  } catch (error) {
    return outcomeOfError(error, running, label);
  } finally {
    for (const statement of statements.values()) statement.close();
  }
  return results;
}

async function runForm(
  form: Form,
  copy: Copy,
  statements: Map<string, PreparedStatement>,
): Promise<Result> {
  if (form.parameters.length === 0) return copy.session.query(form.sql);
  const statement =
    statements.get(form.sql) ?? (await copy.session.prepare(form.sql));
  statements.set(form.sql, statement);
  return statement.query(form.parameters);
}

function outcomeOfError(error: unknown, sql: string, label: string): Outcome {
  const isRuntime =
    error instanceof OpfsqlError && RUNTIME_ERRORS.has(error.code);
  if (isRuntime) return { kind: "skipped", code: error.code };
  const code = error instanceof OpfsqlError ? error.code : "not an OpfsqlError";
  const reason = `${label}: ${code}: ${String(error)}\n  in: ${sql}`;
  return { kind: "failed", reason };
}

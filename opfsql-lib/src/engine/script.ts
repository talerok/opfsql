import { OpfsqlError } from "../errors.js";
import type { Catalog } from "../store/catalog.js";
import type { Result } from "./api/index.js";
import { changesSchema, checkParameters, type Step } from "./statements.js";

// Running a script: its statements in order, each failure tagged with the
// statement's position for execute() to report.

/**
 * Statement `index` of a script failed; execute() reports the failure,
 * located. It never leaves the session.
 */
export class StepFailure extends OpfsqlError {
  override readonly name = "StepFailure";

  constructor(
    readonly index: number,
    readonly failure: unknown,
  ) {
    super("INTERNAL", `statement ${String(index)} of the script failed`, {
      cause: failure,
    });
  }
}

/** Runs each step in order; a failure becomes the StepFailure of its step. */
export async function eachStep(
  steps: readonly Step[],
  run: (step: Step) => Promise<Result> | Result,
): Promise<Result[]> {
  const results: Result[] = [];
  for (const [index, step] of steps.entries()) {
    try {
      results.push(await run(step));
    } catch (error) {
      throw new StepFailure(index, error);
    }
  }
  return results;
}

/** Binds a script's statements up to and including its first DDL. */
export function bindAhead(steps: readonly Step[], catalog: Catalog): void {
  for (const [index, step] of steps.entries()) {
    try {
      checkParameters(step.bind(catalog));
    } catch (error) {
      throw new StepFailure(index, error);
    }
    if (changesSchema(step.statement)) return;
  }
}

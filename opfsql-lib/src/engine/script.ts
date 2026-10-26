import { OpfsqlError } from "../errors.js";
import type { Schema } from "../schema/index.js";
import type { Result } from "./api/index.js";
import {
  changesSchema,
  checkParameters,
  type CachedStatement,
} from "./statements.js";

// ---------------------------------------------------------------------------
// Running a script: its statements in order, each failure tagged with the
// statement's position for execute() to report.
// ---------------------------------------------------------------------------

/**
 * Statement `index` of a script failed; execute() reports the failure,
 * located (locateStatement). It never leaves the session, so its code is
 * INTERNAL: one that escaped would be the bug it says it is.
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
  steps: readonly CachedStatement[],
  run: (step: CachedStatement) => Promise<Result> | Result,
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

/** Plans a script's statements up to and including its first DDL. */
export function planAhead(
  steps: readonly CachedStatement[],
  schema: Schema,
): void {
  for (const [index, step] of steps.entries()) {
    try {
      checkParameters(step.plan(schema));
    } catch (error) {
      throw new StepFailure(index, error);
    }
    if (changesSchema(step.statement)) return;
  }
}

/**
 * Records which statement of a script `error` came from. Only for an error
 * made for this call: the engine creates every error it reports afresh.
 */
export function locateStatement(
  error: OpfsqlError,
  statementIndex: number,
): OpfsqlError {
  Object.defineProperty(error, "statementIndex", {
    value: statementIndex,
    enumerable: true,
    configurable: true,
    writable: false,
  });
  return error;
}

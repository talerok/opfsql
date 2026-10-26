import type { Check } from "./check.js";
import type { Row } from "./data.js";
import { closeCopies, type Copies, loadCopies } from "./database.js";
import {
  childrenOf,
  type Expression,
  replacements,
  withChild,
} from "./expression.js";
import { checksOf } from "./oracles.js";
import type { QueryCase } from "./query.js";
import { runCheck } from "./run-check.js";
import type { DatabaseSpec } from "./schema.js";

// ---------------------------------------------------------------------------
// Reducing a failure, greedily and within a budget of attempts: each
// condition to one of its parts while the oracle still isStillFailing, the select
// list to fewer columns, then each table to fewer rows.
// ---------------------------------------------------------------------------

/** Times a reduction runs the check, at most. */
const MAX_ATTEMPTS = 300;

export interface Reduction {
  readonly queryCase: QueryCase;
  readonly rows: ReadonlyMap<string, readonly Row[]>;
  readonly check: Check;
  readonly reason: string;
}

type RowsByTable = ReadonlyMap<string, readonly Row[]>;

/** A smaller case that isStillFailing `oracle` as `queryCase` does, or null if none was found. */
export async function reduceFailure(
  spec: DatabaseSpec,
  copies: Copies,
  queryCase: QueryCase,
  oracle: string,
): Promise<Reduction | null> {
  const reducer = new Reducer(spec, copies.pageSize, oracle);
  const reducedCase = await reducer.reduceQuery(queryCase, copies);
  const rows = await reducer.reduceRows(reducedCase, copies.rows);
  return reducer.outcome(reducedCase, rows);
}

class Reducer {
  private attempts = 0;

  constructor(
    private readonly spec: DatabaseSpec,
    private readonly pageSize: number,
    private readonly oracle: string,
  ) {}

  async reduceQuery(queryCase: QueryCase, copies: Copies): Promise<QueryCase> {
    const isStillFailing = async (candidate: QueryCase): Promise<boolean> =>
      (await this.failure(candidate, copies)) !== null;
    let current = queryCase;
    // The parameters' form is the condition with chosen constants: reducing
    // one without the other would compare two different conditions.
    if (this.oracle !== "parameters") {
      const condition = await smallest(current.condition, async (candidate) =>
        isStillFailing({ ...current, condition: candidate }),
      );
      current = { ...current, condition };
    }
    const groupCondition = await smallest(
      current.groupCondition,
      async (candidate) =>
        isStillFailing({ ...current, groupCondition: candidate }),
    );
    current = { ...current, groupCondition };
    return this.fewerOutputs(current, isStillFailing);
  }

  async reduceRows(
    queryCase: QueryCase,
    rows: RowsByTable,
  ): Promise<RowsByTable> {
    let current = rows;
    for (const table of this.spec.tables) {
      current = await this.reduceTable(queryCase, current, table.name);
    }
    return current;
  }

  /** The failure the reduced case shows, run once more whatever the budget. */
  async outcome(
    queryCase: QueryCase,
    rows: RowsByTable,
  ): Promise<Reduction | null> {
    const copies = await loadCopies(this.spec, rows, this.pageSize);
    try {
      const failure = await this.failure(queryCase, copies, false);
      return failure === null ? null : { queryCase, rows, ...failure };
    } finally {
      await closeCopies(copies);
    }
  }

  private async fewerOutputs(
    queryCase: QueryCase,
    isStillFailing: (candidate: QueryCase) => Promise<boolean>,
  ): Promise<QueryCase> {
    let current = queryCase;
    for (let index = current.outputs.length - 1; index >= 0; index--) {
      if (current.outputs.length === 1) break;
      const outputs = current.outputs.filter((_, at) => at !== index);
      const candidate = { ...current, outputs };
      if (await isStillFailing(candidate)) current = candidate;
    }
    return current;
  }

  /** Without chunks of the table's rows, halving the chunks down to single rows. */
  private async reduceTable(
    queryCase: QueryCase,
    rows: RowsByTable,
    table: string,
  ): Promise<RowsByTable> {
    let current = rows;
    let chunk = Math.ceil((current.get(table)?.length ?? 0) / 2);
    while (chunk > 0) {
      current = await this.withoutChunks(queryCase, current, { table, chunk });
      chunk = chunk === 1 ? 0 : Math.ceil(chunk / 2);
    }
    return current;
  }

  private async withoutChunks(
    queryCase: QueryCase,
    rows: RowsByTable,
    { table, chunk }: { table: string; chunk: number },
  ): Promise<RowsByTable> {
    let current = rows;
    let start = 0;
    while (start < (current.get(table)?.length ?? 0)) {
      const tableRows = current.get(table) ?? [];
      const fewer = [
        ...tableRows.slice(0, start),
        ...tableRows.slice(start + chunk),
      ];
      const candidate = new Map(current).set(table, fewer);
      if (await this.failsOn(queryCase, candidate)) current = candidate;
      else start += chunk;
    }
    return current;
  }

  private async failsOn(
    queryCase: QueryCase,
    rows: RowsByTable,
  ): Promise<boolean> {
    if (this.attempts >= MAX_ATTEMPTS) return false;
    const copies = await loadCopies(this.spec, rows, this.pageSize);
    try {
      return (await this.failure(queryCase, copies)) !== null;
    } finally {
      await closeCopies(copies);
    }
  }

  /** How the oracle isStillFailing on `queryCase`, or null if it does not or the budget is spent. */
  private async failure(
    queryCase: QueryCase,
    copies: Copies,
    isCounted = true,
  ): Promise<{ check: Check; reason: string } | null> {
    if (isCounted && this.attempts >= MAX_ATTEMPTS) return null;
    if (isCounted) this.attempts++;
    const check = checksOf(queryCase).find(
      (each) => each.oracle === this.oracle,
    );
    if (check === undefined) return null;
    const outcome = await runCheck(check, copies);
    return outcome.kind === "failed" ? { check, reason: outcome.reason } : null;
  }
}

/** The smallest part of `expression` that still `isStillFailing`, one step at a time. */
async function smallest(
  expression: Expression,
  isStillFailing: (candidate: Expression) => Promise<boolean>,
): Promise<Expression> {
  let current = expression;
  let next = await firstFailing(current, isStillFailing);
  while (next !== null) {
    current = next;
    next = await firstFailing(current, isStillFailing);
  }
  return current;
}

async function firstFailing(
  expression: Expression,
  isStillFailing: (candidate: Expression) => Promise<boolean>,
): Promise<Expression | null> {
  for (const candidate of smaller(expression)) {
    if (await isStillFailing(candidate)) return candidate;
  }
  return null;
}

/** Every expression one step smaller than `expression`: a node replaced, at any depth. */
function* smaller(expression: Expression): Generator<Expression> {
  yield* replacements(expression);
  if (expression.kind !== "node") return;
  for (const [index, child] of childrenOf(expression).entries()) {
    for (const candidate of smaller(child)) {
      yield withChild(expression, index, candidate);
    }
  }
}

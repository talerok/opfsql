import type { Run, Tuple } from "../expressions/index.js";

// ---------------------------------------------------------------------------
// Operators (docs/planning.md, Execution): a plan node compiled for one run,
// pulled in batches of at most what its caller asks for. An operator
// evaluates no more of its input than the rows it returns need — but for a
// sort, an aggregate, a hash join's build side and a set operation's other
// side, which need all of theirs — so a row past a LIMIT, or past the one
// row EXISTS needs, is never evaluated and cannot fail the statement.
// ---------------------------------------------------------------------------

export interface Operator {
  /** The next batch of at most `max` rows, never empty, or null when there are no more. */
  next(max: number): readonly Tuple[] | null;
  /** Starts over: for a subquery's next outer row, or a join's inner side. */
  rescan(): void;
}

/** The most rows an operator returns at once. */
export const BATCH_SIZE = 512;

/** How many more rows the batch being filled wants: what a walk pulls its children by. */
export interface Demand {
  readonly wanted: number;
}

/** Every row there is: what a sort, an aggregate or a hash join's build side pulls by. */
export const EVERY_ROW: Demand = { wanted: BATCH_SIZE };

/** One row at a time: what a join pulls its left side by, each row of which may give several. */
export const ONE_ROW: Demand = { wanted: 1 };

/** An operator's rows from the start, its children pulled as `demand` wants. */
export type Walk = (demand: Demand) => Iterator<Tuple>;

/**
 * An operator whose rows come from a walk, started on the first pull and
 * again after each rescan, which also rescans the operator's children.
 */
export class WalkOperator implements Operator {
  #rows: Iterator<Tuple> | null = null;
  readonly #batch = new Batch();

  /** `beforeBatch` runs before each batch: a leaf's checkpoint. */
  constructor(
    private readonly walk: Walk,
    private readonly children: readonly Operator[],
    private readonly beforeBatch: (() => void) | null = null,
  ) {}

  next(max: number): readonly Tuple[] | null {
    this.beforeBatch?.();
    this.#rows ??= this.walk(this.#batch);
    return this.#batch.fill(this.#rows, Math.min(max, BATCH_SIZE));
  }

  rescan(): void {
    this.#rows = null;
    for (const child of this.children) child.rescan();
  }
}

/**
 * An operator over rows of its own — a scan's, VALUES' — which calls the
 * run's checkpoint once per batch, where the engine can end the statement.
 */
export function leafOperator(rows: () => Iterator<Tuple>, run: Run): Operator {
  return new WalkOperator(rows, [], () => {
    run.checkpoint();
  });
}

/** The operator's rows, pulled in batches of what `demand` wants. */
export function* pull(operator: Operator, demand: Demand): Generator<Tuple> {
  let batch = operator.next(demand.wanted);
  while (batch !== null) {
    yield* batch;
    batch = operator.next(demand.wanted);
  }
}

/** Every row the operator has left. */
export function drain(operator: Operator): Tuple[] {
  return [...pull(operator, EVERY_ROW)];
}

/** At most `count` of the operator's rows, evaluating no more than those. */
export function take(operator: Operator, count: number): Tuple[] {
  const rows: Tuple[] = [];
  if (count <= 0) return rows;
  const pulled = pull(operator, upTo(count, rows));
  for (const row of pulled) {
    rows.push(row);
    if (rows.length === count) break;
  }
  return rows;
}

/** A demand for what `rows` lack of `count`. */
function upTo(count: number, rows: readonly Tuple[]): Demand {
  return {
    get wanted() {
      return count - rows.length;
    },
  };
}

/** A batch being filled from a walk, whose demand is what the batch still wants. */
class Batch implements Demand {
  #rows: Tuple[] = [];
  #size = 0;

  get wanted(): number {
    return this.#size - this.#rows.length;
  }

  /** Up to `size` rows from `rows`, or null if it has none left. */
  fill(rows: Iterator<Tuple>, size: number): Tuple[] | null {
    this.#rows = [];
    this.#size = size;
    while (this.wanted > 0) {
      const next = rows.next();
      if (next.done === true) break;
      this.#rows.push(next.value);
    }
    return this.#rows.length > 0 ? this.#rows : null;
  }
}

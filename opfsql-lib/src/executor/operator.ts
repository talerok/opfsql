import type { Tuple } from "../expressions/index.js";

// ---------------------------------------------------------------------------
// Operators (docs/planning.md, Execution): a plan node compiled for one run,
// pulled in batches. Batches start at one row and double, so EXISTS, a
// scalar subquery or LIMIT 1 decodes about as many rows as it needs, while a
// full scan soon moves in batches of hundreds.
// ---------------------------------------------------------------------------

export interface Operator {
  /** The next batch of rows, never empty, or null when there are no more. */
  next(): readonly Tuple[] | null;
  /** Starts over: for a subquery's next outer row, or a join's inner side. */
  rescan(): void;
}

const FIRST_BATCH = 1;
const LARGEST_BATCH = 512;

/** A walk over an operator's rows, from the start. */
export type Walk = () => Iterator<Tuple>;

/**
 * An operator whose rows come from a walk: started on the first pull, and
 * again after each rescan, which also rescans the operator's children.
 */
export class WalkOperator implements Operator {
  #rows: Iterator<Tuple> | null = null;
  #batchSize = FIRST_BATCH;

  constructor(
    private readonly walk: Walk,
    private readonly children: readonly Operator[],
  ) {}

  next(): readonly Tuple[] | null {
    this.#rows ??= this.walk();
    const batch = takeBatch(this.#rows, this.#batchSize);
    this.#batchSize = Math.min(this.#batchSize * 2, LARGEST_BATCH);
    return batch;
  }

  rescan(): void {
    this.#rows = null;
    this.#batchSize = FIRST_BATCH;
    for (const child of this.children) child.rescan();
  }
}

/** Every row the operator has left. */
export function drain(operator: Operator): Tuple[] {
  const rows: Tuple[] = [];
  for (let batch = operator.next(); batch !== null; batch = operator.next()) {
    for (const row of batch) rows.push(row);
  }
  return rows;
}

/** At most `limit` of the operator's rows, pulling no batch past them. */
export function take(operator: Operator, limit: number): Tuple[] {
  const rows: Tuple[] = [];
  while (rows.length < limit) {
    const batch = operator.next();
    if (batch === null) return rows;
    const wanted = batch.slice(0, limit - rows.length);
    for (const row of wanted) rows.push(row);
  }
  return rows;
}

/** The operator's rows one at a time, pulled batch by batch. */
export function* rowsOf(operator: Operator): Generator<Tuple> {
  for (let batch = operator.next(); batch !== null; batch = operator.next()) {
    yield* batch;
  }
}

/**
 * The child's next batch with `transform` applied that still has rows, or
 * null when the child has none left: an operator never returns an empty
 * batch.
 */
export function nextNonEmpty(
  child: Operator,
  transform: (batch: readonly Tuple[]) => readonly Tuple[],
): readonly Tuple[] | null {
  for (let batch = child.next(); batch !== null; batch = child.next()) {
    const result = transform(batch);
    if (result.length > 0) return result;
  }
  return null;
}

/** Up to `size` rows from `rows`, or null if it has none left. */
function takeBatch(rows: Iterator<Tuple>, size: number): Tuple[] | null {
  const batch: Tuple[] = [];
  while (batch.length < size) {
    const next = rows.next();
    if (next.done === true) break;
    batch.push(next.value);
  }
  return batch.length > 0 ? batch : null;
}

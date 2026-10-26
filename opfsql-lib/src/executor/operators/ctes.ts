import { InternalError } from "../../errors.js";
import type { Run, Tuple } from "../../expressions/index.js";
import { firstSighting } from "../../values/index.js";
import { type Demand, type Operator, pull, WalkOperator } from "../operator.js";

// ---------------------------------------------------------------------------
// Common table expressions (docs/planning.md, Execution). A CTE's rows are
// computed once per run of its query level, and only as far as they are
// read: a CTE nothing reads never runs, and `LIMIT 10` over an endless
// recursive CTE stops. Its definition is pulled with its own level's outer
// rows on the stack, so a subquery may read it as lazily as its own query.
// A recursive CTE runs its step on the rows the run before added, its
// working set, until a step adds none.
// ---------------------------------------------------------------------------

/** What a source gives when asked for at most `count` rows: a batch, or null at the end. */
type Source = (count: number) => readonly Tuple[] | null;

/** The rows the `cte-scan`s of one CTE index read: a CTE's result, or a working set. */
export class CteRows {
  private current: Spool | null = null;

  constructor(readonly name: string) {}

  /** From now on, the rows `source` gives, as far as scans read them. */
  spool(source: Source): void {
    this.current = new Spool([], source);
  }

  /** From now on, `rows`. */
  hold(rows: readonly Tuple[]): void {
    this.current = new Spool(rows, null);
  }

  /** At most `count` rows from `position` on; none past the last. */
  rowsFrom(position: number, count: number): readonly Tuple[] {
    if (this.current === null) {
      throw new InternalError(`CTE ${this.name} is read before it is computed`);
    }
    return this.current.rowsFrom(position, count);
  }
}

/** Where a CTE belongs: the depth of its query level, and the run. */
export interface CteLevel {
  readonly level: number;
  readonly run: Run;
}

/** What a recursive CTE needs besides its anchor and step. */
export interface Recursion {
  /** The rows the run before added, which the step reads. */
  readonly workingSet: CteRows;
  /** UNION ALL keeps duplicates; UNION adds only rows not seen yet. */
  readonly isUnionAll: boolean;
  /** The run, whose checkpoint each step calls. */
  readonly run: Run;
}

/** The body's rows, its CTE's rows spooled from the definition as read. */
export function cte(
  rows: CteRows,
  definition: Operator,
  body: Operator,
  { level, run }: CteLevel,
): Operator {
  const source: Source = (count) =>
    run.outer.atLevel(level, () => definition.next(count));
  return new WalkOperator(
    (demand) => bodyRows(rows, source, body, demand),
    [definition, body],
  );
}

export function cteScan(rows: CteRows): Operator {
  return new WalkOperator((demand) => scanned(rows, demand), []);
}

/**
 * The anchor's rows, then each step's on the rows the run before added,
 * until a step adds none: each run's rows pulled as they are wanted.
 */
export function recursiveCte(
  anchor: Operator,
  step: Operator,
  recursion: Recursion,
): Operator {
  return new WalkOperator(
    (demand) => recursiveRows([anchor, step], recursion, demand),
    [anchor, step],
  );
}

function* bodyRows(
  rows: CteRows,
  source: Source,
  body: Operator,
  demand: Demand,
): Generator<Tuple> {
  rows.spool(source);
  yield* pull(body, demand);
}

function* scanned(rows: CteRows, demand: Demand): Generator<Tuple> {
  let position = 0;
  let batch = rows.rowsFrom(position, demand.wanted);
  while (batch.length > 0) {
    yield* batch;
    position += batch.length;
    batch = rows.rowsFrom(position, demand.wanted);
  }
}

function* recursiveRows(
  [anchor, step]: readonly [Operator, Operator],
  { workingSet, isUnionAll, run }: Recursion,
  demand: Demand,
): Generator<Tuple> {
  const isNew = isUnionAll ? () => true : firstSighting();
  let added = yield* newRows(pull(anchor, demand), isNew);
  while (added.length > 0) {
    run.checkpoint();
    workingSet.hold(added);
    step.rescan();
    added = yield* newRows(pull(step, demand), isNew);
  }
}

/** The rows `isNew` keeps, yielded as they come and returned together. */
function* newRows(
  rows: Iterable<Tuple>,
  isNew: (row: Tuple) => boolean,
): Generator<Tuple, Tuple[]> {
  const added: Tuple[] = [];
  for (const row of rows) {
    if (!isNew(row)) continue;
    added.push(row);
    yield row;
  }
  return added;
}

/** Rows read from a source as far as readers ask, and kept for every reader. */
class Spool {
  private readonly rows: Tuple[];
  private source: Source | null;

  constructor(rows: readonly Tuple[], source: Source | null) {
    this.rows = [...rows];
    this.source = source;
  }

  rowsFrom(position: number, count: number): readonly Tuple[] {
    this.fillTo(position + count);
    return this.rows.slice(position, position + count);
  }

  /** Pulls from the source until `count` rows are held or it has none left. */
  private fillTo(count: number): void {
    while (this.rows.length < count && this.source !== null) {
      const batch = this.source(count - this.rows.length);
      if (batch === null) this.source = null;
      else this.rows.push(...batch);
    }
  }
}

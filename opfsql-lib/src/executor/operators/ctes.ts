import { InternalError } from "../../errors.js";
import type { Run, Tuple } from "../../expressions/index.js";
import { firstSighting } from "../../values/index.js";
import { type Demand, type Operator, pull, WalkOperator } from "../operator.js";

// ---------------------------------------------------------------------------
// Common table expressions (docs/planning.md, Execution). A CTE's result is
// computed once per run of its query level and read by every `cte-scan` of
// its index, pulled from its definition only as far as the scans read — so
// `LIMIT 10` over an endless recursive CTE stops. A recursive CTE runs its
// step on the rows the last run added, its working set, until it adds none.
// ---------------------------------------------------------------------------

/** The rows a `cte-scan` reads: a CTE's result, or a recursive CTE's working set. */
export class CteTable {
  #spool: Spool | null = null;
  #isReadByNestedLevel = false;

  /** `level` is the depth of the query level the CTE belongs to. */
  constructor(
    readonly name: string,
    private readonly level: number,
  ) {}

  /** Notes a `cte-scan` compiled at `level`: a subquery's, if deeper. */
  noteScanAt(level: number): void {
    if (level > this.level) this.#isReadByNestedLevel = true;
  }

  /**
   * Holds `definition`'s rows, pulled as scans need them — or all at once
   * when a subquery reads them: pulled from inside the subquery, the
   * definition would run with the subquery's outer rows on the stack.
   */
  spoolFrom(definition: Operator): void {
    const spool = new Spool([], definition);
    if (this.#isReadByNestedLevel) spool.fill();
    this.#spool = spool;
  }

  /** Holds `rows`, all known. */
  hold(rows: readonly Tuple[]): void {
    this.#spool = new Spool(rows, null);
  }

  /** The row at `position`, or undefined past the last. */
  rowAt(position: number): Tuple | undefined {
    if (this.#spool === null) {
      throw new InternalError(`CTE ${this.name} is read before it is computed`);
    }
    return this.#spool.rowAt(position);
  }
}

/** The body's rows, its CTE's result spooled from the definition at its start. */
export function cte(
  table: CteTable,
  definition: Operator,
  body: Operator,
): Operator {
  let isStarted = false;
  return {
    next: (max) => {
      if (!isStarted) table.spoolFrom(definition);
      isStarted = true;
      return body.next(max);
    },
    rescan: () => {
      isStarted = false;
      definition.rescan();
      body.rescan();
    },
  };
}

export function cteScan(table: CteTable): Operator {
  return new WalkOperator(() => rowsOfTable(table), []);
}

/** What a recursive CTE needs besides its anchor and step. */
export interface Recursion {
  /** The rows the last run added, which the step reads. */
  readonly workingSet: CteTable;
  /** UNION ALL keeps duplicates; UNION adds only rows not seen yet. */
  readonly isUnionAll: boolean;
  /** The run, whose checkpoint each step calls. */
  readonly run: Run;
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

function* rowsOfTable(table: CteTable): Generator<Tuple> {
  for (let position = 0; ; position++) {
    const row = table.rowAt(position);
    if (row === undefined) return;
    yield row;
  }
}

/** Rows read once from a source and kept, for any number of readers. */
class Spool {
  readonly #rows: Tuple[];
  #source: Operator | null;

  constructor(rows: readonly Tuple[], source: Operator | null) {
    this.#rows = [...rows];
    this.#source = source;
  }

  rowAt(position: number): Tuple | undefined {
    this.fillTo(position + 1);
    return this.#rows[position];
  }

  fill(): void {
    this.fillTo(Infinity);
  }

  /** Pulls from the source until `count` rows are held or it has none left. */
  private fillTo(count: number): void {
    while (this.#rows.length < count && this.#source !== null) {
      const batch = this.#source.next(count - this.#rows.length);
      if (batch === null) this.#source = null;
      else this.#rows.push(...batch);
    }
  }
}

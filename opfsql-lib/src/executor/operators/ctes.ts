import { InternalError } from "../../errors.js";
import type { Tuple } from "../../expressions/index.js";
import { firstSighting } from "../../values/index.js";
import { drain, type Operator, WalkOperator } from "../operator.js";

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
    next: () => {
      if (!isStarted) table.spoolFrom(definition);
      isStarted = true;
      return body.next();
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

/** The anchor's rows, then each step's, until a step adds none. */
export function recursiveCte(
  anchor: Operator,
  step: Operator,
  workingSet: CteTable,
  isUnionAll: boolean,
): Operator {
  function* rows(): Generator<Tuple> {
    // UNION adds only rows not seen yet, in this run or an earlier one.
    const isAdded = isUnionAll ? () => true : firstSighting();
    let added = drain(anchor).filter((row) => isAdded(row));
    while (added.length > 0) {
      yield* added;
      workingSet.hold(added);
      step.rescan();
      added = drain(step).filter((row) => isAdded(row));
    }
  }
  return new WalkOperator(rows, [anchor, step]);
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
      const batch = this.#source.next();
      if (batch === null) this.#source = null;
      else this.#rows.push(...batch);
    }
  }
}

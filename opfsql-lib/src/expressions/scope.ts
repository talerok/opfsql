import { InternalError } from "../errors.js";
import type { ColumnBinding, physical } from "../sql/plan/index.js";
import { normalizeValue, type Value } from "../values/index.js";

// ---------------------------------------------------------------------------
// What compiling an expression knows, and what running it reads. Compiling
// resolves each column to a position in a layout; per row, a closure reads
// the row and, for a column of an enclosing query, the outer row stack, on
// which a correlated subquery pushes its outer row while it runs.
// ---------------------------------------------------------------------------

/** A row: its values in the order of its plan node's layout. */
export type Tuple = readonly Value[];

/** The bindings of a plan node's output columns, in order (`layoutOf`). */
export type Layout = readonly ColumnBinding[];

/** An expression compiled for one layout: the value for a row. */
export type CompiledExpression = (row: Tuple) => Value;

/** Where one query level is compiled: its run, and its enclosing rows. */
export interface QueryContext {
  readonly run: Run;
  /**
   * The layouts of the enclosing queries' rows, nearest first: a column of
   * depth d is found in `outerLayouts[d − 1]`.
   */
  readonly outerLayouts: readonly Layout[];
}

/** Where an expression is compiled: a query level and its input's layout. */
export interface Scope extends QueryContext {
  /** The layout of the rows the expression reads: its plan node's input. */
  readonly layout: Layout;
}

/** What one run of a statement gives every expression compiled for it. */
export interface Run {
  /** The run's parameters, normalized as values are when stored. */
  readonly parameters: readonly Value[];
  /** The executor's compiler for the subqueries in the run's expressions. */
  readonly subqueries: SubqueryCompiler;
  /** The rows of the enclosing queries while correlated subqueries run. */
  readonly outer: OuterRows;
  /**
   * Called once for each batch of rows the statement reads, and for each
   * step of a recursive CTE: where the engine can end a statement nothing
   * else can stop (docs/planning.md, Execution).
   */
  checkpoint(): void;
}

/** Turns a subquery's plan into something to run: the executor's part. */
export interface SubqueryCompiler {
  /**
   * Compiles `plan` once for the run; its columns of depth d read rows of
   * `context.outerLayouts[d − 1]`.
   */
  compile(plan: physical.Plan, context: QueryContext): CompiledSubquery;
}

/** A subquery's operators, compiled once and run as often as needed. */
export interface CompiledSubquery {
  /** Runs the subquery from the start; its first `limit` rows, or all. */
  rows(limit?: number): readonly Tuple[];
}

/**
 * The rows of the enclosing queries, nearest last. A correlated subquery
 * runs `within` its outer row, so its columns of depth 1 read that row,
 * those of depth 2 the row its own enclosing subquery runs within, and so
 * on.
 */
export class OuterRows {
  readonly #rows: Tuple[] = [];

  /** The current row `depth` query levels out. */
  at(depth: number): Tuple {
    const row = this.#rows[this.#rows.length - depth];
    if (row === undefined) {
      throw new InternalError(
        `no outer row ${String(depth)} levels out (${String(this.#rows.length)} deep)`,
      );
    }
    return row;
  }

  /**
   * What `action` returns with only the rows of the outermost `level` query
   * levels on the stack, those above set aside: a CTE of an enclosing level,
   * computed when a subquery first reads it, must see its own level's rows.
   */
  atLevel<T>(level: number, action: () => T): T {
    const above = this.#rows.splice(level);
    try {
      return action();
    } finally {
      this.#rows.push(...above);
    }
  }

  /** What `action` returns while `row` is the nearest outer row. */
  within<T>(row: Tuple, action: () => T): T {
    this.#rows.push(row);
    try {
      return action();
    } finally {
      this.#rows.pop();
    }
  }
}

/**
 * A run with `parameters`, the executor's subquery compiler and the engine's
 * checkpoint, which does nothing unless given. The parameters are normalized
 * here, the one place they are: NaN becomes NULL and -0 becomes 0, as in a
 * stored value.
 */
export function createRun(
  parameters: readonly Value[],
  subqueries: SubqueryCompiler,
  checkpoint: () => void = runToTheEnd,
): Run {
  return {
    parameters: parameters.map((parameter) => normalizeValue(parameter)),
    subqueries,
    outer: new OuterRows(),
    checkpoint,
  };
}

function runToTheEnd(): void {
  // Without a checkpoint, nothing ends a statement before it is done.
}

/** What compiling a node needs: its scope, and a compiler for its operands. */
export interface Compiler {
  readonly scope: Scope;
  readonly compile: (expression: physical.Expression) => CompiledExpression;
}

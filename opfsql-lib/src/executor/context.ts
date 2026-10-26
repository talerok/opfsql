import {
  compileExpression,
  type CompiledPredicate,
  compilePredicate,
  type Layout,
  type QueryContext,
  type Scope,
  type Tuple,
} from "../expressions/index.js";
import type { physical } from "../sql/plan/index.js";
import type { PageReader } from "../storage/index.js";
import type { Value } from "../values/index.js";
import type { CteTable } from "./operators/ctes.js";

/** Where a plan node is compiled. */
export interface Context {
  /** Its query level: the run, and the layouts of the enclosing queries' rows. */
  readonly query: QueryContext;
  /** The CTEs a `cte-scan` here may read, by index. */
  readonly ctes: ReadonlyMap<number, CteTable>;
  /** The transaction's pages. */
  readonly pages: PageReader;
}

/** The scope of expressions over rows of `layout`. */
export function scopeOf(context: Context, layout: Layout): Scope {
  return { ...context.query, layout };
}

/**
 * The scope of a value fixed for one run of the node, such as a seek value
 * or a LIMIT: no row of its own, though it may read enclosing queries' rows.
 */
export function fixedScope(context: Context): Scope {
  return scopeOf(context, []);
}

/** How deep in subqueries the node is: 0 in the statement's own query. */
export function levelOf(context: Context): number {
  return context.query.outerLayouts.length;
}

/** A value fixed for one run of its node, evaluated when the node starts. */
export function compileFixed(
  expression: physical.Expression,
  context: Context,
): () => Value {
  const value = compileExpression(expression, fixedScope(context));
  return () => value([]);
}

/** Conditions that a row passes when every one is TRUE. */
export function compileConditions(
  conditions: readonly physical.Expression[],
  scope: Scope,
): (row: Tuple) => boolean {
  const predicates = conditions.map((condition) =>
    compilePredicate(condition, scope),
  );
  return (row) => passes(predicates, row);
}

function passes(predicates: readonly CompiledPredicate[], row: Tuple): boolean {
  for (const predicate of predicates) {
    if (!predicate(row)) return false;
  }
  return true;
}

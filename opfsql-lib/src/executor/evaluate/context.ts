import type { LogicalOperator } from "../../sql/binder/types.js";
import type { Value } from "../../values/index.js";
import type { Resolver } from "../resolve.js";
import type { Tuple } from "../types.js";

/** The row a correlated subquery runs for, and where its columns are. */
export interface OuterRow {
  readonly tuple: Tuple;
  readonly resolver: Resolver;
}

/** What compiled expressions need from the statement that runs them. */
export interface ExecutionContext {
  /** The statement's parameters, normalized; their count is checked. */
  readonly parameters: readonly Value[];
  /** The outer row, while running a correlated subquery. */
  readonly outer: OuterRow | null;
  /** Runs a subquery's plan for an outer row, stopping after `limit` rows. */
  readonly runSubquery: (
    plan: LogicalOperator,
    outer: OuterRow,
    limit?: number,
  ) => Tuple[];
}

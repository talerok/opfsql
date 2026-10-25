import { InternalError } from "../../errors.js";
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

/**
 * The context of an expression that reads only its row — a constant being
 * folded, an index key: no parameters, no outer row, and running a
 * subquery is a bug, never an empty result.
 */
export const ROW_ONLY_CONTEXT: ExecutionContext = {
  parameters: [],
  outer: null,
  runSubquery: () => {
    throw new InternalError("an expression of one row ran a subquery");
  },
};

import type { Schema, Table } from "../../schema/index.js";
import type { logical, physical } from "../plan/index.js";

// ---------------------------------------------------------------------------
// What every pass that plans may consult, besides the plan it is given.
// ---------------------------------------------------------------------------

/**
 * Binds an index's expression key — SQL text in the schema — as an
 * expression over the columns of `table` read as relation `tableIndex`, so
 * a condition or ORDER BY key written the same way matches it. The optimizer
 * cannot parse (docs/architecture.md, Modules): the engine passes the
 * binder's. Without one, expression keys match nothing.
 */
export type KeyBinder = (
  sql: string,
  table: Table,
  tableIndex: number,
) => logical.Expression;

export interface Planner {
  readonly schema: Schema;
  readonly bindKey: KeyBinder | null;
  /** A subquery's plan, through every pass. */
  readonly planSubquery: (plan: logical.Plan) => physical.Plan;
}

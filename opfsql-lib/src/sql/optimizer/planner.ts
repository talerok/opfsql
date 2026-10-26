import type { logical, physical } from "../plan/index.js";
import type { IndexCatalog } from "./key-parts.js";

// ---------------------------------------------------------------------------
// What every pass that plans may consult, besides the plan it is given.
// ---------------------------------------------------------------------------

export interface Planner {
  /** The indexes of each table read, with their key parts. */
  readonly indexes: IndexCatalog;
  /** A subquery's plan, through every pass. */
  readonly planSubquery: (plan: logical.Plan) => physical.Plan;
}

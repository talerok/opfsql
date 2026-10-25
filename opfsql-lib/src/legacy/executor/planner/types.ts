import type { LogicalOperator } from "../../binder/types.js";
import type {
  SyncIIndexManager,
  SyncIRowManager,
} from "../../../store/types.js";
import type { ExecutionContext } from "../evaluate/index.js";
import type { CTECacheEntry, SyncPhysicalOperator } from "../types.js";

export interface PlannerContext {
  rowManager: SyncIRowManager;
  cteCache: Map<number, CTECacheEntry>;
  context: ExecutionContext;
  indexManager?: SyncIIndexManager;
  plan: (child: LogicalOperator) => SyncPhysicalOperator;
}

export type PlanHandler = (
  node: LogicalOperator,
  pc: PlannerContext,
) => SyncPhysicalOperator;

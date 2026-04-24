import type { LogicalOperator } from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import type {
  ICatalog,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import {
  executeAlterTable,
  executeCreateIndex,
  executeCreateTable,
  executeDrop,
} from "../ddl/index.js";
import { executeDelete, executeInsert, executeUpdate } from "../dml/index.js";
import type { ExecuteResult, Value } from "../types.js";
import { createEvalContext, executeSelect } from "./select.js";

import type { SyncEvalContext } from "../evaluate/context.js";

type ExecuteHandler = (
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  catalog: ICatalog,
  ctx: SyncEvalContext,
  indexManager?: SyncIIndexManager,
) => ExecuteResult;

// Type safety is guaranteed by Map keys matching plan.type at runtime.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const handlers = new Map<string, ExecuteHandler>([
  [
    LogicalOperatorType.LOGICAL_CREATE_TABLE,
    (p, rm, cat, _, im) => executeCreateTable(p as any, cat, rm, im),
  ],
  [
    LogicalOperatorType.LOGICAL_CREATE_INDEX,
    (p, rm, cat, _, im) => executeCreateIndex(p as any, cat, rm, im!),
  ],
  [
    LogicalOperatorType.LOGICAL_ALTER_TABLE,
    (p, _rm, cat) => executeAlterTable(p as any, cat),
  ],
  [
    LogicalOperatorType.LOGICAL_DROP,
    (p, rm, cat, _, im) => executeDrop(p as any, cat, rm, im!),
  ],
  [
    LogicalOperatorType.LOGICAL_INSERT,
    (p, rm, _cat, ctx, im) => executeInsert(p as any, rm, ctx, _cat, im),
  ],
  [
    LogicalOperatorType.LOGICAL_UPDATE,
    (p, rm, _cat, ctx, im) => executeUpdate(p as any, rm, ctx, _cat, im),
  ],
  [
    LogicalOperatorType.LOGICAL_DELETE,
    (p, rm, _cat, ctx, im) => executeDelete(p as any, rm, ctx, _cat, im),
  ],
]);

export function execute(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  catalog: ICatalog,
  indexManager?: SyncIIndexManager,
  params?: readonly Value[],
): ExecuteResult {
  const handler = handlers.get(plan.type);
  const ctx = createEvalContext(rowManager, catalog, params);

  if (handler) {
    return handler(plan, rowManager, catalog, ctx, indexManager);
  }

  return executeSelect(plan, rowManager, ctx, indexManager);
}

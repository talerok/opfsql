import type { LogicalOperator } from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import type {
  ICatalog,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../../store/types.js";
import {
  executeAlterTable,
  executeCreateIndex,
  executeCreateTable,
  executeDrop,
} from "../ddl/index.js";
import { executeDelete, executeInsert, executeUpdate } from "../dml/index.js";
import type { ExecutionContext } from "../evaluate/index.js";
import { normalizeValue } from "../../../values/index.js";
import type { ExecuteResult, Value } from "../types.js";
import { createExecutionContext, executeSelect } from "./select.js";

type ExecuteHandler = (
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  catalog: ICatalog,
  context: ExecutionContext,
  indexManager?: SyncIIndexManager,
) => ExecuteResult;

// Type safety is guaranteed by Map keys matching plan.type at runtime.
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
    (p, rm, cat) => executeAlterTable(p as any, cat, rm),
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
    (p, rm, _cat, ctx, im) =>
      executeUpdate(p as any, {
        rowManager: rm,
        context: ctx,
        catalog: _cat,
        indexManager: im,
      }),
  ],
  [
    LogicalOperatorType.LOGICAL_DELETE,
    (p, rm, _cat, ctx, im) =>
      executeDelete(p as any, {
        rowManager: rm,
        context: ctx,
        catalog: _cat,
        indexManager: im,
      }),
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
  // The engine checked their count; values from outside are normalized
  // here (NaN is NULL, -0 is 0), as every value coming in is.
  const parameters = (params ?? []).map((value) => normalizeValue(value));
  const context = createExecutionContext(rowManager, parameters);

  if (handler) {
    return handler(plan, rowManager, catalog, context, indexManager);
  }

  return executeSelect(plan, rowManager, context, indexManager);
}

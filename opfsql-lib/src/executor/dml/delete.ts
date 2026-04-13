import type { LogicalDelete } from "../../binder/types.js";
import type {
  ICatalog,
  Row,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import type { ExecuteResult } from "../types.js";
import { maintainIndexesDelete } from "./utils/index-maintenance.js";
import { extractDmlScan, passesFilter, rowToTuple } from "./utils/scan.js";

export function executeDelete(
  op: LogicalDelete,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const scan = extractDmlScan(op.children[0]);

  const targets: Array<{ rowId: number; row: Row }> = [];
  for (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const tuple = rowToTuple(row, scan.get);
    if (passesFilter(tuple, scan, ctx)) targets.push({ rowId, row });
  }

  for (const { rowId, row } of targets) {
    maintainIndexesDelete(op.tableName, row, rowId, catalog, indexManager);
    rowManager.prepareDelete(op.tableName, rowId);
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}

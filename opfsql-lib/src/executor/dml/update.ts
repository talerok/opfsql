import type { LogicalUpdate } from "../../binder/types.js";
import type {
  ICatalog,
  Row,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { evaluateExpression } from "../evaluate/index.js";
import type { ExecuteResult } from "../types.js";
import { coerceJsonIfNeeded } from "./utils/coerce.js";
import {
  maintainIndexesDelete,
  maintainIndexesInsert,
} from "./utils/index-maintenance.js";
import { extractDmlScan, passesFilter, rowToTuple } from "./utils/scan.js";

export function executeUpdate(
  op: LogicalUpdate,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const scan = extractDmlScan(op.children[0]);

  const targets: Array<{
    rowId: number;
    oldRow: Row;
    newRow: Row;
  }> = [];
  for (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const tuple = rowToTuple(row, scan.get);
    if (!passesFilter(tuple, scan, ctx)) continue;

    const newRow = { ...row };
    for (let i = 0; i < op.updateColumns.length; i++) {
      const colIdx = op.updateColumns[i];
      const val = evaluateExpression(
        op.expressions[i],
        tuple,
        scan.resolver,
        ctx,
      );
      newRow[op.schema.columns[colIdx].name] = coerceJsonIfNeeded(
        val,
        op.schema,
        colIdx,
      );
    }
    targets.push({ rowId, oldRow: row, newRow });
  }

  for (const { rowId, oldRow, newRow } of targets) {
    maintainIndexesDelete(op.tableName, oldRow, rowId, catalog, indexManager);
    const newRowId = rowManager.prepareUpdate(op.tableName, rowId, newRow);
    maintainIndexesInsert(
      op.tableName,
      newRow,
      newRowId,
      catalog,
      indexManager,
    );
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}

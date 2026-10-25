import type { LogicalDelete } from "../../sql/binder/types.js";
import type { Row } from "../../store/types.js";
import type { ExecuteResult } from "../types.js";
import { maintainIndexesDelete } from "./utils/index-maintenance.js";
import {
  type DmlTarget,
  extractDmlScan,
  passesFilter,
  rowToTuple,
} from "./utils/scan.js";

export function executeDelete(
  operator: LogicalDelete,
  { rowManager, context, catalog, indexManager }: DmlTarget,
): ExecuteResult {
  const scan = extractDmlScan(operator.children[0], context);

  const targets: { rowId: number; row: Row }[] = [];
  for (const { rowId, row } of rowManager.scanTable(operator.tableName)) {
    if (passesFilter(rowToTuple(row, scan.get), scan)) {
      targets.push({ rowId, row });
    }
  }

  for (const { rowId, row } of targets) {
    maintainIndexesDelete(
      operator.tableName,
      row,
      rowId,
      catalog,
      indexManager,
    );
    rowManager.prepareDelete(operator.tableName, rowId);
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}

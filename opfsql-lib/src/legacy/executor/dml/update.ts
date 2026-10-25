import type { LogicalUpdate } from "../../binder/types.js";
import type { Row } from "../../../store/types.js";
import { compileExpression } from "../evaluate/index.js";
import type { ExecuteResult } from "../types.js";
import { assignToColumn } from "./utils/coerce.js";
import {
  maintainIndexesDelete,
  maintainIndexesInsert,
} from "./utils/index-maintenance.js";
import { passesFilters, rowToTuple } from "../operators/shared.js";
import { type DmlTarget, extractDmlScan } from "./utils/scan.js";

export function executeUpdate(
  operator: LogicalUpdate,
  { rowManager, context, catalog, indexManager }: DmlTarget,
): ExecuteResult {
  const { schema, tableName } = operator;
  const scan = extractDmlScan(operator.children[0], context);
  const assignments = operator.expressions.map((expression, index) => {
    const columnIndex = operator.updateColumns[index];
    return {
      columnIndex,
      name: schema.columns[columnIndex].name,
      value: compileExpression(expression, scan.resolver, context),
    };
  });

  const targets: { rowId: number; oldRow: Row; newRow: Row }[] = [];
  for (const { rowId, row } of rowManager.scanTable(tableName)) {
    const tuple = rowToTuple(row, scan.columnIds, scan.table.schema);
    if (!passesFilters(tuple, scan.filters)) continue;
    const newRow = { ...row };
    for (const { columnIndex, name, value } of assignments) {
      newRow[name] = assignToColumn(value(tuple), schema, columnIndex);
    }
    targets.push({ rowId, oldRow: row, newRow });
  }

  for (const { rowId, oldRow, newRow } of targets) {
    maintainIndexesDelete(tableName, oldRow, rowId, catalog, indexManager);
    const newRowId = rowManager.prepareUpdate(tableName, rowId, newRow);
    maintainIndexesInsert(tableName, newRow, newRowId, catalog, indexManager);
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}

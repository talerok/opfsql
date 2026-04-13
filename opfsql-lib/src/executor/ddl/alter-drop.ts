import type {
  LogicalAlterTable,
  LogicalDrop,
} from "../../binder/types.js";
import type {
  ICatalog,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import { ExecutorError } from "../errors.js";
import type { ExecuteResult } from "../types.js";

const EMPTY: ExecuteResult = { rows: [], rowsAffected: 0, catalogChanges: [] };

export function executeAlterTable(
  op: LogicalAlterTable,
  catalog: ICatalog,
): ExecuteResult {
  const before = catalog.getTable(op.tableName);
  if (!before) throw new ExecutorError(`Table "${op.tableName}" not found`);

  let columns;
  if (op.action.type === "ADD_COLUMN") {
    columns = [...before.columns, op.action.column];
  } else {
    const columnName = op.action.columnName.toLowerCase();
    columns = before.columns.filter((c) => c.name.toLowerCase() !== columnName);
  }
  const after = { ...before, columns };

  return {
    ...EMPTY,
    catalogChanges: [
      { type: "ALTER_TABLE", name: op.tableName, before, after },
    ],
  };
}

export function executeDrop(
  op: LogicalDrop,
  catalog: ICatalog,
  rowManager: SyncIRowManager,
  indexManager: SyncIIndexManager,
): ExecuteResult {
  if (op.dropType === "TABLE") {
    const schema = catalog.getTable(op.name);
    if (!schema) {
      if (op.ifExists) return { ...EMPTY };
      throw new ExecutorError(`Table "${op.name}" not found`);
    }
    rowManager.deleteTableData(op.name);
    for (const idx of catalog.getTableIndexes(op.name)) {
      indexManager.dropIndex(idx.name);
    }
    return {
      ...EMPTY,
      catalogChanges: [{ type: "DROP_TABLE", name: op.name, schema }],
    };
  }

  const index = catalog.getIndex(op.name);
  if (!index) {
    if (op.ifExists) return { ...EMPTY };
    throw new ExecutorError(`Index "${op.name}" not found`);
  }
  indexManager.dropIndex(op.name);
  return {
    ...EMPTY,
    catalogChanges: [{ type: "DROP_INDEX", name: op.name, index }],
  };
}

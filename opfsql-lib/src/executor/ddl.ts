import type {
  LogicalAlterTable,
  LogicalCreateIndex,
  LogicalCreateTable,
  LogicalDrop,
} from "../binder/types.js";
import { compareIndexKeys } from "../store/index-btree/compare.js";
import type { IndexKey } from "../store/index-btree/types.js";
import type {
  SyncIIndexManager,
  SyncIRowManager,
} from "../store/types.js";
import type { ICatalog } from "../store/types.js";
import { ExecutorError } from "./errors.js";
import type { ExecuteResult } from "./types.js";

const EMPTY: ExecuteResult = { rows: [], rowsAffected: 0, catalogChanges: [] };

export function executeCreateTable(
  op: LogicalCreateTable,
  catalog: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  if (catalog.hasTable(op.schema.name)) {
    if (op.ifNotExists) return { ...EMPTY };
    throw new ExecutorError(`Table "${op.schema.name}" already exists`);
  }

  const changes: ExecuteResult["catalogChanges"] = [
    { type: "CREATE_TABLE", schema: op.schema },
  ];

  const pkColumns = op.schema.columns.filter((c) => c.primaryKey);
  if (pkColumns.length > 0 && indexManager) {
    const indexDef = {
      name: `__pk_${op.schema.name}`,
      tableName: op.schema.name,
      columns: pkColumns.map((c) => c.name),
      unique: true,
    };
    indexManager.bulkLoad(indexDef.name, [], true);
    changes.push({ type: "CREATE_INDEX", index: indexDef });
  }

  return { ...EMPTY, catalogChanges: changes };
}

export function executeCreateIndex(
  op: LogicalCreateIndex,
  catalog: ICatalog,
  rowManager: SyncIRowManager,
  indexManager: SyncIIndexManager,
): ExecuteResult {
  if (catalog.hasIndex(op.index.name)) {
    if (op.ifNotExists) return { ...EMPTY };
    throw new ExecutorError(`Index "${op.index.name}" already exists`);
  }

  const entries: Array<{ key: IndexKey; rowId: number }> = [];
  for (const { rowId, row } of rowManager.scanTable(op.index.tableName)) {
    const key: IndexKey = op.index.columns.map(
      (col) => (row[col] ?? null) as IndexKey[number],
    );
    entries.push({ key, rowId });
  }

  entries.sort((a, b) => compareIndexKeys(a.key, b.key));
  indexManager.bulkLoad(op.index.name, entries, op.index.unique);

  return {
    ...EMPTY,
    catalogChanges: [{ type: "CREATE_INDEX", index: op.index }],
  };
}

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

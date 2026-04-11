import type {
  LogicalAlterTable,
  LogicalCreateIndex,
  LogicalCreateTable,
  LogicalDrop,
} from "../binder/types.js";
import { compareIndexKeys } from "../store/btree/compare.js";
import type { IndexKey } from "../store/btree/types.js";
import type { IIndexManager } from "../store/index-manager.js";
import type { ICatalog, IPageManager, IRowManager } from "../store/types.js";
import { ExecutorError } from "./errors.js";
import type { ExecuteResult } from "./types.js";

const EMPTY: ExecuteResult = {
  rows: [],
  rowsAffected: 0,
  catalogChanges: [],
};

export async function executeCreateTable(
  op: LogicalCreateTable,
  catalog: ICatalog,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  if (catalog.hasTable(op.schema.name)) {
    if (op.ifNotExists) return { ...EMPTY };
    throw new ExecutorError(`Table "${op.schema.name}" already exists`);
  }

  const changes: ExecuteResult["catalogChanges"] = [
    { type: "CREATE_TABLE", schema: op.schema },
  ];

  // Auto-create unique index for PRIMARY KEY columns
  const pkColumns = op.schema.columns.filter((c) => c.primaryKey);
  if (pkColumns.length > 0 && indexManager) {
    const indexDef = {
      name: `__pk_${op.schema.name}`,
      tableName: op.schema.name,
      columns: pkColumns.map((c) => c.name),
      unique: true,
    };
    await indexManager.bulkLoad(indexDef.name, [], true);
    changes.push({ type: "CREATE_INDEX", index: indexDef });
  }

  return { ...EMPTY, catalogChanges: changes };
}

export async function executeCreateIndex(
  op: LogicalCreateIndex,
  catalog: ICatalog,
  rowManager: IRowManager,
  indexManager: IIndexManager,
): Promise<ExecuteResult> {
  if (catalog.hasIndex(op.index.name)) {
    if (op.ifNotExists) return { ...EMPTY };
    throw new ExecutorError(`Index "${op.index.name}" already exists`);
  }

  // Backfill: scan the table and build the index
  const entries: Array<{
    key: IndexKey;
    rowId: { pageId: number; slotId: number };
  }> = [];

  for await (const { rowId, row } of rowManager.scanTable(op.index.tableName)) {
    const key: IndexKey = op.index.columns.map(
      (col) => (row[col] ?? null) as IndexKey[number],
    );
    entries.push({ key, rowId });
  }

  // Sort entries by key for bulk load
  entries.sort((a, b) => compareIndexKeys(a.key, b.key));

  await indexManager.bulkLoad(op.index.name, entries, op.index.unique);

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
  if (!before) {
    throw new ExecutorError(`Table "${op.tableName}" not found`);
  }

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

export async function executeDrop(
  op: LogicalDrop,
  catalog: ICatalog,
  pageManager: IPageManager,
  indexManager: IIndexManager,
): Promise<ExecuteResult> {
  if (op.dropType === "TABLE") {
    return executeDropTable(op, catalog, pageManager, indexManager);
  }
  return executeDropIndex(op, catalog, indexManager);
}

async function executeDropTable(
  op: LogicalDrop,
  catalog: ICatalog,
  pageManager: IPageManager,
  indexManager: IIndexManager,
): Promise<ExecuteResult> {
  const schema = catalog.getTable(op.name);
  if (!schema) {
    if (op.ifExists) return { ...EMPTY };
    throw new ExecutorError(`Table "${op.name}" not found`);
  }

  // Delete table data pages + meta
  await pageManager.deleteTableData(op.name);

  // Delete B-tree data for all table indexes
  const indexes = catalog.getTableIndexes(op.name);
  for (const idx of indexes) {
    await indexManager.dropIndex(idx.name);
  }

  return {
    ...EMPTY,
    catalogChanges: [{ type: "DROP_TABLE", name: op.name, schema }],
  };
}

async function executeDropIndex(
  op: LogicalDrop,
  catalog: ICatalog,
  indexManager: IIndexManager,
): Promise<ExecuteResult> {
  const index = catalog.getIndex(op.name);
  if (!index) {
    if (op.ifExists) return { ...EMPTY };
    throw new ExecutorError(`Index "${op.name}" not found`);
  }

  // Delete B-tree storage
  await indexManager.dropIndex(op.name);

  return {
    ...EMPTY,
    catalogChanges: [{ type: "DROP_INDEX", name: op.name, index }],
  };
}

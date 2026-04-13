import type {
  LogicalCreateIndex,
  LogicalCreateTable,
} from "../../binder/types.js";
import { compareIndexKeys } from "../../store/index-btree/compare.js";
import type { IndexKey } from "../../store/index-btree/types.js";
import type {
  ICatalog,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import { ExecutorError } from "../errors.js";
import type { ExecuteResult } from "../types.js";

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

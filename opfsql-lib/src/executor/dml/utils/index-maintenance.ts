import type { IndexKey } from "../../../store/index-btree/types.js";
import type {
  ICatalog,
  Row,
  RowId,
  SyncIIndexManager,
} from "../../../store/types.js";

export function buildIndexKey(row: Row, columns: string[]): IndexKey {
  return columns.map((col) => (row[col] ?? null) as IndexKey[number]);
}

export function maintainIndexesInsert(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): void {
  if (!catalog || !indexManager) return;
  for (const idx of catalog.getTableIndexes(tableName)) {
    indexManager.insert(idx.name, buildIndexKey(row, idx.columns), rowId);
  }
}

export function maintainIndexesDelete(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): void {
  if (!catalog || !indexManager) return;
  for (const idx of catalog.getTableIndexes(tableName)) {
    indexManager.delete(idx.name, buildIndexKey(row, idx.columns), rowId);
  }
}

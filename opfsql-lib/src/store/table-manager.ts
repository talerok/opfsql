import type { SyncIKVStore, SyncIRowManager, Row, RowId } from './types.js';
import { SyncTableBTree } from './table-btree.js';

export class SyncTableManager implements SyncIRowManager {
  constructor(private readonly kv: SyncIKVStore) {}

  private tree(tableName: string): SyncTableBTree {
    return new SyncTableBTree(tableName, this.kv);
  }

  prepareInsert(tableId: string, row: Row): RowId { return this.tree(tableId).insert(row); }
  prepareUpdate(tableId: string, rowId: RowId, row: Row): RowId { this.tree(tableId).update(rowId, row); return rowId; }
  prepareDelete(tableId: string, rowId: RowId): void { this.tree(tableId).delete(rowId); }
  *scanTable(tableId: string): Generator<{ rowId: RowId; row: Row }> { yield* this.tree(tableId).scan(); }
  readRow(tableId: string, rowId: RowId): Row | null { return this.tree(tableId).get(rowId); }
  deleteTableData(tableId: string): void { this.tree(tableId).drop(); }
}

import type { SyncIPageStore, SyncIRowManager, ICatalog, Row, RowId } from './types.js';
import { SyncTableBTree } from './table-btree.js';
import type { TableBTreeMeta, TableLeafNode } from './table-btree.js';

export class SyncTableManager implements SyncIRowManager {
  constructor(
    private readonly ps: SyncIPageStore,
    private readonly getCatalog: () => ICatalog,
  ) {}

  private tree(tableName: string): SyncTableBTree {
    const schema = this.getCatalog().getTable(tableName);
    if (!schema) throw new Error(`Table "${tableName}" not found in catalog`);
    return new SyncTableBTree(schema.metaPageNo!, this.ps);
  }

  createTable(): number {
    const metaPageNo = this.ps.allocPage();
    const rootPageNo = this.ps.allocPage();
    const leaf: TableLeafNode = { kind: 'leaf', nodeId: rootPageNo, keys: [], values: [], nextLeafId: null };
    const meta: TableBTreeMeta = { rootNodeId: rootPageNo, height: 1, nextRowId: 0, size: 0 };
    this.ps.writePage(rootPageNo, leaf);
    this.ps.writePage(metaPageNo, meta);
    return metaPageNo;
  }

  prepareInsert(tableId: string, row: Row): RowId { return this.tree(tableId).insert(row); }
  prepareUpdate(tableId: string, rowId: RowId, row: Row): RowId { this.tree(tableId).update(rowId, row); return rowId; }
  prepareDelete(tableId: string, rowId: RowId): void { this.tree(tableId).delete(rowId); }
  *scanTable(tableId: string): Generator<{ rowId: RowId; row: Row }> { yield* this.tree(tableId).scan(); }
  readRow(tableId: string, rowId: RowId): Row | null { return this.tree(tableId).get(rowId); }
  deleteTableData(tableId: string): void { this.tree(tableId).drop(); }
}

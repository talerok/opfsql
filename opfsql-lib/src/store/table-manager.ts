import type { IKVStore, IRowManager, Row, RowId } from './types.js';
import { TableBTree } from './table-btree.js';

export class TableManager implements IRowManager {
  constructor(private readonly kv: IKVStore) {}

  private getTree(tableName: string): TableBTree {
    return new TableBTree(tableName, this.kv);
  }

  async prepareInsert(tableId: string, row: Row): Promise<RowId> {
    return this.getTree(tableId).insert(row);
  }

  async prepareUpdate(tableId: string, rowId: RowId, row: Row): Promise<RowId> {
    await this.getTree(tableId).update(rowId, row);
    return rowId;
  }

  async prepareDelete(tableId: string, rowId: RowId): Promise<void> {
    await this.getTree(tableId).delete(rowId);
  }

  async *scanTable(tableId: string): AsyncGenerator<{ rowId: RowId; row: Row }> {
    yield* this.getTree(tableId).scan();
  }

  async readRow(tableId: string, rowId: RowId): Promise<Row | null> {
    return this.getTree(tableId).get(rowId);
  }

  async deleteTableData(tableId: string): Promise<void> {
    await this.getTree(tableId).drop();
  }
}

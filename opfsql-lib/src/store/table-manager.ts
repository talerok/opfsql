import { decodeRow, encodeRow } from "./row-codec.js";
import type { TableBTreeMeta, TableLeafNode } from "./table-btree.js";
import { SyncTableBTree } from "./table-btree.js";
import type {
  ICatalog,
  Row,
  RowId,
  SyncIPageStore,
  SyncIRowManager,
  TableSchema,
} from "./types.js";

export class SyncTableManager implements SyncIRowManager {
  constructor(
    private readonly ps: SyncIPageStore,
    private readonly getCatalog: () => ICatalog,
  ) {}

  private schema(tableName: string): TableSchema {
    const schema = this.getCatalog().getTable(tableName);
    if (!schema) throw new Error(`Table "${tableName}" not found in catalog`);
    return schema;
  }

  private tree(schema: TableSchema): SyncTableBTree {
    return new SyncTableBTree(schema.metaPageNo!, this.ps);
  }

  createTable(): number {
    const metaPageNo = this.ps.allocPage();
    const rootPageNo = this.ps.allocPage();
    const leaf: TableLeafNode = {
      kind: "leaf",
      nodeId: rootPageNo,
      keys: [],
      cells: [],
      nextLeafId: null,
    };
    const meta: TableBTreeMeta = {
      rootNodeId: rootPageNo,
      height: 1,
      nextRowId: 0,
      size: 0,
      lastAutoIncrement: 0,
    };
    this.ps.writePage(rootPageNo, leaf);
    this.ps.writePage(metaPageNo, meta);
    return metaPageNo;
  }

  prepareInsert(tableId: string, row: Row): RowId {
    const schema = this.schema(tableId);
    return this.tree(schema).insert(encodeRow(schema, row));
  }
  prepareUpdate(tableId: string, rowId: RowId, row: Row): RowId {
    const schema = this.schema(tableId);
    this.tree(schema).update(rowId, encodeRow(schema, row));
    return rowId;
  }
  prepareDelete(tableId: string, rowId: RowId): void {
    this.tree(this.schema(tableId)).delete(rowId);
  }
  *scanTable(tableId: string): Generator<{ rowId: RowId; row: Row }> {
    const schema = this.schema(tableId);
    for (const { rowId, cell } of this.tree(schema).scan()) {
      yield { rowId, row: decodeRow(schema, cell) };
    }
  }
  readRow(tableId: string, rowId: RowId): Row | null {
    const schema = this.schema(tableId);
    const cell = this.tree(schema).get(rowId);
    return cell && decodeRow(schema, cell);
  }
  rewriteTable(tableId: string, from: TableSchema, to: TableSchema): void {
    const tree = this.tree(this.schema(tableId));
    for (const { rowId, cell } of [...tree.scan()]) {
      tree.update(rowId, encodeRow(to, decodeRow(from, cell)));
    }
  }
  nextAutoIncrement(tableId: string): number {
    return this.tree(this.schema(tableId)).nextAutoIncrement();
  }
  observeAutoIncrement(tableId: string, value: number): void {
    this.tree(this.schema(tableId)).observeAutoIncrement(value);
  }
  deleteTableData(tableId: string): void {
    this.tree(this.schema(tableId)).drop();
  }
}

import { BTree } from "./btree.js";
import { decodeRow, encodeRow, rowidColumn } from "./row-codec.js";
import { SyncTableBTree } from "./table-btree.js";
import type {
  ICatalog,
  Row,
  RowId,
  SyncIPageStore,
  SyncIRowManager,
  TableSchema,
  Value,
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
    return new SyncTableBTree(this.ps, schema.rootPageNo!, schema.name);
  }

  createTable(
    schema: TableSchema,
  ): Pick<TableSchema, "rootPageNo" | "sequencePageNo"> {
    const rootPageNo = BTree.create(this.ps);
    if (!schema.columns.some((c) => c.autoIncrement)) return { rootPageNo };

    const sequencePageNo = this.ps.allocPage();
    this.ps.writePage(sequencePageNo, 0);
    return { rootPageNo, sequencePageNo };
  }

  prepareInsert(tableId: string, row: Row): RowId {
    const schema = this.schema(tableId);
    const tree = this.tree(schema);
    const rowid = rowidColumn(schema);
    if (!rowid) {
      const rowId = tree.nextRowId();
      tree.insert(rowId, encodeRow(schema, row));
      return rowId;
    }

    const named = row[rowid.name] ?? null;
    const rowId = named === null ? tree.nextRowId() : toRowId(schema, named);
    if (!tree.insert(rowId, encodeRow(schema, row))) {
      throw uniqueViolation(schema);
    }
    row[rowid.name] = rowId;
    return rowId;
  }
  prepareUpdate(tableId: string, rowId: RowId, row: Row): RowId {
    const schema = this.schema(tableId);
    const tree = this.tree(schema);
    const rowid = rowidColumn(schema);
    const newRowId = rowid ? toRowId(schema, row[rowid.name] ?? null) : rowId;
    const cell = encodeRow(schema, row);
    if (newRowId === rowId) {
      tree.update(rowId, cell);
      return rowId;
    }

    // A new INTEGER PRIMARY KEY moves the row to its new key.
    if (!tree.insert(newRowId, cell)) throw uniqueViolation(schema);
    tree.delete(rowId);
    return newRowId;
  }
  prepareDelete(tableId: string, rowId: RowId): void {
    this.tree(this.schema(tableId)).delete(rowId);
  }
  *scanTable(tableId: string): Generator<{ rowId: RowId; row: Row }> {
    const schema = this.schema(tableId);
    for (const [rowId, cell] of this.tree(schema).scan()) {
      yield { rowId, row: decodeRow(schema, rowId, cell) };
    }
  }
  readRow(tableId: string, rowId: RowId): Row | null {
    const schema = this.schema(tableId);
    const cell = this.tree(schema).get(rowId);
    return cell && decodeRow(schema, rowId, cell);
  }
  rewriteTable(tableId: string, from: TableSchema, to: TableSchema): void {
    const tree = this.tree(this.schema(tableId));
    for (const [rowId, cell] of [...tree.scan()]) {
      tree.update(rowId, encodeRow(to, decodeRow(from, rowId, cell)));
    }
  }
  nextAutoIncrement(tableId: string): number {
    const pageNo = this.sequencePage(tableId);
    const value = this.ps.readPage<number>(pageNo)! + 1;
    this.ps.writePage(pageNo, value);
    return value;
  }
  observeAutoIncrement(tableId: string, value: number): void {
    const pageNo = this.sequencePage(tableId);
    if (value > this.ps.readPage<number>(pageNo)!) {
      this.ps.writePage(pageNo, value);
    }
  }
  deleteTableData(tableId: string): void {
    const schema = this.schema(tableId);
    this.tree(schema).drop();
    if (schema.sequencePageNo !== undefined) {
      this.ps.freePage(schema.sequencePageNo);
    }
  }

  private sequencePage(tableName: string): number {
    const { sequencePageNo } = this.schema(tableName);
    if (sequencePageNo === undefined) {
      throw new Error(`Table "${tableName}" has no AUTOINCREMENT sequence`);
    }
    return sequencePageNo;
  }
}

/** The rowid an INTEGER PRIMARY KEY value names. */
function toRowId(schema: TableSchema, value: Value): RowId {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  const column = rowidColumn(schema)!.name;
  throw new Error(
    `datatype mismatch: PRIMARY KEY "${column}" of table "${schema.name}" must be an integer, got ${value === null ? "NULL" : JSON.stringify(value)}`,
  );
}

function uniqueViolation(schema: TableSchema): Error {
  const column = rowidColumn(schema)!.name;
  return new Error(`UNIQUE constraint failed: ${schema.name}.${column}`);
}

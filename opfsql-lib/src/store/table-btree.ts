import { BTree, type BTreeLayout } from "./btree.js";
import { RowTooLargeError } from "./errors.js";
import type { RowId, SyncIPageStore } from "./types.js";

/** A row: its rowid and its encoded columns (row-codec.ts). */
export type TableEntry = [rowId: RowId, cell: Uint8Array];

function tableLayout(table: string): BTreeLayout<RowId, TableEntry> {
  return {
    keyOf: ([rowId]) => rowId,
    compare: (a, b) => a - b,
    tooLarge: (size, limit) => new RowTooLargeError(table, size, limit),
  };
}

/** The rows of a table, keyed by rowid. */
export class SyncTableBTree {
  private readonly tree: BTree<RowId, TableEntry>;

  constructor(pages: SyncIPageStore, rootPageNo: number, table: string) {
    this.tree = new BTree(pages, rootPageNo, tableLayout(table));
  }

  /** The rowid after the largest in use. */
  nextRowId(): RowId {
    const last = this.tree.last();
    const rowId = last ? last[0] + 1 : 1;
    if (!Number.isSafeInteger(rowId)) throw new Error("table is out of rowids");
    return rowId;
  }

  /** Stores a row under `rowId`. False, changing nothing, if it is taken. */
  insert(rowId: RowId, cell: Uint8Array): boolean {
    return this.tree.insert([rowId, cell]);
  }

  get(rowId: RowId): Uint8Array | null {
    return this.tree.get(rowId)?.[1] ?? null;
  }

  update(rowId: RowId, cell: Uint8Array): void {
    if (!this.tree.replace([rowId, cell])) {
      throw new Error(`Row ${rowId} not found`);
    }
  }

  delete(rowId: RowId): void {
    this.tree.delete(rowId);
  }

  /** Rows in rowid order, from the first at or after `from`. */
  scan(from?: RowId): Iterable<TableEntry> {
    return this.tree.ascending(from);
  }

  /** The row with the largest rowid, or null if the table is empty. */
  last(): TableEntry | null {
    return this.tree.last();
  }

  drop(): void {
    this.tree.drop();
  }
}

import { BTree } from "./btree.js";
import { SyncBTree } from "./index-btree/index-btree.js";
import { computeBounds } from "./index-btree/search-bounds.js";
import { RowidIndex } from "./rowid-index.js";
import { SyncTableBTree } from "./table-btree.js";
import type {
  ICatalog,
  IndexKey,
  RowId,
  SearchPredicate,
  SyncIIndexManager,
  SyncIPageStore,
} from "./types.js";

export class SyncIndexManager implements SyncIIndexManager {
  constructor(
    private readonly ps: SyncIPageStore,
    private readonly getCatalog: () => ICatalog,
  ) {}

  insert(indexName: string, key: IndexKey, rowId: RowId): void {
    this.tree(indexName).insert(key, rowId);
  }

  delete(indexName: string, key: IndexKey, rowId: RowId): void {
    this.tree(indexName).delete(key, rowId);
  }

  search(indexName: string, predicates: SearchPredicate[]): RowId[] {
    return this.tree(indexName).range(computeBounds(predicates));
  }

  bulkLoad(
    indexName: string,
    entries: { key: IndexKey; rowId: RowId }[],
    unique: boolean,
  ): number {
    const rootPageNo = BTree.create(this.ps);
    new SyncBTree(this.ps, rootPageNo, { name: indexName, unique }).bulkLoad(
      entries,
    );
    return rootPageNo;
  }

  dropIndex(indexName: string): void {
    this.tree(indexName).drop();
  }

  first(indexName: string) {
    return this.tree(indexName).first();
  }

  last(indexName: string) {
    return this.tree(indexName).last();
  }

  private tree(indexName: string): SyncBTree | RowidIndex {
    const catalog = this.getCatalog();
    const indexDef = catalog.getIndex(indexName.toLowerCase());
    if (!indexDef) throw new Error(`Index "${indexName}" not found in catalog`);
    if (!indexDef.rowid) {
      return new SyncBTree(this.ps, indexDef.rootPageNo!, indexDef);
    }
    const table = catalog.getTable(indexDef.tableName)!;
    return new RowidIndex(
      new SyncTableBTree(this.ps, table.rootPageNo!, table.name),
    );
  }
}

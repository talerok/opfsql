import { SyncBTree } from "./index-btree/index-btree.js";
import { computeBounds } from "./index-btree/search-bounds.js";
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

  private tree(indexName: string): SyncBTree {
    const indexDef = this.getCatalog().getIndex(indexName.toLowerCase());
    if (!indexDef) throw new Error(`Index "${indexName}" not found in catalog`);
    return new SyncBTree(indexDef.metaPageNo!, this.ps, indexDef.unique);
  }

  insert(indexName: string, key: IndexKey, rowId: RowId): void {
    this.tree(indexName).insert(key, rowId);
  }

  delete(indexName: string, key: IndexKey, rowId: RowId): void {
    this.tree(indexName).delete(key, rowId);
  }

  search(
    indexName: string,
    predicates: SearchPredicate[],
    totalColumns?: number,
  ): RowId[] {
    const tree = this.tree(indexName);
    const bounds = computeBounds(predicates, totalColumns);
    if (bounds.exactKey) return tree.lookup(bounds.exactKey);
    return tree.range({
      lower: bounds.lowerKey ?? undefined,
      upper: bounds.upperKey ?? undefined,
      lowerInclusive: bounds.lowerInclusive,
      upperInclusive: bounds.upperInclusive,
      prefixScan: bounds.prefixScan,
    });
  }

  bulkLoad(
    indexName: string,
    entries: Array<{ key: IndexKey; rowId: RowId }>,
    unique: boolean,
  ): number {
    const metaPageNo = this.ps.allocPage();
    const tree = new SyncBTree(metaPageNo, this.ps, unique);
    tree.bulkLoad(entries);
    return metaPageNo;
  }

  dropIndex(indexName: string): void {
    this.tree(indexName).drop();
  }
}

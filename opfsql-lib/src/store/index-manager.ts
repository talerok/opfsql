import { SyncBTree } from "./index-btree/index-btree.js";
import type {
  IndexKey,
  RowId,
  SearchPredicate,
  SyncIIndexManager,
  SyncIKVStore,
} from "./types.js";

export class SyncIndexManager implements SyncIIndexManager {
  private readonly trees = new Map<string, SyncBTree>();

  constructor(private readonly pm: SyncIKVStore) {}

  private tree(indexName: string): SyncBTree {
    const key = indexName.toLowerCase();
    const cached = this.trees.get(key);
    if (cached) return cached;
    const metaRaw = this.pm.readKey<{ unique?: boolean }>(`btree:${key}:meta`);
    const unique = metaRaw?.unique ?? false;
    const tree = new SyncBTree(key, this.pm, unique);
    this.trees.set(key, tree);
    return tree;
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
    return this.tree(indexName).search(predicates, totalColumns);
  }

  bulkLoad(
    indexName: string,
    entries: Array<{ key: IndexKey; rowId: RowId }>,
    unique: boolean,
  ): void {
    const key = indexName.toLowerCase();
    const tree = new SyncBTree(key, this.pm, unique);
    this.trees.set(key, tree);
    tree.bulkLoad(entries);
  }

  dropIndex(indexName: string): void {
    const key = indexName.toLowerCase();
    this.tree(indexName).drop();
    this.trees.delete(key);
  }
}

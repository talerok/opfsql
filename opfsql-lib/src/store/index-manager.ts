import type { IKVStore, RowId } from './types.js';
import type { IndexKey } from './index-btree/types.js';
import { BTree, type SearchPredicate } from './index-btree/index-btree.js';

// ---------------------------------------------------------------------------
// IIndexManager — public interface
// ---------------------------------------------------------------------------

export interface IIndexManager {
  /** Insert a key/rowId pair. Throws on unique constraint violation. */
  insert(indexName: string, key: IndexKey, rowId: RowId): Promise<void>;
  delete(indexName: string, key: IndexKey, rowId: RowId): Promise<void>;
  search(indexName: string, predicates: SearchPredicate[], totalColumns?: number): Promise<RowId[]>;
  bulkLoad(
    indexName: string,
    entries: Array<{ key: IndexKey; rowId: RowId }>,
    unique: boolean,
  ): Promise<void>;
  dropIndex(indexName: string): Promise<void>;
}

// ---------------------------------------------------------------------------
// IndexManager — delegates to BTree instances
// ---------------------------------------------------------------------------

export class IndexManager implements IIndexManager {
  private readonly trees = new Map<string, BTree>();

  constructor(private readonly pm: IKVStore) {}

  /**
   * Returns the cached BTree for the given index, creating it if necessary.
   * The unique flag is read from persisted BTreeMeta so callers don't need to supply it.
   */
  private async getTree(indexName: string): Promise<BTree> {
    const key = indexName.toLowerCase();
    const cached = this.trees.get(key);
    if (cached) return cached;

    // Read the unique flag from stored meta — avoids passing it on every operation.
    const metaRaw = await this.pm.readKey<{ unique?: boolean }>(`btree:${key}:meta`);
    const unique = metaRaw?.unique ?? false;

    const tree = new BTree(key, this.pm, unique);
    this.trees.set(key, tree);
    return tree;
  }

  async insert(indexName: string, key: IndexKey, rowId: RowId): Promise<void> {
    await (await this.getTree(indexName)).insert(key, rowId);
  }

  async delete(indexName: string, key: IndexKey, rowId: RowId): Promise<void> {
    await (await this.getTree(indexName)).delete(key, rowId);
  }

  async search(indexName: string, predicates: SearchPredicate[], totalColumns?: number): Promise<RowId[]> {
    return (await this.getTree(indexName)).search(predicates, totalColumns);
  }

  async bulkLoad(
    indexName: string,
    entries: Array<{ key: IndexKey; rowId: RowId }>,
    unique: boolean,
  ): Promise<void> {
    const key = indexName.toLowerCase();
    const tree = new BTree(key, this.pm, unique);
    this.trees.set(key, tree);
    await tree.bulkLoad(entries);
  }

  async dropIndex(indexName: string): Promise<void> {
    const key = indexName.toLowerCase();
    await (await this.getTree(indexName)).drop();
    this.trees.delete(key);
  }
}

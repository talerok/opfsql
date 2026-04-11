import type { IKVStore, RowId } from './types.js';
import type { IndexKey } from './btree/types.js';
import { BTree, type SearchPredicate } from './btree/btree.js';

// ---------------------------------------------------------------------------
// IIndexManager — public interface
// ---------------------------------------------------------------------------

export interface IIndexManager {
  /** Insert a key/rowId pair. Throws if unique=true and key already exists. */
  insert(indexName: string, key: IndexKey, rowId: RowId, unique: boolean): Promise<void>;
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
  constructor(private readonly pm: IKVStore) {}

  /** BTree instances are stateless (all state in PM), so we create on demand. */
  private getTree(indexName: string, unique: boolean): BTree {
    return new BTree(indexName.toLowerCase(), this.pm, unique);
  }

  async insert(indexName: string, key: IndexKey, rowId: RowId, unique: boolean): Promise<void> {
    await this.getTree(indexName, unique).insert(key, rowId);
  }

  async delete(indexName: string, key: IndexKey, rowId: RowId): Promise<void> {
    await this.getTree(indexName, false).delete(key, rowId);
  }

  async search(indexName: string, predicates: SearchPredicate[], totalColumns?: number): Promise<RowId[]> {
    return this.getTree(indexName, false).search(predicates, totalColumns);
  }

  async bulkLoad(
    indexName: string,
    entries: Array<{ key: IndexKey; rowId: RowId }>,
    unique: boolean,
  ): Promise<void> {
    await this.getTree(indexName, unique).bulkLoad(entries);
  }

  async dropIndex(indexName: string): Promise<void> {
    await this.getTree(indexName, false).drop();
  }
}

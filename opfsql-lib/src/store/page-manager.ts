import { StorageError, wrapStorageError } from './errors.js';
import type { IPageManager, IStorage, Page, PageMeta, Row, RowId } from './types.js';
import { PAGE_SIZE } from './types.js';

const PAGE_ID_WIDTH = 6;
const DEFAULT_META: PageMeta = { lastPageId: -1, nextRowId: 0, totalRowCount: 0, freePageIds: [] };

type RowMapShard = Record<number, number>; // logicalId → pageId
const ROW_MAP_SHARD_SIZE = 65536;

export class PageManager implements IPageManager {
  private wal = new Map<string, unknown>();
  private cache = new Map<string, unknown>();

  constructor(private readonly storage: IStorage) {}

  // ---------------------------------------------------------------------------
  // Row operations
  // ---------------------------------------------------------------------------

  async prepareInsert(tableId: string, row: Row): Promise<RowId> {
    try {
      const meta = await this.getPageMeta(tableId);

      const id = meta.nextRowId++;

      // Find a page with room
      let page: Page | null = null;
      if (meta.lastPageId >= 0) {
        page = await this.readPage(tableId, meta.lastPageId);
      }

      let writable: Page;
      if (!page || this.rowCount(page) >= PAGE_SIZE) {
        const freeId = meta.freePageIds.pop();
        const newPageId = freeId !== undefined ? freeId : ++meta.lastPageId;
        writable = { pageId: newPageId, tableId, rows: {} };
        this.writePage(tableId, writable);
      } else {
        writable = this.ensureWritable(tableId, page);
      }

      writable.rows[id] = row;
      const shard = await this.readRowMapShard(tableId, id);
      shard[id] = writable.pageId;
      meta.totalRowCount++;

      this.writeMeta(tableId, meta);

      return id;
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  async prepareUpdate(
    tableId: string,
    rowId: RowId,
    row: Row,
  ): Promise<RowId> {
    try {
      const shard = await this.readRowMapShard(tableId, rowId);
      const pageId = shard[rowId];
      if (pageId === undefined) {
        throw new StorageError(`Row ${rowId} not found in table ${tableId}`);
      }

      const page = await this.requirePage(tableId, pageId);
      const writable = this.ensureWritable(tableId, page);
      writable.rows[rowId] = row;

      return rowId;
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  async prepareDelete(tableId: string, rowId: RowId): Promise<void> {
    try {
      const meta = await this.getPageMeta(tableId);
      const shard = await this.readRowMapShard(tableId, rowId);
      const pageId = shard[rowId];
      if (pageId === undefined) {
        throw new StorageError(`Row ${rowId} not found in table ${tableId}`);
      }

      const page = await this.requirePage(tableId, pageId);
      const writable = this.ensureWritable(tableId, page);
      delete writable.rows[rowId];
      delete shard[rowId];
      meta.totalRowCount--;

      // Reclaim empty non-last pages
      if (this.rowCount(writable) === 0 && pageId !== meta.lastPageId) {
        meta.freePageIds.push(pageId);
      }

      this.writeMeta(tableId, meta);
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  async *scanTable(
    tableId: string,
  ): AsyncGenerator<{ rowId: RowId; row: Row }> {
    const meta = await this.getPageMeta(tableId);
    for (let pid = 0; pid <= meta.lastPageId; pid++) {
      const page = await this.readPage(tableId, pid);
      if (!page) continue;
      for (const [id, data] of Object.entries(page.rows)) {
        yield { rowId: Number(id), row: data };
      }
    }
  }

  async readRow(tableId: string, rowId: RowId): Promise<Row | null> {
    try {
      const shard = await this.readRowMapShard(tableId, rowId);
      const pageId = shard[rowId];
      if (pageId === undefined) return null;

      const page = await this.readPage(tableId, pageId);
      if (!page) return null;
      return page.rows[rowId] ?? null;
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  // ---------------------------------------------------------------------------
  // Row map (logicalId → pageId)
  // ---------------------------------------------------------------------------

  private async readRowMapShard(tableId: string, rowId: number): Promise<RowMapShard> {
    const key = this.getRowMapShardKey(tableId, rowId);
    if (this.wal.has(key)) return this.wal.get(key) as RowMapShard;
    if (this.cache.has(key)) {
      const copy = { ...(this.cache.get(key) as RowMapShard) };
      this.wal.set(key, copy);
      return copy;
    }
    const map = await this.storage.get<RowMapShard>(key);
    if (map) {
      this.cache.set(key, map);
      const copy = { ...map };
      this.wal.set(key, copy);
      return copy;
    }
    const fresh: RowMapShard = {};
    this.wal.set(key, fresh);
    return fresh;
  }

  private getRowMapShardKey(tableId: string, rowId: number): string {
    const shard = Math.floor(rowId / ROW_MAP_SHARD_SIZE);
    return `rowmap:${tableId}:${shard}`;
  }

  // ---------------------------------------------------------------------------
  // Page-level (internal)
  // ---------------------------------------------------------------------------

  private async readPage(tableId: string, pageId: number): Promise<Page | null> {
    const key = this.getPageKey(tableId, pageId);
    if (this.wal.has(key)) return this.wal.get(key) as Page | null;
    if (this.cache.has(key)) return this.cache.get(key) as Page | null;
    const page = await this.storage.get<Page>(key);
    if (page) this.cache.set(key, page);
    return page;
  }

  async getPageMeta(tableId: string): Promise<PageMeta> {
    const key = this.getMetaKey(tableId);
    if (this.wal.has(key)) return this.wal.get(key) as PageMeta;
    if (this.cache.has(key)) return this.cache.get(key) as PageMeta;
    const meta = await this.storage.get<PageMeta>(key);
    if (meta) {
      this.cache.set(key, meta);
      return meta;
    }
    return { ...DEFAULT_META, freePageIds: [] };
  }

  private writePage(tableId: string, page: Page): void {
    this.wal.set(this.getPageKey(tableId, page.pageId), page);
  }

  private writeMeta(tableId: string, meta: PageMeta): void {
    this.wal.set(this.getMetaKey(tableId), meta);
  }

  private getPageKey(tableId: string, pageId: number): string {
    return `page:${tableId}:${String(pageId).padStart(PAGE_ID_WIDTH, '0')}`;
  }

  private getMetaKey(tableId: string): string {
    return `meta:pages:${tableId}`;
  }

  async getAllPageKeys(tableId: string): Promise<string[]> {
    return this.storage.getAllKeys(`page:${tableId}:`);
  }

  // ---------------------------------------------------------------------------
  // Delete all page data + meta + rowmap for a table
  // ---------------------------------------------------------------------------

  async deleteTableData(tableId: string): Promise<void> {
    const pageKeys = await this.getAllPageKeys(tableId);
    for (const key of pageKeys) {
      this.deleteKey(key);
    }
    // Also delete any page keys only in WAL (not yet committed)
    const prefix = `page:${tableId}:`;
    for (const key of this.wal.keys()) {
      if (key.startsWith(prefix) && this.wal.get(key) !== null) {
        this.deleteKey(key);
      }
    }
    this.deleteKey(this.getMetaKey(tableId));
    // Delete all rowmap shards
    const rmPrefix = `rowmap:${tableId}:`;
    const rmKeys = await this.getAllKeys(rmPrefix);
    for (const key of rmKeys) {
      this.deleteKey(key);
    }
  }

  // ---------------------------------------------------------------------------
  // KV operations (used by B-tree)
  // ---------------------------------------------------------------------------

  async readKey<T>(key: string): Promise<T | null> {
    if (this.wal.has(key)) {
      const val = this.wal.get(key);
      return val === null ? null : (val as T);
    }
    if (this.cache.has(key)) {
      const val = this.cache.get(key);
      return val === null ? null : (val as T);
    }
    const val = await this.storage.get<T>(key);
    if (val !== null && val !== undefined) {
      this.cache.set(key, val);
    }
    return val;
  }

  async getAllKeys(prefix: string): Promise<string[]> {
    const storageKeys = new Set(await this.storage.getAllKeys(prefix));
    for (const [k, v] of this.wal) {
      if (!k.startsWith(prefix)) continue;
      if (v === null) {
        storageKeys.delete(k);
      } else {
        storageKeys.add(k);
      }
    }
    return [...storageKeys].sort();
  }

  writeKey(key: string, value: unknown): void {
    this.cache.delete(key);
    this.wal.set(key, value);
  }

  deleteKey(key: string): void {
    this.cache.delete(key);
    this.wal.set(key, null);
  }

  // ---------------------------------------------------------------------------
  // Transaction control
  // ---------------------------------------------------------------------------

  async commit(): Promise<void> {
    if (this.wal.size === 0) return;
    const entries: Array<[string, unknown]> = [...this.wal.entries()];
    await this.storage.putMany(entries);
    for (const [key, value] of entries) {
      if (value === null) {
        this.cache.delete(key);
      } else {
        this.cache.set(key, value);
      }
    }
    this.wal.clear();
  }

  rollback(): void {
    this.wal.clear();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private ensureWritable(tableId: string, page: Page): Page {
    const key = this.getPageKey(tableId, page.pageId);
    if (this.wal.has(key)) {
      return this.wal.get(key) as Page;
    }
    const copy: Page = { ...page, rows: { ...page.rows } };
    this.wal.set(key, copy);
    return copy;
  }

  private rowCount(page: Page): number {
    return Object.keys(page.rows).length;
  }

  private async requirePage(tableId: string, pageId: number): Promise<Page> {
    const page = await this.readPage(tableId, pageId);
    if (!page) {
      throw new StorageError(`Page ${pageId} not found in table ${tableId}`);
    }
    return page;
  }
}

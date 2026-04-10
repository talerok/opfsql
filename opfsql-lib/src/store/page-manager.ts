import { StorageError, wrapStorageError } from './errors.js';
import type { IPageManager, IStorage, Page, PageMeta, PageRow, Row, RowId } from './types.js';
import { PAGE_SIZE } from './types.js';

const PAGE_ID_WIDTH = 6;
const DEFAULT_META: PageMeta = { lastPageId: -1, totalRowCount: 0, deadRowCount: 0 };

export class PageManager implements IPageManager {
  private wal = new Map<string, unknown>();
  private cache = new Map<string, unknown>();

  constructor(private readonly storage: IStorage) {}

  // ---------------------------------------------------------------------------
  // Row operations (absorbed from RowManager)
  // ---------------------------------------------------------------------------

  async prepareInsert(tableId: string, row: Row): Promise<RowId> {
    try {
      const meta = await this.getPageMeta(tableId);

      let page: Page | null = null;
      if (meta.lastPageId >= 0) {
        page = await this.readPage(tableId, meta.lastPageId);
      }

      let writable: Page;
      if (!page || this.liveRowCount(page) >= PAGE_SIZE) {
        meta.lastPageId++;
        writable = { pageId: meta.lastPageId, tableId, rows: [] };
        this.writePage(tableId, writable);
      } else {
        writable = this.ensureWritable(tableId, page);
      }

      const slotId = writable.rows.length;
      writable.rows.push({ slotId, deleted: false, data: row });
      meta.totalRowCount++;

      this.writeMeta(tableId, meta);

      return { pageId: writable.pageId, slotId };
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
      const meta = await this.getPageMeta(tableId);
      const oldPage = await this.requirePage(tableId, rowId.pageId);

      // Mark old slot as deleted in-place on writable page
      const writableOld = this.ensureWritable(tableId, oldPage);
      writableOld.rows[rowId.slotId] = { ...writableOld.rows[rowId.slotId], deleted: true };
      meta.deadRowCount++;

      let writableNew: Page;
      if (rowId.pageId === meta.lastPageId) {
        writableNew = writableOld;
      } else {
        const lastPage = await this.requirePage(tableId, meta.lastPageId);
        writableNew = this.ensureWritable(tableId, lastPage);
      }

      if (this.liveRowCount(writableNew) >= PAGE_SIZE) {
        meta.lastPageId++;
        writableNew = { pageId: meta.lastPageId, tableId, rows: [] };
        this.writePage(tableId, writableNew);
      }

      const slotId = writableNew.rows.length;
      writableNew.rows.push({ slotId, deleted: false, data: row });
      meta.totalRowCount++;

      this.writeMeta(tableId, meta);

      return { pageId: writableNew.pageId, slotId };
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  async prepareDelete(tableId: string, rowId: RowId): Promise<void> {
    try {
      const meta = await this.getPageMeta(tableId);
      const page = await this.requirePage(tableId, rowId.pageId);

      const writable = this.ensureWritable(tableId, page);
      writable.rows[rowId.slotId] = { ...writable.rows[rowId.slotId], deleted: true };
      meta.deadRowCount++;

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
      for (const pr of page.rows) {
        if (!pr.deleted) {
          yield { rowId: { pageId: pid, slotId: pr.slotId }, row: pr.data };
        }
      }
    }
  }

  async readRow(tableId: string, rowId: RowId): Promise<Row | null> {
    try {
      const page = await this.readPage(tableId, rowId.pageId);
      if (!page) return null;
      const pr = page.rows[rowId.slotId];
      if (!pr || pr.deleted) return null;
      return pr.data;
    } catch (err) {
      throw wrapStorageError(err);
    }
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
    return { ...DEFAULT_META };
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
  // Delete all page data + meta for a table
  // ---------------------------------------------------------------------------

  async deleteTableData(tableId: string): Promise<void> {
    // Delete page keys from storage
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
  }

  // ---------------------------------------------------------------------------
  // Compaction — collects live rows, rewrites pages compactly
  // ---------------------------------------------------------------------------

  async compactTable(tableId: string): Promise<PageRow[]> {
    const meta = await this.getPageMeta(tableId);
    const oldKeys = await this.getAllPageKeys(tableId);

    const liveRows: PageRow[] = [];
    for (let pid = 0; pid <= meta.lastPageId; pid++) {
      const page = await this.readPage(tableId, pid);
      if (!page) continue;
      for (const pr of page.rows) {
        if (!pr.deleted) liveRows.push(pr);
      }
    }

    // Delete all old page keys
    for (const key of oldKeys) {
      this.deleteKey(key);
    }

    // Write new compacted pages
    let pageId = 0;
    for (let i = 0; i < liveRows.length; i += PAGE_SIZE) {
      const chunk = liveRows.slice(i, i + PAGE_SIZE);
      const rows = chunk.map((pr, slotId) => ({
        slotId,
        deleted: false,
        data: pr.data,
      }));
      this.writePage(tableId, { pageId, tableId, rows });
      pageId++;
    }

    const newMeta: PageMeta = {
      lastPageId: liveRows.length === 0 ? -1 : pageId - 1,
      totalRowCount: liveRows.length,
      deadRowCount: 0,
    };
    this.writeMeta(tableId, newMeta);

    return liveRows;
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
    // Invalidate cache to prevent stale data after rollback
    // (BTree may have mutated the cached object in-place)
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
    // Move committed data to cache (buffer pool)
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
    // cache stays intact — only uncommitted data is discarded
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private ensureWritable(tableId: string, page: Page): Page {
    const key = this.getPageKey(tableId, page.pageId);
    if (this.wal.has(key)) {
      return this.wal.get(key) as Page;
    }
    const copy: Page = { ...page, rows: [...page.rows] };
    this.wal.set(key, copy);
    return copy;
  }

  private liveRowCount(page: Page): number {
    return page.rows.filter((r) => !r.deleted).length;
  }

  private async requirePage(tableId: string, pageId: number): Promise<Page> {
    const page = await this.readPage(tableId, pageId);
    if (!page) {
      throw new StorageError(`Page ${pageId} not found in table ${tableId}`);
    }
    return page;
  }
}

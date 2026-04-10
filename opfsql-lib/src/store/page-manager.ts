import type { IPageManager, IStorage, Page, PageMeta } from './types.js';

const PAGE_ID_WIDTH = 6;
const DEFAULT_META: PageMeta = { lastPageId: -1, totalRowCount: 0, deadRowCount: 0 };

export class PageManager implements IPageManager {
  private wal = new Map<string, unknown>();
  private savedWal: Map<string, unknown> | null = null;

  constructor(private readonly storage: IStorage) {}

  async readPage(tableId: string, pageId: number): Promise<Page | null> {
    const key = this.getPageKey(tableId, pageId);
    if (this.wal.has(key)) return this.wal.get(key) as Page | null;
    return this.storage.get<Page>(key);
  }

  async getPageMeta(tableId: string): Promise<PageMeta> {
    const key = this.getMetaKey(tableId);
    if (this.wal.has(key)) return this.wal.get(key) as PageMeta;
    const meta = await this.storage.get<PageMeta>(key);
    return meta ?? { ...DEFAULT_META };
  }

  createEmptyPage(tableId: string, pageId: number): Page {
    return { pageId, tableId, rows: [] };
  }

  getPageKey(tableId: string, pageId: number): string {
    return `page:${tableId}:${String(pageId).padStart(PAGE_ID_WIDTH, '0')}`;
  }

  getMetaKey(tableId: string): string {
    return `meta:pages:${tableId}`;
  }

  async getAllPageKeys(tableId: string): Promise<string[]> {
    return this.storage.getAllKeys(`page:${tableId}:`);
  }

  async readKey<T>(key: string): Promise<T | null> {
    if (this.wal.has(key)) {
      const val = this.wal.get(key);
      return val === null ? null : (val as T);
    }
    return this.storage.get<T>(key);
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

  writePage(tableId: string, page: Page): void {
    this.wal.set(this.getPageKey(tableId, page.pageId), page);
  }

  writeMeta(tableId: string, meta: PageMeta): void {
    this.wal.set(this.getMetaKey(tableId), meta);
  }

  writeKey(key: string, value: unknown): void {
    this.wal.set(key, value);
  }

  deleteKey(key: string): void {
    this.wal.set(key, null);
  }

  checkpoint(): void {
    this.savedWal = new Map(
      [...this.wal.entries()].map(([k, v]) => [k, v !== null ? structuredClone(v) : null]),
    );
  }

  restoreCheckpoint(): void {
    if (this.savedWal) {
      this.wal = this.savedWal;
      this.savedWal = null;
    }
  }

  async commit(): Promise<void> {
    if (this.wal.size === 0) return;
    const entries: Array<[string, unknown]> = [...this.wal.entries()];
    await this.storage.putMany(entries);
    this.wal.clear();
  }

  rollback(): void {
    this.wal.clear();
  }
}

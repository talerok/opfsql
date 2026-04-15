import type { SyncIPageStore, SyncIPageStorage } from './types.js';
import type { ICache } from './cache.js';
import { LRUCache } from './cache.js';

const DEFAULT_CACHE_SIZE = 256;
const FREELIST_PAGE_NO = 2;

export class SyncPageStore implements SyncIPageStore {
  private wal = new Map<number, unknown>();
  private cache: ICache<number, unknown>;
  private nextPageId: number;
  private freeList: number[];
  private allocatorDirty = false;

  // Lazy snapshots for rollback (created on first mutation)
  private snapNextPageId: number | null = null;
  private snapFreeList: number[] | null = null;

  constructor(
    private readonly storage: SyncIPageStorage,
    nextPageId: number,
    freeList: number[],
    cacheSize = DEFAULT_CACHE_SIZE,
  ) {
    this.nextPageId = nextPageId;
    this.freeList = freeList;
    this.cache = new LRUCache(cacheSize);
  }

  readPage<T>(pageNo: number): T | null {
    if (this.wal.has(pageNo)) {
      const val = this.wal.get(pageNo);
      return val === null ? null : (val as T);
    }
    const cached = this.cache.get(pageNo);
    if (cached !== undefined) return cached as T;
    const val = this.storage.readPage<T>(pageNo);
    if (val !== null && val !== undefined) this.cache.set(pageNo, val);
    return val;
  }

  writePage(pageNo: number, value: unknown): void {
    this.ensureSnapshot();
    this.wal.set(pageNo, value);
  }

  allocPage(): number {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    if (this.freeList.length > 0) {
      const pageNo = this.freeList.pop()!;
      this.cache.delete(pageNo);
      return pageNo;
    }
    return this.nextPageId++;
  }

  freePage(pageNo: number): void {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    this.freeList.push(pageNo);
  }

  commit(): void {
    if (this.wal.size === 0 && !this.allocatorDirty) return;

    // Write all dirty pages to storage
    for (const [pageNo, value] of this.wal) {
      if (value !== null) {
        this.storage.writePage(pageNo, value);
        this.cache.set(pageNo, value);
      }
    }

    if (this.allocatorDirty) {
      this.storage.writePage(FREELIST_PAGE_NO, this.freeList);
      this.storage.writeHeader(this.nextPageId);
    }

    this.storage.flush();

    this.wal.clear();
    this.snapNextPageId = null;
    this.snapFreeList = null;
    this.allocatorDirty = false;
  }

  rollback(): void {
    this.wal.clear();
    if (this.snapNextPageId !== null) {
      this.nextPageId = this.snapNextPageId;
      this.freeList = this.snapFreeList!;
    }
    this.snapNextPageId = null;
    this.snapFreeList = null;
    this.allocatorDirty = false;
  }

  private ensureSnapshot(): void {
    if (this.snapNextPageId === null) {
      this.snapNextPageId = this.nextPageId;
      this.snapFreeList = [...this.freeList];
    }
  }
}

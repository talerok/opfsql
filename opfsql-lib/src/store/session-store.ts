import type { Storage } from "./storage.js";
import type { CatalogData, SyncIPageStore } from "./types.js";

// ---------------------------------------------------------------------------
// SessionStore — per-session write buffer over Storage
// ---------------------------------------------------------------------------

export class SessionStore implements SyncIPageStore {
  private buffer = new Map<number, unknown>();
  private allocatorDirty = false;
  private snapNextPageId: number | null = null;
  private snapFreeList: number[] | null = null;

  constructor(
    private readonly storage: Storage,
    private readonly catalogPageNo: number,
  ) {}

  readPage<T>(pageNo: number): T | null {
    if (this.buffer.has(pageNo)) {
      const v = this.buffer.get(pageNo);
      return v === null ? null : (v as T);
    }
    return this.storage.readPage<T>(pageNo);
  }

  writePage(pageNo: number, value: unknown): void {
    this.ensureSnapshot();
    this.buffer.set(pageNo, value);
  }

  allocPage(): number {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    return this.storage.allocPage();
  }

  freePage(pageNo: number): void {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    this.storage.freePage(pageNo);
  }

  commit(): void {
    this.storage.commitPages(this.buffer, this.allocatorDirty);
    this.buffer.clear();
    this.clearSnapshot();
  }

  rollback(): void {
    this.buffer.clear();
    if (this.snapNextPageId !== null) {
      this.storage.restoreAllocator(this.snapNextPageId, this.snapFreeList!);
    }
    this.clearSnapshot();
  }

  writeCatalog(data: CatalogData): void {
    this.writePage(this.catalogPageNo, data);
  }

  private ensureSnapshot(): void {
    if (this.snapNextPageId === null) {
      const snap = this.storage.snapshotAllocator();
      this.snapNextPageId = snap.nextPageId;
      this.snapFreeList = snap.freeList;
    }
  }

  private clearSnapshot(): void {
    this.snapNextPageId = null;
    this.snapFreeList = null;
    this.allocatorDirty = false;
  }
}

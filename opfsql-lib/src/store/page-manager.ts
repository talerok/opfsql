import type { SyncIPageStorage, SyncIPageStore } from "./types.js";

const FREELIST_PAGE_NO = 2;

export class SyncPageStore implements SyncIPageStore {
  private dirtyPages = new Map<number, unknown>();
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
  ) {
    this.nextPageId = nextPageId;
    this.freeList = freeList;
  }

  readPage<T>(pageNo: number): T | null {
    if (this.dirtyPages.has(pageNo)) {
      const val = this.dirtyPages.get(pageNo);
      return val === null ? null : (val as T);
    }
    return this.storage.readPage<T>(pageNo);
  }

  writePage(pageNo: number, value: unknown): void {
    this.ensureSnapshot();
    this.dirtyPages.set(pageNo, value);
  }

  allocPage(): number {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    if (this.freeList.length > 0) {
      return this.freeList.pop()!;
    }
    return this.nextPageId++;
  }

  freePage(pageNo: number): void {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    this.freeList.push(pageNo);
  }

  commit(): void {
    if (this.dirtyPages.size === 0 && !this.allocatorDirty) return;

    // Write all dirty pages to storage
    for (const [pageNo, value] of this.dirtyPages) {
      if (value !== null) {
        this.storage.writePage(pageNo, value);
      }
    }

    if (this.allocatorDirty) {
      this.storage.writePage(FREELIST_PAGE_NO, this.freeList);
      this.storage.writeHeader(this.nextPageId);
    }

    this.storage.flush();

    this.dirtyPages.clear();
    this.snapNextPageId = null;
    this.snapFreeList = null;
    this.allocatorDirty = false;
  }

  rollback(): void {
    this.dirtyPages.clear();
    if (this.snapNextPageId !== null) {
      this.nextPageId = this.snapNextPageId;
      this.freeList = this.snapFreeList!;
    }
    this.snapNextPageId = null;
    this.snapFreeList = null;
    this.allocatorDirty = false;
  }

  snapshotAllocator(): { nextPageId: number; freeList: number[] } {
    return { nextPageId: this.nextPageId, freeList: [...this.freeList] };
  }

  restoreAllocator(nextPageId: number, freeList: number[]): void {
    this.nextPageId = nextPageId;
    this.freeList = freeList;
  }

  /** Re-read allocator state from storage (after external catch-up). */
  refreshFromStorage(): void {
    this.nextPageId = this.storage.getNextPageId();
    this.freeList = this.storage.readPage<number[]>(FREELIST_PAGE_NO) ?? [];
    this.dirtyPages.clear();
  }

  private ensureSnapshot(): void {
    if (this.snapNextPageId === null) {
      this.snapNextPageId = this.nextPageId;
      this.snapFreeList = [...this.freeList];
    }
  }
}

// ---------------------------------------------------------------------------
// SessionPageStore — per-session write buffer over shared SyncPageStore
// ---------------------------------------------------------------------------

export class SessionPageStore implements SyncIPageStore {
  private buffer = new Map<number, unknown>();
  private allocatorDirty = false;
  private snapNextPageId: number | null = null;
  private snapFreeList: number[] | null = null;

  constructor(private readonly shared: SyncPageStore) {}

  readPage<T>(pageNo: number): T | null {
    if (this.buffer.has(pageNo)) {
      const v = this.buffer.get(pageNo);
      return v === null ? null : (v as T);
    }
    return this.shared.readPage<T>(pageNo);
  }

  writePage(pageNo: number, value: unknown): void {
    this.ensureSnapshot();
    this.buffer.set(pageNo, value);
  }

  allocPage(): number {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    return this.shared.allocPage();
  }

  freePage(pageNo: number): void {
    this.ensureSnapshot();
    this.allocatorDirty = true;
    this.shared.freePage(pageNo);
  }

  commit(): void {
    if (this.buffer.size === 0 && !this.allocatorDirty) return;
    for (const [pageNo, value] of this.buffer) {
      this.shared.writePage(pageNo, value);
    }
    this.shared.commit();
    this.buffer.clear();
    this.clearSnapshot();
  }

  rollback(): void {
    this.buffer.clear();
    if (this.snapNextPageId !== null) {
      this.shared.restoreAllocator(this.snapNextPageId, this.snapFreeList!);
    }
    this.clearSnapshot();
  }

  private ensureSnapshot(): void {
    if (this.snapNextPageId === null) {
      const snap = this.shared.snapshotAllocator();
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

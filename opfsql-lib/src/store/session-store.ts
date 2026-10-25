import type { Storage } from "./storage.js";
import type { CatalogData, SyncIPageStore } from "./types.js";

// ---------------------------------------------------------------------------
// SessionStore — per-session write buffer and read snapshot over Storage
// ---------------------------------------------------------------------------

export class SessionStore implements SyncIPageStore {
  private buffer = new Map<number, unknown>();
  private allocatorDirty = false;
  private savedNextPageId: number | null = null;
  private savedFreeList: number[] | null = null;

  /** Commit position this session reads at; null reads the latest commit. */
  private readSnapshot: number | null = null;

  constructor(
    private readonly storage: Storage,
    private readonly catalogPageNo: number,
  ) {}

  readPage<T>(pageNo: number): T | null {
    if (this.buffer.has(pageNo)) {
      const v = this.buffer.get(pageNo);
      return v === null ? null : (v as T);
    }
    return this.storage.readPage<T>(pageNo, this.readSnapshot ?? undefined);
  }

  writePage(pageNo: number, value: unknown): void {
    this.saveAllocator();
    this.buffer.set(pageNo, value);
  }

  allocPage(): number {
    this.saveAllocator();
    this.allocatorDirty = true;
    return this.storage.allocPage();
  }

  freePage(pageNo: number): void {
    this.saveAllocator();
    this.allocatorDirty = true;
    this.storage.freePage(pageNo);
  }

  commit(): void {
    this.storage.commitPages(this.buffer, this.allocatorDirty);
    this.buffer.clear();
    this.forgetAllocator();
  }

  rollback(): void {
    this.buffer.clear();
    if (this.savedNextPageId !== null) {
      this.storage.restoreAllocator(this.savedNextPageId, this.savedFreeList!);
    }
    this.forgetAllocator();
  }

  writeCatalog(data: CatalogData): void {
    this.writePage(this.catalogPageNo, data);
  }

  // -------------------------------------------------------------------------
  // Read snapshot
  // -------------------------------------------------------------------------

  /** Pins the latest commit: reads see it until endSnapshot(), whatever commits later. */
  beginSnapshot(): void {
    if (this.readSnapshot === null) {
      this.readSnapshot = this.storage.pinSnapshot();
    }
  }

  endSnapshot(): void {
    if (this.readSnapshot === null) return;
    this.storage.releaseSnapshot(this.readSnapshot);
    this.readSnapshot = null;
  }

  /** True when something has committed since this session's snapshot. */
  isSnapshotStale(): boolean {
    return (
      this.readSnapshot !== null &&
      this.storage.latestCommit() > this.readSnapshot
    );
  }

  // -------------------------------------------------------------------------
  // Allocator rollback state
  // -------------------------------------------------------------------------

  private saveAllocator(): void {
    if (this.savedNextPageId === null) {
      const saved = this.storage.snapshotAllocator();
      this.savedNextPageId = saved.nextPageId;
      this.savedFreeList = saved.freeList;
    }
  }

  private forgetAllocator(): void {
    this.savedNextPageId = null;
    this.savedFreeList = null;
    this.allocatorDirty = false;
  }
}

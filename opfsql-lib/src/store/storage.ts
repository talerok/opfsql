import { SessionStore } from "./session-store.js";
import type { CatalogData, SyncIPageStorage } from "./types.js";

// ---------------------------------------------------------------------------
// Page layout (private to storage layer)
// ---------------------------------------------------------------------------

const CATALOG_PAGE_NO = 1;
const FREELIST_PAGE_NO = 2;

export interface CatchUpResult {
  catalog: boolean;
  data: boolean;
}

// ---------------------------------------------------------------------------
// Storage — single entry point for all page I/O and allocation
// ---------------------------------------------------------------------------

export class Storage {
  private nextPageId!: number;
  private freeList!: number[];

  constructor(private readonly backend: SyncIPageStorage) {}

  async open(): Promise<void> {
    await this.backend.open();
    this.nextPageId = this.backend.getNextPageId();
    this.freeList = this.backend.readPage<number[]>(FREELIST_PAGE_NO) ?? [];
  }

  // -------------------------------------------------------------------------
  // Session factory
  // -------------------------------------------------------------------------

  createSession(): SessionStore {
    return new SessionStore(this, CATALOG_PAGE_NO);
  }

  // -------------------------------------------------------------------------
  // Page I/O (direct backend access)
  // -------------------------------------------------------------------------

  readPage<T>(pageNo: number): T | null {
    return this.backend.readPage<T>(pageNo);
  }

  // -------------------------------------------------------------------------
  // Allocator
  // -------------------------------------------------------------------------

  allocPage(): number {
    if (this.freeList.length > 0) {
      return this.freeList.pop()!;
    }
    return this.nextPageId++;
  }

  freePage(pageNo: number): void {
    this.freeList.push(pageNo);
  }

  snapshotAllocator(): { nextPageId: number; freeList: number[] } {
    return { nextPageId: this.nextPageId, freeList: [...this.freeList] };
  }

  restoreAllocator(nextPageId: number, freeList: number[]): void {
    this.nextPageId = nextPageId;
    this.freeList = freeList;
  }

  // -------------------------------------------------------------------------
  // Commit — write dirty pages + allocator state to backend
  // -------------------------------------------------------------------------

  commitPages(dirtyPages: Map<number, unknown>, allocatorDirty: boolean): void {
    if (dirtyPages.size === 0 && !allocatorDirty) {
      return;
    }

    for (const [pageNo, value] of dirtyPages) {
      if (value !== null) {
        this.backend.writePage(pageNo, value);
      }
    }

    if (allocatorDirty) {
      this.backend.writePage(FREELIST_PAGE_NO, this.freeList);
      this.backend.writeHeader(this.nextPageId);
    }

    this.backend.flush();
  }

  // -------------------------------------------------------------------------
  // Catalog
  // -------------------------------------------------------------------------

  readCatalog(): CatalogData | null {
    return this.backend.readPage<CatalogData>(CATALOG_PAGE_NO);
  }

  // -------------------------------------------------------------------------
  // Catch-up
  // -------------------------------------------------------------------------

  catchUp(): CatchUpResult | null {
    const changes = this.backend.catchUp?.();

    if (!changes) {
      return null;
    }

    this.refreshFromStorage();
    const array = [...changes];

    // Empty set = epoch change (full invalidation) → both true
    const empty = array.length === 0;

    const catalog = empty || changes.has(CATALOG_PAGE_NO);
    const data = empty || array.some((p) => p !== CATALOG_PAGE_NO);
    return { catalog, data };
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  checkpoint(): void {
    this.backend.checkpoint?.();
  }

  close(): void {
    this.backend.close();
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private refreshFromStorage(): void {
    this.nextPageId = this.backend.getNextPageId();
    this.freeList = this.backend.readPage<number[]>(FREELIST_PAGE_NO) ?? [];
  }
}

import { checkStoreIntegrity } from "./integrity.js";
import { encodePage, type EncodedPage } from "./page-codec.js";
import { SessionStore } from "./session-store.js";
import type { CatalogData, ICatalog, SyncIPageStorage } from "./types.js";

// ---------------------------------------------------------------------------
// Page layout (private to storage layer)
// ---------------------------------------------------------------------------

const CATALOG_PAGE_NO = 1;
const FREELIST_PAGE_NO = 2;

export interface CatchUpResult {
  catalog: boolean;
  data: boolean;
}

export interface StorageOptions {
  /**
   * Called when the checkpoint that follows a commit fails. The commit itself
   * has already succeeded; the checkpoint is retried after the next commit.
   */
  onCheckpointError?: (err: unknown) => void;
}

function warnCheckpointError(err: unknown): void {
  console.warn(
    "[opfsql] checkpoint failed; it will be retried after the next commit",
    err,
  );
}

// ---------------------------------------------------------------------------
// Storage — single entry point for all page I/O and allocation
// ---------------------------------------------------------------------------

export class Storage {
  private nextPageId!: number;
  private freeList!: number[];
  private readonly onCheckpointError: (err: unknown) => void;

  constructor(
    private readonly backend: SyncIPageStorage,
    options: StorageOptions = {},
  ) {
    this.onCheckpointError = options.onCheckpointError ?? warnCheckpointError;
  }

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

  /**
   * Encodes every dirty page once and hands the batch to the backend in one
   * call. backend.commit() returning is the commit point: anything that throws
   * before it leaves storage untouched, nothing after it may fail the commit.
   */
  commitPages(
    dirtyPages: ReadonlyMap<number, unknown>,
    allocatorDirty: boolean,
  ): void {
    if (dirtyPages.size === 0 && !allocatorDirty) {
      return;
    }

    const pages = new Map<number, EncodedPage>();
    for (const [pageNo, value] of dirtyPages) {
      pages.set(pageNo, encodePage(value));
    }
    if (allocatorDirty) {
      pages.set(FREELIST_PAGE_NO, encodePage(this.freeList));
    }

    this.backend.commit(pages, this.nextPageId);
    this.checkpointIfNeeded();
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
  // Integrity
  // -------------------------------------------------------------------------

  /** Structural check of committed pages against `catalog`; empty means consistent. */
  checkIntegrity(catalog: ICatalog): string[] {
    return checkStoreIntegrity({
      readPage: (pageNo) => this.backend.readPage(pageNo),
      catalog,
      nextPageId: this.nextPageId,
      freeList: this.freeList,
    });
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

  /** Runs after the commit point, so nothing in here may throw. */
  private checkpointIfNeeded(): void {
    if (!this.backend.needsCheckpoint?.()) return;
    try {
      this.backend.checkpoint?.();
    } catch (err) {
      this.reportCheckpointError(err);
    }
  }

  private reportCheckpointError(err: unknown): void {
    try {
      this.onCheckpointError(err);
    } catch (callbackErr) {
      console.error("[opfsql] onCheckpointError threw", callbackErr);
    }
  }

  private refreshFromStorage(): void {
    this.nextPageId = this.backend.getNextPageId();
    this.freeList = this.backend.readPage<number[]>(FREELIST_PAGE_NO) ?? [];
  }
}

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

  /** Open snapshots: commit position → number of sessions reading at it. */
  private readonly pins = new Map<number, number>();

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

  /** Committed data as of `snapshot` (default: the latest commit). */
  readPage<T>(pageNo: number, snapshot?: number): T | null {
    return this.backend.readPage<T>(pageNo, snapshot);
  }

  // -------------------------------------------------------------------------
  // Snapshots
  // -------------------------------------------------------------------------

  /** Position of the latest commit; 0 for backends without page versions. */
  latestCommit(): number {
    return this.backend.latestCommit?.() ?? 0;
  }

  /**
   * Pins the latest commit, so its pages stay readable until released.
   * Returns null when another tab's checkpoint may already have moved past
   * it: catch up and try again.
   */
  tryPinSnapshot(): number | null {
    const snapshot = this.latestCommit();
    this.addPin(snapshot, 1);
    if (this.backend.isSnapshotSafe?.(snapshot) ?? true) return snapshot;
    this.addPin(snapshot, -1);
    return null;
  }

  releaseSnapshot(snapshot: number): void {
    this.addPin(snapshot, -1);
  }

  private addPin(snapshot: number, delta: 1 | -1): void {
    const count = (this.pins.get(snapshot) ?? 0) + delta;
    if (count > 0) this.pins.set(snapshot, count);
    else this.pins.delete(snapshot);
    this.backend.setOldestSnapshot?.(this.oldestPin());
  }

  private oldestPin(): number | null {
    let oldest: number | null = null;
    for (const snapshot of this.pins.keys()) {
      if (oldest === null || snapshot < oldest) oldest = snapshot;
    }
    return oldest;
  }

  /** Oldest pinned snapshot, or the latest commit when nothing is pinned. */
  private oldestSnapshot(): number {
    return this.oldestPin() ?? this.latestCommit();
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

  /** Brings the main file as far forward as open snapshots allow. */
  checkpoint(): void {
    this.backend.checkpoint?.(this.oldestSnapshot());
  }

  /**
   * Checkpoints when the backend asks for it. Runs after a commit, so it
   * never throws: a failure is reported and the checkpoint retried later.
   */
  checkpointIfNeeded(): void {
    if (!this.backend.needsCheckpoint?.()) return;
    try {
      this.checkpoint();
    } catch (err) {
      this.reportCheckpointError(err);
      return;
    }
    // Still needed: a snapshot holds it back. If that snapshot belongs to a
    // tab that died, free it so the next checkpoint can go further.
    if (this.backend.needsCheckpoint?.()) {
      this.backend
        .releaseAbandonedSnapshots?.()
        .catch((err: unknown) => this.reportCheckpointError(err));
    }
  }

  close(): void {
    this.backend.close();
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

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

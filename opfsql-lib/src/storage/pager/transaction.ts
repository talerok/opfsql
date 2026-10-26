import { InternalError, MisuseError } from "../../errors.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import {
  AllocationMap,
  isGroupStart,
  isMapPage,
  newMapPage,
} from "./allocation.js";
import {
  HEADER_PAGE,
  SCHEMA_ROOT_PAGE,
  schemaVersionOf,
  setSchemaVersion,
} from "./database-file.js";
import type { Pager } from "./pager.js";

/** The header, the first allocation map and the schema root are never free. */
const FIRST_ALLOCATABLE = SCHEMA_ROOT_PAGE + 1;

/** Reads the database as of one snapshot, pinned until end(). */
export class ReadTransaction implements PageReader {
  protected isOpen = true;
  /**
   * Pages in the database as of the snapshot, taken when it was pinned: the
   * database file's size, which it may come from, changes when another tab
   * truncates a free tail (docs/concurrency.md, rule 4).
   */
  private readonly pinnedPageCount: number;

  constructor(
    protected readonly pager: Pager,
    readonly snapshot: number,
  ) {
    this.pinnedPageCount = pager.pageCount(snapshot);
  }

  get pageSize(): number {
    return this.pager.pageSize;
  }

  /** Pages in the database as of the snapshot. */
  get pageCount(): number {
    return this.pinnedPageCount;
  }

  /** Incremented by every transaction that changes the schema. */
  get schemaVersion(): number {
    return schemaVersionOf(this.read(HEADER_PAGE));
  }

  read(pageNo: number): Uint8Array {
    this.ensureOpen();
    return this.pager.read(pageNo, this.snapshot);
  }

  /**
   * True if the transaction reads changes of its own, not committed: then
   * what it reads is no commit's. A read transaction never has any.
   */
  hasChanges(): boolean {
    return false;
  }

  /** Releases the snapshot; later calls do nothing. */
  end(): void {
    if (!this.isOpen) return;
    this.isOpen = false;
    this.pager.unpin(this.snapshot);
  }

  /** A transaction that ended can do nothing more: its pages may be shared. */
  protected ensureOpen(): void {
    if (!this.isOpen) throw new MisuseError("the transaction has ended");
  }
}

/**
 * A point a write transaction can return to, undoing what was written since:
 * a failed statement, or ROLLBACK TO. It lives in memory only; nothing of it
 * reaches the WAL, the read marks or other tabs.
 */
export class Savepoint {
  /** Each page changed since: its bytes before, or null if it was unchanged. */
  readonly before = new Map<number, Uint8Array | null>();

  /** WriteTransaction.savepoint() makes them. */
  constructor(
    readonly pageCount: number,
    readonly freeHint: number,
  ) {}
}

/**
 * Changes pages on top of the latest commit. Every page it writes is a
 * private copy until commit() appends them to the WAL as one transaction.
 *
 * Savepoints nest. Changing a page records its bytes in the innermost one;
 * releasing one hands what it recorded to the one around it. Objects that
 * hold a page's bytes — editors, cursors — must not outlive a rollback to a
 * savepoint made before they read them: the bytes they hold are discarded.
 */
export class WriteTransaction extends ReadTransaction implements PageWriter {
  private readonly dirty = new Map<number, Uint8Array>();
  /** Open savepoints, the innermost last. */
  private readonly savepoints: Savepoint[] = [];
  private readonly map: AllocationMap;
  private readonly initialPageCount: number;
  private currentPageCount: number;
  /** No page below this one is free. */
  private freeHint = FIRST_ALLOCATABLE;

  constructor(pager: Pager, snapshot: number) {
    super(pager, snapshot);
    this.initialPageCount = super.pageCount;
    this.currentPageCount = this.initialPageCount;
    this.map = new AllocationMap(this);
  }

  /** Pages in the database with this transaction's changes. */
  override get pageCount(): number {
    return this.currentPageCount;
  }

  override read(pageNo: number): Uint8Array {
    this.ensureOpen();
    return this.dirty.get(pageNo) ?? super.read(pageNo);
  }

  write(pageNo: number): Uint8Array {
    this.ensureOpen();
    this.remember(pageNo);
    const dirty = this.dirty.get(pageNo);
    if (dirty !== undefined) return dirty;
    const copy = new Uint8Array(super.read(pageNo));
    this.dirty.set(pageNo, copy);
    return copy;
  }

  allocate(): number {
    this.ensureOpen();
    const pageNo =
      this.map.findFree(this.freeHint, this.currentPageCount) ?? this.extend();
    this.map.markInUse(pageNo);
    this.remember(pageNo);
    this.dirty.set(pageNo, new Uint8Array(this.pageSize));
    this.freeHint = pageNo + 1;
    return pageNo;
  }

  free(pageNo: number): void {
    this.ensureOpen();
    if (pageNo < FIRST_ALLOCATABLE || isMapPage(pageNo, this.pageSize)) {
      throw new InternalError(`page ${String(pageNo)} can never be freed`);
    }
    this.map.markFree(pageNo);
    this.remember(pageNo);
    this.dirty.delete(pageNo);
    this.freeHint = Math.min(this.freeHint, pageNo);
  }

  /** A point to return to with rollbackTo; nested inside any open one. */
  savepoint(): Savepoint {
    this.ensureOpen();
    const savepoint = new Savepoint(this.currentPageCount, this.freeHint);
    this.savepoints.push(savepoint);
    return savepoint;
  }

  /**
   * Undoes everything written since `savepoint`, savepoints made after it
   * included; `savepoint` stays open, empty, as ROLLBACK TO leaves it.
   */
  rollbackTo(savepoint: Savepoint): void {
    this.ensureOpen();
    const depth = this.depthOf(savepoint);
    const inside = this.savepoints.splice(depth + 1);
    // Innermost first, so the oldest bytes of a page are the ones that stay.
    for (const inner of inside.toReversed()) this.restore(inner);
    this.restore(savepoint);
    savepoint.before.clear();
  }

  /** Keeps what was written since `savepoint`, and closes it and any inside it. */
  release(savepoint: Savepoint): void {
    this.ensureOpen();
    const depth = this.depthOf(savepoint);
    const released = this.savepoints.splice(depth);
    const outer = this.savepoints.at(-1);
    if (outer === undefined) return;
    for (const inner of released) handOver(inner, outer);
  }

  override hasChanges(): boolean {
    return (
      this.dirty.size > 0 || this.currentPageCount !== this.initialPageCount
    );
  }

  /** Records that this transaction changes the schema. */
  bumpSchemaVersion(): void {
    const header = this.write(HEADER_PAGE);
    setSchemaVersion(header, schemaVersionOf(header) + 1);
  }

  /** Makes the changes durable and visible, and ends the transaction. */
  commit(): void {
    this.ensureOpen();
    try {
      this.ensureNoOtherCommit();
      if (this.hasChanges())
        this.pager.append(this.dirty, this.currentPageCount);
    } finally {
      this.discard();
    }
  }

  /** Drops the changes and ends the transaction. */
  rollback(): void {
    this.ensureOpen();
    this.discard();
  }

  /** Ends the transaction; any savepoints still open end with it. */
  private discard(): void {
    this.dirty.clear();
    this.savepoints.length = 0;
    this.end();
  }

  /** Records a page's bytes in the innermost savepoint, before its first change there. */
  private remember(pageNo: number): void {
    const innermost = this.savepoints.at(-1);
    if (innermost === undefined || innermost.before.has(pageNo)) return;
    const dirty = this.dirty.get(pageNo);
    innermost.before.set(pageNo, dirty === undefined ? null : dirty.slice());
  }

  private restore(savepoint: Savepoint): void {
    for (const [pageNo, bytes] of savepoint.before) {
      if (bytes === null) this.dirty.delete(pageNo);
      else this.dirty.set(pageNo, bytes);
    }
    this.currentPageCount = savepoint.pageCount;
    this.freeHint = savepoint.freeHint;
  }

  private depthOf(savepoint: Savepoint): number {
    const depth = this.savepoints.indexOf(savepoint);
    if (depth === -1) throw new MisuseError("the savepoint is not open");
    return depth;
  }

  /** Writers are serialized by a lock: another commit means a bug, not a race. */
  private ensureNoOtherCommit(): void {
    if (this.pager.latestCommit() !== this.snapshot) {
      throw new InternalError(
        "another writer committed during this transaction",
      );
    }
  }

  /** Adds a page at the end of the file. */
  private extend(): number {
    const pageNo = this.currentPageCount;
    this.currentPageCount = pageNo + 1;
    if (isGroupStart(pageNo, this.pageSize)) this.addMapPage(pageNo + 1);
    return pageNo;
  }

  /** A new group's allocation map, which follows the group's first page. */
  private addMapPage(mapPageNo: number): void {
    this.remember(mapPageNo);
    this.dirty.set(mapPageNo, newMapPage(this.pageSize, mapPageNo));
    this.currentPageCount = mapPageNo + 1;
  }
}

/** What `inner` recorded, for the pages `outer` has not: its bytes are older. */
function handOver(inner: Savepoint, outer: Savepoint): void {
  for (const [pageNo, bytes] of inner.before) {
    if (!outer.before.has(pageNo)) outer.before.set(pageNo, bytes);
  }
}

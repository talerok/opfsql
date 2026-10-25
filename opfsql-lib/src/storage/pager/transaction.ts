import { InternalError, MisuseError } from "../../errors.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import {
  AllocationMap,
  groupSize,
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

  constructor(
    protected readonly pager: Pager,
    readonly snapshot: number,
  ) {}

  get pageSize(): number {
    return this.pager.pageSize;
  }

  /** Incremented by every transaction that changes the schema. */
  get schemaVersion(): number {
    return schemaVersionOf(this.read(HEADER_PAGE));
  }

  read(pageNo: number): Uint8Array {
    this.ensureOpen();
    return this.pager.read(pageNo, this.snapshot);
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
 * Changes pages on top of the latest commit. Every page it writes is a
 * private copy until commit() appends them to the WAL as one transaction.
 */
export class WriteTransaction extends ReadTransaction implements PageWriter {
  private readonly dirty = new Map<number, Uint8Array>();
  private readonly map: AllocationMap;
  private readonly initialPageCount: number;
  private pageCount: number;
  /** No page below this one is free. */
  private freeHint = FIRST_ALLOCATABLE;

  constructor(pager: Pager, snapshot: number) {
    super(pager, snapshot);
    this.initialPageCount = pager.pageCount(snapshot);
    this.pageCount = this.initialPageCount;
    this.map = new AllocationMap(this, pager.pageSize);
  }

  override read(pageNo: number): Uint8Array {
    this.ensureOpen();
    return this.dirty.get(pageNo) ?? super.read(pageNo);
  }

  write(pageNo: number): Uint8Array {
    this.ensureOpen();
    const dirty = this.dirty.get(pageNo);
    if (dirty !== undefined) return dirty;
    const copy = new Uint8Array(super.read(pageNo));
    this.dirty.set(pageNo, copy);
    return copy;
  }

  allocate(): number {
    this.ensureOpen();
    const pageNo =
      this.map.findFree(this.freeHint, this.pageCount) ?? this.extend();
    this.map.setInUse(pageNo, true);
    this.dirty.set(pageNo, new Uint8Array(this.pageSize));
    this.freeHint = pageNo + 1;
    return pageNo;
  }

  free(pageNo: number): void {
    this.ensureOpen();
    if (pageNo < FIRST_ALLOCATABLE || isMapPage(pageNo, this.pageSize)) {
      throw new InternalError(`page ${String(pageNo)} can never be freed`);
    }
    this.map.setInUse(pageNo, false);
    this.dirty.delete(pageNo);
    this.freeHint = Math.min(this.freeHint, pageNo);
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
      if (this.hasChanges()) this.pager.append(this.dirty, this.pageCount);
    } finally {
      this.dirty.clear();
      this.end();
    }
  }

  /** Drops the changes and ends the transaction. */
  rollback(): void {
    this.ensureOpen();
    this.dirty.clear();
    this.end();
  }

  /** Writers are serialized by a lock: another commit means a bug, not a race. */
  private ensureNoOtherCommit(): void {
    if (this.pager.latestCommit() !== this.snapshot) {
      throw new InternalError(
        "another writer committed during this transaction",
      );
    }
  }

  private hasChanges(): boolean {
    return this.dirty.size > 0 || this.pageCount !== this.initialPageCount;
  }

  /** Adds a page at the end of the file. */
  private extend(): number {
    const pageNo = this.pageCount;
    this.pageCount = pageNo + 1;
    const isGroupStart = pageNo % groupSize(this.pageSize) === 0;
    if (isGroupStart) this.addMapPage(pageNo + 1);
    return pageNo;
  }

  /** A new group's allocation map, which follows the group's first page. */
  private addMapPage(mapPageNo: number): void {
    this.dirty.set(mapPageNo, newMapPage(this.pageSize, mapPageNo));
    this.pageCount = mapPageNo + 1;
  }
}

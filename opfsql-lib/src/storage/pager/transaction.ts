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
    let page = this.dirty.get(pageNo);
    if (page === undefined) {
      page = new Uint8Array(super.read(pageNo));
      this.dirty.set(pageNo, page);
    }
    return page;
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
      if (this.pager.latestCommit() !== this.snapshot) {
        throw new InternalError(
          "another writer committed during this transaction",
        );
      }
      if (this.dirty.size > 0 || this.pageCount !== this.initialPageCount) {
        this.pager.append(this.dirty, this.pageCount);
      }
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

  /** Adds a page at the end, and the allocation map of a new group with it. */
  private extend(): number {
    const pageNo = this.pageCount;
    if (pageNo % groupSize(this.pageSize) === 0) {
      this.dirty.set(pageNo + 1, newMapPage(this.pageSize, pageNo + 1));
      this.pageCount = pageNo + 2;
    } else {
      this.pageCount = pageNo + 1;
    }
    return pageNo;
  }
}

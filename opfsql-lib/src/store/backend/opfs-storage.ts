import { StorageError } from "../errors.js";
import {
  checkFormatPrefix,
  FORMAT_PREFIX_SIZE,
  writeFormatPrefix,
} from "../file-format.js";
import { assertPagesFit, decodePage, type EncodedPage } from "../page-codec.js";
import type { SyncIPageStorage } from "../types.js";

interface FileSystemSyncAccessHandle {
  getSize(): number;
  read(buffer: Uint8Array, options?: { at?: number }): number;
  write(buffer: Uint8Array, options?: { at?: number }): number;
  truncate(size: number): void;
  flush(): void;
  close(): void;
}

declare global {
  interface FileSystemFileHandle {
    createSyncAccessHandle(options?: {
      mode?: "readwrite" | "read-only" | "readwrite-unsafe";
    }): Promise<FileSystemSyncAccessHandle>;
  }
}

// ---------------------------------------------------------------------------
// File format
// ---------------------------------------------------------------------------
//
// Page 0: header (PAGE_SIZE bytes, only the first 20 used)
//   [0..11]  format prefix: magic "OPFSQLDB" + format version (file-format.ts)
//   [12..15] pageSize   u32 BE
//   [16..19] nextPageId u32 BE
//
// Page 1: catalog  (CBOR-encoded CatalogData)
// Page 2: freelist (CBOR-encoded number[])
// Pages 3..N: data pages (B-tree nodes)
//
// Each data page (PAGE_SIZE bytes at offset pageNo * PAGE_SIZE):
//   [0..3]   payload length u32 big-endian
//   [4..]    CBOR bytes
//
// File size = nextPageId * pageSize (no trailing index blob).
// ---------------------------------------------------------------------------

const PAGE_SIZE_OFFSET = FORMAT_PREFIX_SIZE;
const NEXT_PAGE_ID_OFFSET = FORMAT_PREFIX_SIZE + 4;
const HEADER_SIZE = FORMAT_PREFIX_SIZE + 8;
const PAGE_LENGTH_PREFIX = 4;
const DEFAULT_PAGE_SIZE = 32768; // 32 KB
const MIN_PAGE_SIZE = 4096;
const MAX_PAGE_SIZE = 1048576;

export interface OPFSStorageOptions {
  /** Page size for newly created files. Existing files keep the size in their header. */
  pageSize?: number;
}

function isValidPageSize(size: number): boolean {
  return (
    size >= MIN_PAGE_SIZE && size <= MAX_PAGE_SIZE && (size & (size - 1)) === 0
  );
}

export class OPFSSyncStorage implements SyncIPageStorage {
  private handle!: FileSystemSyncAccessHandle;
  private pageSize: number;
  private nextPageId = 3; // pages 0-2 reserved

  constructor(
    private readonly dbName: string,
    options: OPFSStorageOptions = {},
  ) {
    this.pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
    if (!isValidPageSize(this.pageSize)) {
      throw new Error(
        `Invalid pageSize ${this.pageSize}: must be a power of two between ` +
          `${MIN_PAGE_SIZE} and ${MAX_PAGE_SIZE}`,
      );
    }
  }

  get pageCapacity(): number {
    return this.pageSize - PAGE_LENGTH_PREFIX;
  }

  async open(): Promise<void> {
    if (this.handle) return; // already opened
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle(this.fileName, {
      create: true,
    });
    this.handle = await fh.createSyncAccessHandle({
      mode: "readwrite-unsafe",
    });
    if (this.handle.getSize() === 0) this.initNewFile();
    else this.readHeader();
  }

  close(): void {
    this.handle?.close();
    this.handle = undefined!;
  }

  readPage<T>(pageNo: number): T | null {
    const offset = pageNo * this.pageSize;
    if (offset + 4 > this.handle.getSize()) return null;
    const lenBuf = new Uint8Array(4);
    this.handle.read(lenBuf, { at: offset });
    const length = new DataView(lenBuf.buffer).getUint32(0, false);
    if (length === 0) return null;
    const data = new Uint8Array(length);
    this.handle.read(data, { at: offset + PAGE_LENGTH_PREFIX });
    return decodePage<T>(data);
  }

  getNextPageId(): number {
    const buf = new Uint8Array(4);
    this.handle.read(buf, { at: NEXT_PAGE_ID_OFFSET });
    this.nextPageId = new DataView(buf.buffer).getUint32(0, false);
    return this.nextPageId;
  }

  /**
   * Writes pages in place, then the header. Every page is size-checked before
   * anything is written, but the writes themselves are not atomic — crash
   * safety comes from WalStorage, which is the only caller in production.
   */
  commit(pages: ReadonlyMap<number, EncodedPage>, nextPageId: number): void {
    assertPagesFit(pages, this.pageCapacity);
    for (const [pageNo, page] of pages) this.writePage(pageNo, page.bytes);
    this.writeHeader(nextPageId);
    this.handle.flush();
  }

  /** Shrink file to match nextPageId. Safe only after commit(). */
  truncateToSize(): void {
    this.handle.truncate(this.nextPageId * this.pageSize);
  }

  // ---------------------------------------------------------------------------

  private writePage(pageNo: number, bytes: Uint8Array): void {
    const buf = new Uint8Array(PAGE_LENGTH_PREFIX + bytes.length);
    new DataView(buf.buffer).setUint32(0, bytes.length, false);
    buf.set(bytes, PAGE_LENGTH_PREFIX);
    this.handle.write(buf, { at: pageNo * this.pageSize });
  }

  private writeHeader(nextPageId: number): void {
    this.nextPageId = nextPageId;
    const buf = new Uint8Array(HEADER_SIZE);
    const v = new DataView(buf.buffer);
    writeFormatPrefix(buf, "database");
    v.setUint32(PAGE_SIZE_OFFSET, this.pageSize, false);
    v.setUint32(NEXT_PAGE_ID_OFFSET, this.nextPageId, false);
    this.handle.write(buf, { at: 0 });
  }

  private initNewFile(): void {
    this.handle.truncate(3 * this.pageSize);
    this.writeHeader(3);
    this.handle.flush();
  }

  private readHeader(): void {
    const buf = new Uint8Array(HEADER_SIZE);
    this.handle.read(buf, { at: 0 });
    checkFormatPrefix(buf, "database", this.fileName);
    const v = new DataView(buf.buffer);
    const pageSize = v.getUint32(PAGE_SIZE_OFFSET, false);
    if (!isValidPageSize(pageSize)) {
      throw new StorageError(
        `"${this.fileName}" has an invalid page size ${pageSize}`,
      );
    }
    this.pageSize = pageSize;
    this.nextPageId = v.getUint32(NEXT_PAGE_ID_OFFSET, false);
  }

  private get fileName(): string {
    return `${this.dbName}.opfsql`;
  }
}

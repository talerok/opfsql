import { Encoder, decode } from "cbor-x";
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
    createSyncAccessHandle(): Promise<FileSystemSyncAccessHandle>;
  }
}

const encoder = new Encoder({ structuredClone: false, useRecords: true });

// ---------------------------------------------------------------------------
// File format (OPFSQL03)
// ---------------------------------------------------------------------------
//
// Page 0: header (PAGE_SIZE bytes, only first 16 used)
//   [0..7]   magic  "OPFSQL03"
//   [8..11]  pageSize  u32 big-endian
//   [12..15] nextPageId u32
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
//
// v03 change: index B-tree leaves store [...userKey, rowId] as unique keys
// (SQLite-style). The rowId lives in the key's trailing element; there is no
// separate parallel array. Leaves split normally regardless of duplicate skew.
// ---------------------------------------------------------------------------

const MAGIC = new Uint8Array([0x4f, 0x50, 0x46, 0x53, 0x51, 0x4c, 0x30, 0x33]); // "OPFSQL03"
const HEADER_SIZE = 16;
const DEFAULT_PAGE_SIZE = 32768; // 32 KB

export class OPFSSyncStorage implements SyncIPageStorage {
  private handle!: FileSystemSyncAccessHandle;
  private pageSize = DEFAULT_PAGE_SIZE;
  private nextPageId = 3; // pages 0-2 reserved

  constructor(private readonly dbName: string) {}

  async open(): Promise<void> {
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle(`${this.dbName}.opfsql`, {
      create: true,
    });
    console.log("[OPFSSyncStorage] acquiring sync handle…");
    this.handle = await fh.createSyncAccessHandle();
    console.log(
      "[OPFSSyncStorage] handle acquired, size=",
      this.handle.getSize(),
    );
    if (this.handle.getSize() === 0) this.initNewFile();
    else this.readHeader();
  }

  close(): void {
    this.handle.close();
  }

  readPage<T>(pageNo: number): T | null {
    const offset = pageNo * this.pageSize;
    if (offset + 4 > this.handle.getSize()) return null;
    const lenBuf = new Uint8Array(4);
    this.handle.read(lenBuf, { at: offset });
    const length = new DataView(lenBuf.buffer).getUint32(0, false);
    if (length === 0) return null;
    const data = new Uint8Array(length);
    this.handle.read(data, { at: offset + 4 });
    return decode(data) as T;
  }

  writePage(pageNo: number, value: unknown): void {
    const data = encoder.encode(value);
    if (data.length + 4 > this.pageSize) {
      throw new Error(
        `OPFSSyncStorage: value for page ${pageNo} is ${data.length} bytes, ` +
          `exceeds page capacity ${this.pageSize - 4}. Increase PAGE_SIZE.`,
      );
    }
    const buf = new Uint8Array(4 + data.length);
    new DataView(buf.buffer).setUint32(0, data.length, false);
    buf.set(data, 4);
    this.handle.write(buf, { at: pageNo * this.pageSize });
  }

  getNextPageId(): number {
    return this.nextPageId;
  }

  writeHeader(nextPageId: number): void {
    this.nextPageId = nextPageId;
    const buf = new Uint8Array(HEADER_SIZE);
    const v = new DataView(buf.buffer);
    buf.set(MAGIC, 0);
    v.setUint32(8, this.pageSize, false);
    v.setUint32(12, this.nextPageId, false);
    this.handle.write(buf, { at: 0 });
  }

  /** Shrink file to match nextPageId. Safe only after flush(). */
  truncateToSize(): void {
    this.handle.truncate(this.nextPageId * this.pageSize);
  }

  flush(): void {
    this.handle.flush();
  }

  // ---------------------------------------------------------------------------

  private initNewFile(): void {
    this.nextPageId = 3;
    this.handle.truncate(3 * this.pageSize);
    const buf = new Uint8Array(HEADER_SIZE);
    const v = new DataView(buf.buffer);
    buf.set(MAGIC, 0);
    v.setUint32(8, this.pageSize, false);
    v.setUint32(12, this.nextPageId, false);
    this.handle.write(buf, { at: 0 });
    this.handle.flush();
  }

  private readHeader(): void {
    const buf = new Uint8Array(HEADER_SIZE);
    this.handle.read(buf, { at: 0 });
    for (let i = 0; i < MAGIC.length; i++) {
      if (buf[i] !== MAGIC[i])
        throw new Error("Invalid OPFSQL file: bad magic");
    }
    const v = new DataView(buf.buffer);
    const pageSize = v.getUint32(8, false);
    if (
      pageSize < 4096 ||
      pageSize > 1048576 ||
      (pageSize & (pageSize - 1)) !== 0
    ) {
      throw new Error(`Invalid OPFSQL file: bad pageSize ${pageSize}`);
    }
    this.pageSize = pageSize;
    this.nextPageId = v.getUint32(12, false);
  }
}

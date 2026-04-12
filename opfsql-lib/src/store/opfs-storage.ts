import { Encoder, decode } from 'cbor-x';
import type { SyncIStorage } from './types.js';

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
// File format
// ---------------------------------------------------------------------------
//
// Page 0: header (always PAGE_SIZE bytes)
//   [0..7]   magic  "OPFSQL01"
//   [8..11]  pageSize  u32 big-endian
//   [12..15] nextPageId u32  (monotonic allocator, page 0 = header)
//   [16..19] freeListHead u32  (0 = empty)
//   [20..27] indexOffset u64  (byte offset of key→pageNo CBOR index)
//   [28..31] indexLength u32
//
// Pages 1..N  (each exactly PAGE_SIZE bytes at offset pageNo * PAGE_SIZE)
//   [0..3]   payload length u32
//   [4..]    CBOR bytes
//
// Free page: [0..3] next free page u32  (0 = end of freelist)
//
// Index: CBOR Record<string,number> stored at indexOffset.
//        On commit, written right after the last data page; file truncated.
// ---------------------------------------------------------------------------

const MAGIC = new Uint8Array([0x4f, 0x50, 0x46, 0x53, 0x51, 0x4c, 0x30, 0x31]); // "OPFSQL01"
const HEADER_FIELDS_SIZE = 32;
const DEFAULT_PAGE_SIZE = 32768; // 32 KB

export class OPFSSyncStorage implements SyncIStorage {
  private handle!: FileSystemSyncAccessHandle;
  private pageSize = DEFAULT_PAGE_SIZE;
  private nextPageId = 1;
  private freeListHead = 0;
  private indexOffset = 0;
  private indexLength = 0;
  private index = new Map<string, number>();

  constructor(private readonly dbName: string) {}

  async open(): Promise<void> {
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle(`${this.dbName}.opfsql`, { create: true });
    console.log('[OPFSSyncStorage] acquiring sync handle…');
    this.handle = await fh.createSyncAccessHandle();
    console.log('[OPFSSyncStorage] handle acquired, size=', this.handle.getSize());
    if (this.handle.getSize() === 0) this.initNewFile();
    else { this.readHeader(); this.readIndex(); }
  }

  close(): void { this.handle.close(); }

  get<T>(key: string): T | null {
    const pageNo = this.index.get(key);
    if (pageNo === undefined) return null;
    return this.readPageData<T>(pageNo);
  }

  getAllKeys(prefix: string): string[] {
    const result: string[] = [];
    for (const k of this.index.keys()) if (k.startsWith(prefix)) result.push(k);
    return result.sort();
  }

  putMany(entries: Array<[string, unknown]>): void {
    for (const [key, value] of entries) {
      if (value === null) this.freePage(key);
      else this.writePageData(key, value);
    }
    this.persistIndex();
    this.writeHeader();
    this.handle.flush();
  }

  // ---------------------------------------------------------------------------

  private initNewFile(): void {
    this.handle.write(new Uint8Array(DEFAULT_PAGE_SIZE), { at: 0 });
    this.writeHeader();
    this.handle.flush();
  }

  private readHeader(): void {
    const buf = new Uint8Array(HEADER_FIELDS_SIZE);
    this.handle.read(buf, { at: 0 });
    const v = new DataView(buf.buffer);
    this.pageSize      = v.getUint32(8,  false);
    this.nextPageId    = v.getUint32(12, false);
    this.freeListHead  = v.getUint32(16, false);
    this.indexOffset   = v.getUint32(20, false) * 0x1_0000_0000 + v.getUint32(24, false);
    this.indexLength   = v.getUint32(28, false);
  }

  private writeHeader(): void {
    const buf = new Uint8Array(HEADER_FIELDS_SIZE);
    const v = new DataView(buf.buffer);
    buf.set(MAGIC, 0);
    v.setUint32(8,  this.pageSize,     false);
    v.setUint32(12, this.nextPageId,   false);
    v.setUint32(16, this.freeListHead, false);
    v.setUint32(20, Math.floor(this.indexOffset / 0x1_0000_0000), false);
    v.setUint32(24, this.indexOffset >>> 0, false);
    v.setUint32(28, this.indexLength,  false);
    this.handle.write(buf, { at: 0 });
  }

  private readIndex(): void {
    if (this.indexLength === 0) return;
    const buf = new Uint8Array(this.indexLength);
    this.handle.read(buf, { at: this.indexOffset });
    const obj = decode(buf) as Record<string, number>;
    this.index = new Map(Object.entries(obj));
  }

  private persistIndex(): void {
    const buf = encoder.encode(Object.fromEntries(this.index));
    this.indexOffset = this.nextPageId * this.pageSize;
    this.indexLength = buf.length;
    this.handle.write(buf, { at: this.indexOffset });
    this.handle.truncate(this.indexOffset + this.indexLength);
  }

  private readPageData<T>(pageNo: number): T | null {
    const offset = pageNo * this.pageSize;
    const lenBuf = new Uint8Array(4);
    this.handle.read(lenBuf, { at: offset });
    const length = new DataView(lenBuf.buffer).getUint32(0, false);
    if (length === 0) return null;
    const data = new Uint8Array(length);
    this.handle.read(data, { at: offset + 4 });
    return decode(data) as T;
  }

  private writePageData(key: string, value: unknown): void {
    let pageNo = this.index.get(key);
    if (pageNo === undefined) { pageNo = this.allocPage(); this.index.set(key, pageNo); }
    const data = encoder.encode(value);
    if (data.length + 4 > this.pageSize) {
      throw new Error(
        `OPFSSyncStorage: value for "${key}" is ${data.length} bytes, ` +
        `exceeds page capacity ${this.pageSize - 4}. Reduce row size or increase PAGE_SIZE.`,
      );
    }
    const lenBuf = new Uint8Array(4);
    new DataView(lenBuf.buffer).setUint32(0, data.length, false);
    this.handle.write(lenBuf, { at: pageNo * this.pageSize });
    this.handle.write(data,   { at: pageNo * this.pageSize + 4 });
  }

  private freePage(key: string): void {
    const pageNo = this.index.get(key);
    if (pageNo === undefined) return;
    this.index.delete(key);
    const buf = new Uint8Array(4);
    new DataView(buf.buffer).setUint32(0, this.freeListHead, false);
    this.handle.write(buf, { at: pageNo * this.pageSize });
    this.freeListHead = pageNo;
  }

  private allocPage(): number {
    if (this.freeListHead !== 0) {
      const pageNo = this.freeListHead;
      const buf = new Uint8Array(4);
      this.handle.read(buf, { at: pageNo * this.pageSize });
      this.freeListHead = new DataView(buf.buffer).getUint32(0, false);
      return pageNo;
    }
    return this.nextPageId++;
  }
}

import * as fs from "node:fs";
import * as path from "node:path";
import { Encoder, decode } from "cbor-x";
import type { ISyncFileHandle } from "../wal/file-handle.js";
import type { SyncIPageStorage } from "../types.js";

const encoder = new Encoder({ structuredClone: false, useRecords: true });

const MAGIC = new Uint8Array([0x4f, 0x50, 0x46, 0x53, 0x51, 0x4c, 0x30, 0x33]); // "OPFSQL03"
const HEADER_SIZE = 16;
const DEFAULT_PAGE_SIZE = 32768; // 32 KB

// ---------------------------------------------------------------------------
// Node.js ISyncFileHandle implementation
// ---------------------------------------------------------------------------

export class NodeFileHandle implements ISyncFileHandle {
  private fd: number;

  constructor(filePath: string) {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    this.fd = fs.openSync(filePath, fs.existsSync(filePath) ? "r+" : "w+");
  }

  read(buffer: Uint8Array, options?: { at?: number }): number {
    return fs.readSync(this.fd, buffer, 0, buffer.length, options?.at ?? 0);
  }

  write(buffer: Uint8Array, options?: { at?: number }): number {
    return fs.writeSync(this.fd, buffer, 0, buffer.length, options?.at ?? 0);
  }

  getSize(): number {
    return fs.fstatSync(this.fd).size;
  }

  truncate(size: number): void {
    fs.ftruncateSync(this.fd, size);
  }

  flush(): void {
    fs.fdatasyncSync(this.fd);
  }

  close(): void {
    fs.closeSync(this.fd);
  }
}

// ---------------------------------------------------------------------------
// Node.js SyncIPageStorage implementation
// ---------------------------------------------------------------------------

export class NodeSyncStorage implements SyncIPageStorage {
  private handle!: NodeFileHandle;
  private pageSize = DEFAULT_PAGE_SIZE;
  private nextPageId = 3;

  constructor(private readonly filePath: string) {}

  async open(): Promise<void> {
    this.handle = new NodeFileHandle(this.filePath);
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
        `NodeSyncStorage: value for page ${pageNo} is ${data.length} bytes, ` +
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

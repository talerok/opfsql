/**
 * Abstraction over FileSystemSyncAccessHandle for testability.
 * In OPFS workers, the real handle is used directly.
 * In tests, MemoryFileHandle provides an ArrayBuffer-backed implementation.
 */
export interface ISyncFileHandle {
  read(buffer: Uint8Array, options?: { at?: number }): number;
  write(buffer: Uint8Array, options?: { at?: number }): number;
  getSize(): number;
  truncate(size: number): void;
  flush(): void;
  close(): void;
}

export class MemoryFileHandle implements ISyncFileHandle {
  private buf: Uint8Array;
  private size = 0;

  constructor(initialCapacity = 4096) {
    this.buf = new Uint8Array(initialCapacity);
  }

  read(buffer: Uint8Array, options?: { at?: number }): number {
    const at = options?.at ?? 0;
    const readable = Math.max(0, Math.min(buffer.length, this.size - at));
    if (readable > 0) {
      buffer.set(this.buf.subarray(at, at + readable));
    }
    return readable;
  }

  write(buffer: Uint8Array, options?: { at?: number }): number {
    const at = options?.at ?? 0;
    const end = at + buffer.length;
    if (end > this.buf.length) {
      const next = new Uint8Array(Math.max(this.buf.length * 2, end));
      next.set(this.buf);
      this.buf = next;
    }
    this.buf.set(buffer, at);
    if (end > this.size) this.size = end;
    return buffer.length;
  }

  getSize(): number {
    return this.size;
  }

  truncate(size: number): void {
    if (size < this.size) {
      this.buf.fill(0, size, this.size);
    }
    this.size = size;
  }

  flush(): void {
    // no-op for memory
  }

  close(): void {
    // no-op for memory
  }

  /** Test helper: return a copy of the underlying data. */
  snapshot(): Uint8Array {
    return this.buf.slice(0, this.size);
  }
}

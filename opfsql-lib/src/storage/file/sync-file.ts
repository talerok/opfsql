// ---------------------------------------------------------------------------
// The file operations opfsql needs: the synchronous OPFS access handle
// (FileSystemSyncAccessHandle) in a worker, a byte array in tests.
// ---------------------------------------------------------------------------

export interface SyncFile {
  read(buffer: Uint8Array, options: { at: number }): number;
  write(buffer: Uint8Array, options: { at: number }): number;
  getSize(): number;
  truncate(size: number): void;
  flush(): void;
  close(): void;
}

/** A file in memory, for tests. */
export class MemoryFile implements SyncFile {
  private bytes = new Uint8Array(4096);
  private size = 0;

  read(buffer: Uint8Array, options: { at: number }): number {
    const available = Math.max(0, this.size - options.at);
    const readable = Math.min(buffer.length, available);
    buffer.set(this.bytes.subarray(options.at, options.at + readable));
    return readable;
  }

  write(buffer: Uint8Array, options: { at: number }): number {
    const end = options.at + buffer.length;
    this.reserve(end);
    this.bytes.set(buffer, options.at);
    this.size = Math.max(this.size, end);
    return buffer.length;
  }

  getSize(): number {
    return this.size;
  }

  /** Bytes past the end are kept zero, so growing the file adds zeros. */
  truncate(size: number): void {
    this.reserve(size);
    this.bytes.fill(0, size);
    this.size = size;
  }

  flush(): void {
    // Nothing to persist.
  }

  close(): void {
    // Nothing to release.
  }

  private reserve(capacity: number): void {
    if (capacity <= this.bytes.length) return;
    const grown = new Uint8Array(Math.max(this.bytes.length * 2, capacity));
    grown.set(this.bytes);
    this.bytes = grown;
  }
}

// The file operations opfsql needs: the synchronous OPFS access handle
// (FileSystemSyncAccessHandle) in a worker, a byte array in tests.

export interface SyncFile {
  read(buffer: Uint8Array, options: { at: number }): number;
  write(buffer: Uint8Array, options: { at: number }): number;
  getSize(): number;
  truncate(size: number): void;
  flush(): void;
  close(): void;
}

/** A file in memory, for tests and for databases that need no persistence. */
export class MemoryFile implements SyncFile {
  private bytes = new Uint8Array(4096);
  private size = 0;

  read(buffer: Uint8Array, options: { at: number }): number {
    const readable = Math.max(
      0,
      Math.min(buffer.length, this.size - options.at),
    );
    buffer.set(this.bytes.subarray(options.at, options.at + readable));
    return readable;
  }

  write(buffer: Uint8Array, options: { at: number }): number {
    const end = options.at + buffer.length;
    if (end > this.bytes.length) {
      const grown = new Uint8Array(Math.max(this.bytes.length * 2, end));
      grown.set(this.bytes);
      this.bytes = grown;
    }
    this.bytes.set(buffer, options.at);
    this.size = Math.max(this.size, end);
    return buffer.length;
  }

  getSize(): number {
    return this.size;
  }

  truncate(size: number): void {
    if (size > this.bytes.length) {
      this.write(new Uint8Array(size - this.size), { at: this.size });
    } else {
      this.bytes.fill(0, size);
    }
    this.size = size;
  }

  flush(): void {
    // Nothing to persist.
  }

  close(): void {
    // Nothing to release.
  }
}

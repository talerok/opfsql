// The synchronous OPFS access handle exists only in dedicated workers, so
// TypeScript's DOM library does not declare it.

interface SyncAccessHandle {
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
    }): Promise<SyncAccessHandle>;
  }
}

export type { SyncAccessHandle };

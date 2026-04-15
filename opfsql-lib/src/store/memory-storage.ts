import type { SyncIPageStorage } from "./types.js";

/**
 * Test-only in-memory SyncIPageStorage. Uses structuredClone to mimic real storage
 * (no shared references between stored and returned values).
 */
export class MemoryPageStorage implements SyncIPageStorage {
  private pages = new Map<number, unknown>();
  private nextPageId = 3; // 0=header, 1=catalog, 2=freelist

  async open(): Promise<void> {}
  close(): void {}

  readPage<T>(pageNo: number): T | null {
    const val = this.pages.get(pageNo);
    if (val === undefined) return null;
    return structuredClone(val) as T;
  }

  writePage(pageNo: number, value: unknown): void {
    this.pages.set(pageNo, structuredClone(value));
  }

  getNextPageId(): number { return this.nextPageId; }

  writeHeader(nextPageId: number): void {
    this.nextPageId = nextPageId;
  }

  flush(): void {}
}

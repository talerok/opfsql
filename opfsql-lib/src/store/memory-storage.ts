import type { SyncIStorage } from "./types.js";

/**
 * Test-only in-memory SyncIStorage. Uses structuredClone to mimic real storage
 * (no shared references between stored and returned values).
 */
export class MemoryStorage implements SyncIStorage {
  private data = new Map<string, unknown>();

  async open(): Promise<void> {}
  close(): void {}

  get<T>(key: string): T | null {
    const val = this.data.get(key);
    if (val === undefined) return null;
    return structuredClone(val) as T;
  }

  putMany(entries: Array<[string, unknown]>): void {
    for (const [key, value] of entries) {
      if (value === null) {
        this.data.delete(key);
      } else {
        this.data.set(key, structuredClone(value));
      }
    }
  }

  getAllKeys(prefix: string): string[] {
    const keys: string[] = [];
    for (const key of this.data.keys()) {
      if (key.startsWith(prefix)) keys.push(key);
    }
    return keys.sort();
  }
}

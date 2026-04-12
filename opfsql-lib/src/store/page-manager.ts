import type { SyncIKVStore, SyncIStorage } from './types.js';
import type { ICache } from './cache.js';
import { LRUCache } from './cache.js';

const DEFAULT_CACHE_SIZE = 256;

export class SyncPageManager implements SyncIKVStore {
  private wal = new Map<string, unknown>();
  private cache: ICache<string, unknown>;

  constructor(
    private readonly storage: SyncIStorage,
    cacheSize = DEFAULT_CACHE_SIZE,
  ) {
    this.cache = new LRUCache(cacheSize);
  }

  readKey<T>(key: string): T | null {
    if (this.wal.has(key)) {
      const val = this.wal.get(key);
      return val === null ? null : (val as T);
    }
    const cached = this.cache.get(key);
    if (cached !== undefined) return cached as T;
    const val = this.storage.get<T>(key);
    if (val !== null && val !== undefined) this.cache.set(key, val);
    return val;
  }

  getAllKeys(prefix: string): string[] {
    const storageKeys = new Set(this.storage.getAllKeys(prefix));
    for (const [k, v] of this.wal) {
      if (!k.startsWith(prefix)) continue;
      if (v === null) storageKeys.delete(k);
      else storageKeys.add(k);
    }
    return [...storageKeys].sort();
  }

  writeKey(key: string, value: unknown): void {
    this.wal.set(key, value);
  }

  deleteKey(key: string): void {
    this.wal.set(key, null);
  }

  commit(): void {
    if (this.wal.size === 0) return;
    const entries: Array<[string, unknown]> = [...this.wal.entries()];
    this.storage.putMany(entries);
    for (const [key, value] of entries) {
      if (value === null) this.cache.delete(key);
      else this.cache.set(key, value);
    }
    this.wal.clear();
  }

  rollback(): void {
    this.wal.clear();
  }
}

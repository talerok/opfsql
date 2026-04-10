import type { IStorage } from '../types.js';

export class MemoryStorage implements IStorage {
  private data = new Map<string, unknown>();

  async open(): Promise<void> {}
  close(): void {}

  async get<T>(key: string): Promise<T | null> {
    const val = this.data.get(key);
    return val === undefined ? null : (val as T);
  }

  async put(key: string, value: unknown): Promise<void> {
    this.data.set(key, value);
  }

  async delete(key: string): Promise<void> {
    this.data.delete(key);
  }

  async putMany(entries: Array<[string, unknown]>): Promise<void> {
    for (const [key, value] of entries) {
      if (value === null) {
        this.data.delete(key);
      } else {
        this.data.set(key, value);
      }
    }
  }

  async getAllKeys(prefix: string): Promise<string[]> {
    const keys: string[] = [];
    for (const key of this.data.keys()) {
      if (key.startsWith(prefix)) keys.push(key);
    }
    return keys.sort();
  }
}

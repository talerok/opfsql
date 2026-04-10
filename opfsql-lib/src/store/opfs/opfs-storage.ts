import { Encoder, decode } from 'cbor-x';
import type { IStorage } from '../types.js';

const encoder = new Encoder({ structuredClone: false, useRecords: true });

export class OPFSStorage implements IStorage {
  private dir!: FileSystemDirectoryHandle;

  constructor(private readonly dbName: string) {}

  async open(): Promise<void> {
    const root = await navigator.storage.getDirectory();
    this.dir = await root.getDirectoryHandle(this.dbName, { create: true });
  }

  close(): void {}

  async get<T>(key: string): Promise<T | null> {
    try {
      const fh = await this.dir.getFileHandle(this.encodeKey(key));
      const file = await fh.getFile();
      if (file.size === 0) return null;
      return decode(new Uint8Array(await file.arrayBuffer())) as T;
    } catch (e) {
      if (e instanceof DOMException && e.name === 'NotFoundError') return null;
      throw e;
    }
  }

  async put(key: string, value: unknown): Promise<void> {
    const fh = await this.dir.getFileHandle(this.encodeKey(key), { create: true });
    const w = await fh.createWritable();
    await w.write(new Uint8Array(encoder.encode(value)));
    await w.close();
  }

  async delete(key: string): Promise<void> {
    try {
      await this.dir.removeEntry(this.encodeKey(key));
    } catch (e) {
      if (!(e instanceof DOMException && e.name === 'NotFoundError')) throw e;
    }
  }

  async putMany(entries: Array<[string, unknown]>): Promise<void> {
    await Promise.all(
      entries.map(([k, v]) => (v === null ? this.delete(k) : this.put(k, v))),
    );
  }

  async getAllKeys(prefix: string): Promise<string[]> {
    const ep = this.encodeKey(prefix);
    const keys: string[] = [];
    for await (const name of (this.dir as any).keys()) {
      if (name.startsWith(ep)) keys.push(this.decodeKey(name));
    }
    return keys.sort();
  }

  private encodeKey(key: string): string {
    return encodeURIComponent(key);
  }

  private decodeKey(name: string): string {
    return decodeURIComponent(name);
  }
}

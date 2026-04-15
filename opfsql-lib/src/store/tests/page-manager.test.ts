import { describe, it, expect, beforeEach } from 'vitest';
import { SyncPageStore } from '../page-manager.js';
import { MemoryPageStorage } from '../memory-storage.js';

function createStore(storage?: MemoryPageStorage, cacheSize?: number): SyncPageStore {
  const s = storage ?? new MemoryPageStorage();
  const nextPageId = s.getNextPageId();
  const freeList = s.readPage<number[]>(2) ?? [];
  return new SyncPageStore(s, nextPageId, freeList, cacheSize);
}

describe('SyncPageStore (page-based)', () => {
  let storage: MemoryPageStorage;
  let ps: SyncPageStore;

  beforeEach(() => {
    storage = new MemoryPageStorage();
    ps = createStore(storage);
  });

  describe('readPage / writePage', () => {
    it('writePage → readPage returns value from WAL', () => {
      ps.writePage(10, { data: 42 });
      expect(ps.readPage<{ data: number }>(10)).toEqual({ data: 42 });
    });

    it('readPage returns null for missing page', () => {
      expect(ps.readPage(99)).toBeNull();
    });

    it('writePage → commit persists to storage', () => {
      ps.writePage(10, { data: 42 });
      ps.commit();
      expect(storage.readPage<{ data: number }>(10)).toEqual({ data: 42 });
    });
  });

  describe('allocPage', () => {
    it('allocates sequential page numbers starting from 3', () => {
      const p1 = ps.allocPage();
      const p2 = ps.allocPage();
      const p3 = ps.allocPage();
      expect(p1).toBe(3);
      expect(p2).toBe(4);
      expect(p3).toBe(5);
    });

    it('reuses freed pages', () => {
      const p1 = ps.allocPage();
      ps.freePage(p1);
      const p2 = ps.allocPage();
      expect(p2).toBe(p1);
    });
  });

  describe('commit', () => {
    it('flushes WAL to storage', () => {
      ps.writePage(10, 1);
      ps.writePage(11, 2);
      ps.commit();
      expect(storage.readPage(10)).toBe(1);
      expect(storage.readPage(11)).toBe(2);
    });

    it('noop when WAL is empty', () => {
      ps.commit(); // should not throw
    });

    it('persists nextPageId across reload', () => {
      ps.allocPage(); // 3
      ps.allocPage(); // 4
      ps.commit();

      const ps2 = createStore(storage);
      const p = ps2.allocPage();
      expect(p).toBe(5);
    });

    it('persists freelist across reload', () => {
      const p1 = ps.allocPage();
      const p2 = ps.allocPage();
      ps.freePage(p1);
      ps.commit();

      const ps2 = createStore(storage);
      const reused = ps2.allocPage();
      expect(reused).toBe(p1);
    });
  });

  describe('rollback', () => {
    it('discards uncommitted writes', () => {
      ps.writePage(10, 'val');
      ps.rollback();
      expect(ps.readPage(10)).toBeNull();
    });

    it('does not affect committed data', () => {
      ps.writePage(10, 'committed');
      ps.commit();

      ps.writePage(10, 'uncommitted');
      ps.rollback();

      expect(ps.readPage<string>(10)).toBe('committed');
    });

    it('restores nextPageId', () => {
      ps.allocPage(); // 3
      ps.commit();

      ps.allocPage(); // 4
      ps.allocPage(); // 5
      ps.rollback();

      const p = ps.allocPage();
      expect(p).toBe(4);
    });

    it('restores freeList', () => {
      const p = ps.allocPage(); // 3
      ps.commit();

      ps.freePage(p);
      ps.rollback();

      // p was not actually freed, so next alloc should be 4
      const next = ps.allocPage();
      expect(next).toBe(4);
    });
  });

  describe('LRU cache', () => {
    it('caches reads from storage', () => {
      storage.writePage(10, { data: 1 });
      expect(ps.readPage<{ data: number }>(10)).toEqual({ data: 1 });

      // Delete from storage directly — cache should still serve
      storage.writePage(10, null as any);
      expect(ps.readPage<{ data: number }>(10)).toEqual({ data: 1 });
    });

    it('commit promotes WAL to cache', () => {
      ps.writePage(10, 'v');
      ps.commit();

      storage.writePage(10, null as any);
      expect(ps.readPage<string>(10)).toBe('v');
    });

    it('evicts oldest entries', () => {
      const ps2 = createStore(storage, 2); // capacity=2
      storage.writePage(10, 1);
      storage.writePage(11, 2);
      storage.writePage(12, 3);

      ps2.readPage(10); // cached
      ps2.readPage(11); // cached, evicts nothing yet
      ps2.readPage(12); // cached, evicts 10 (capacity=2)

      // 10 was evicted from cache, now delete from storage too
      storage.writePage(10, null as any);
      expect(ps2.readPage(10)).toBeNull(); // not in cache, not in storage
    });
  });
});

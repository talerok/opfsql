import { resetMockOPFS } from 'opfs-mock';
import { describe, it, expect, beforeEach } from 'vitest';
import { Storage } from '../storage.js';
import type { SessionStore } from '../session-store.js';
import { OPFSSyncStorage } from '../backend/opfs-storage.js';

let seq = 0;

async function createStoreAndSession(backend: OPFSSyncStorage): Promise<{ storage: Storage; ss: SessionStore }> {
  const storage = new Storage(backend);
  await storage.open();
  return { storage, ss: storage.createSession() };
}

describe('Storage + SessionStore', () => {
  let backend: OPFSSyncStorage;
  let store: Storage;
  let ss: SessionStore;

  beforeEach(async () => {
    resetMockOPFS();
    backend = new OPFSSyncStorage(`pm-test-${seq++}`);
    ({ storage: store, ss } = await createStoreAndSession(backend));
  });

  describe('readPage / writePage', () => {
    it('writePage → readPage returns value from buffer', () => {
      ss.writePage(10, { data: 42 });
      expect(ss.readPage<{ data: number }>(10)).toEqual({ data: 42 });
    });

    it('readPage returns null for missing page', () => {
      expect(ss.readPage(99)).toBeNull();
    });

    it('writePage → commit persists to backend', () => {
      ss.writePage(10, { data: 42 });
      ss.commit();
      expect(backend.readPage<{ data: number }>(10)).toEqual({ data: 42 });
    });
  });

  describe('allocPage', () => {
    it('allocates sequential page numbers starting from 3', () => {
      const p1 = ss.allocPage();
      const p2 = ss.allocPage();
      const p3 = ss.allocPage();
      expect(p1).toBe(3);
      expect(p2).toBe(4);
      expect(p3).toBe(5);
    });

    it('reuses freed pages', () => {
      const p1 = ss.allocPage();
      ss.freePage(p1);
      const p2 = ss.allocPage();
      expect(p2).toBe(p1);
    });
  });

  describe('commit', () => {
    it('flushes to backend', () => {
      ss.writePage(10, 1);
      ss.writePage(11, 2);
      ss.commit();
      expect(backend.readPage(10)).toBe(1);
      expect(backend.readPage(11)).toBe(2);
    });

    it('noop when buffer is empty', () => {
      ss.commit(); // should not throw
    });

    it('persists nextPageId across reload', async () => {
      ss.allocPage(); // 3
      ss.allocPage(); // 4
      ss.commit();

      const { ss: ss2 } = await createStoreAndSession(backend);
      const p = ss2.allocPage();
      expect(p).toBe(5);
    });

    it('persists freelist across reload', async () => {
      const p1 = ss.allocPage();
      const p2 = ss.allocPage();
      ss.freePage(p1);
      ss.commit();

      const { ss: ss2 } = await createStoreAndSession(backend);
      const reused = ss2.allocPage();
      expect(reused).toBe(p1);
    });
  });

  describe('rollback', () => {
    it('discards uncommitted writes', () => {
      ss.writePage(10, 'val');
      ss.rollback();
      expect(ss.readPage(10)).toBeNull();
    });

    it('does not affect committed data', () => {
      ss.writePage(10, 'committed');
      ss.commit();

      ss.writePage(10, 'uncommitted');
      ss.rollback();

      expect(ss.readPage<string>(10)).toBe('committed');
    });

    it('restores nextPageId', () => {
      ss.allocPage(); // 3
      ss.commit();

      ss.allocPage(); // 4
      ss.allocPage(); // 5
      ss.rollback();

      const p = ss.allocPage();
      expect(p).toBe(4);
    });

    it('restores freeList', () => {
      const p = ss.allocPage(); // 3
      ss.commit();

      ss.freePage(p);
      ss.rollback();

      // p was not actually freed, so next alloc should be 4
      const next = ss.allocPage();
      expect(next).toBe(4);
    });
  });

  describe('allocPage — freelist reuse', () => {
    it('allocPage from freelist allows overwriting page', () => {
      ss.writePage(10, { old: 'data' });
      ss.commit();
      expect(ss.readPage(10)).toEqual({ old: 'data' });

      ss.freePage(10);
      const reused = ss.allocPage();
      expect(reused).toBe(10);

      ss.writePage(10, { new: 'data' });
      ss.commit();
      expect(ss.readPage(10)).toEqual({ new: 'data' });
    });

    it('allocPage from freelist reads fresh data from backend', () => {
      backend.writePage(10, 'stale');
      expect(ss.readPage(10)).toBe('stale');

      ss.freePage(10);
      const reused = ss.allocPage();
      expect(reused).toBe(10);

      backend.writePage(10, 'fresh');
      expect(ss.readPage(10)).toBe('fresh');
    });
  });

  describe('allocatorDirty — commit optimization', () => {
    it('commit skips header write when only pages written', () => {
      const headerBefore = backend.getNextPageId();
      ss.writePage(100, 'data');
      ss.commit();
      expect(backend.getNextPageId()).toBe(headerBefore);
    });

    it('commit writes header when allocPage was called', () => {
      const headerBefore = backend.getNextPageId();
      ss.allocPage();
      ss.commit();
      expect(backend.getNextPageId()).toBe(headerBefore + 1);
    });

    it('commit writes freelist when freePage was called', async () => {
      const p = ss.allocPage();
      ss.commit();

      ss.freePage(p);
      ss.commit();

      const { ss: ss2 } = await createStoreAndSession(backend);
      const reused = ss2.allocPage();
      expect(reused).toBe(p);
    });
  });

});

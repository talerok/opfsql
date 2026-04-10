import { describe, it, expect, beforeEach } from 'vitest';
import { PageManager } from '../page-manager.js';
import { MemoryStorage } from './memory-storage.js';

describe('PageManager', () => {
  let storage: MemoryStorage;
  let pm: PageManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
  });

  describe('read defaults', () => {
    it('readRow returns null for missing page', async () => {
      expect(await pm.readRow('t1', { pageId: 0, slotId: 0 })).toBeNull();
    });

    it('getPageMeta returns defaults for missing table', async () => {
      const meta = await pm.getPageMeta('t1');
      expect(meta).toEqual({ lastPageId: -1, totalRowCount: 0, deadRowCount: 0 });
    });
  });

  describe('KV operations (writeKey/readKey)', () => {
    it('writeKey → readKey returns value from WAL', async () => {
      pm.writeKey('custom:key', { data: 42 });
      const val = await pm.readKey<{ data: number }>('custom:key');
      expect(val).toEqual({ data: 42 });
    });

    it('writeKey → commit persists to storage', async () => {
      pm.writeKey('custom:key', { data: 42 });
      await pm.commit();
      const val = await storage.get<{ data: number }>('custom:key');
      expect(val).toEqual({ data: 42 });
    });

    it('deleteKey → commit removes from storage', async () => {
      await storage.put('page:t1:000000', { pageId: 0, tableId: 't1', rows: [] });
      pm.deleteKey('page:t1:000000');
      await pm.commit();
      expect(await storage.get('page:t1:000000')).toBeNull();
    });
  });

  describe('commit', () => {
    it('flushes WAL to storage', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.commit();

      // Verify data is in storage
      const page = await storage.get<any>('page:t1:000000');
      expect(page).not.toBeNull();
      expect(page.rows).toHaveLength(1);
    });

    it('data survives in cache after commit', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.commit();

      // Read from cache (not storage)
      const row = await pm.readRow('t1', { pageId: 0, slotId: 0 });
      expect(row).toEqual({ id: 1 });
    });

    it('noop when WAL is empty', async () => {
      await pm.commit(); // should not throw
    });
  });

  describe('rollback', () => {
    it('discards uncommitted data', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      pm.rollback();
      expect(await pm.readRow('t1', { pageId: 0, slotId: 0 })).toBeNull();
    });

    it('does not affect committed data in cache', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.commit();

      await pm.prepareInsert('t1', { id: 2 });
      pm.rollback();

      // First row still accessible from cache
      expect(await pm.readRow('t1', { pageId: 0, slotId: 0 })).toEqual({ id: 1 });
    });
  });

  describe('getAllPageKeys', () => {
    it('returns keys from storage', async () => {
      await storage.put('page:t1:000000', {});
      await storage.put('page:t1:000001', {});
      await storage.put('page:t2:000000', {});
      const keys = await pm.getAllPageKeys('t1');
      expect(keys).toEqual(['page:t1:000000', 'page:t1:000001']);
    });
  });

  describe('deleteTableData', () => {
    it('removes all pages and meta for a table', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareInsert('t1', { id: 2 });
      await pm.commit();

      await pm.deleteTableData('t1');
      await pm.commit();

      // Fresh PM reads from storage
      const pm2 = new PageManager(storage);
      const meta = await pm2.getPageMeta('t1');
      expect(meta.lastPageId).toBe(-1);
    });
  });

  describe('getAllKeys', () => {
    it('merges WAL and storage keys', async () => {
      await storage.put('idx:test:a', 'val');
      pm.writeKey('idx:test:b', 'val');
      const keys = await pm.getAllKeys('idx:test:');
      expect(keys).toEqual(['idx:test:a', 'idx:test:b']);
    });

    it('WAL deletes remove storage keys', async () => {
      await storage.put('idx:test:a', 'val');
      pm.deleteKey('idx:test:a');
      const keys = await pm.getAllKeys('idx:test:');
      expect(keys).toEqual([]);
    });
  });

  describe('buffer pool', () => {
    it('readKey caches storage reads', async () => {
      await storage.put('some:key', { data: 1 });
      const val1 = await pm.readKey<{ data: number }>('some:key');
      expect(val1).toEqual({ data: 1 });

      // Delete from storage — should still be in cache
      await storage.delete('some:key');
      const val2 = await pm.readKey<{ data: number }>('some:key');
      expect(val2).toEqual({ data: 1 });
    });

    it('commit promotes WAL to cache', async () => {
      pm.writeKey('k', 'v');
      await pm.commit();

      // Delete from storage — cache should serve it
      await storage.delete('k');
      expect(await pm.readKey<string>('k')).toBe('v');
    });
  });
});

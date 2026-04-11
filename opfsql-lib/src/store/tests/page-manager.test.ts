import { describe, it, expect, beforeEach } from 'vitest';
import { PageManager } from '../page-manager.js';
import { MemoryStorage } from './memory-storage.js';

describe('PageManager (KV store)', () => {
  let storage: MemoryStorage;
  let pm: PageManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
  });

  describe('readKey / writeKey', () => {
    it('writeKey → readKey returns value from WAL', async () => {
      pm.writeKey('k', { data: 42 });
      expect(await pm.readKey<{ data: number }>('k')).toEqual({ data: 42 });
    });

    it('readKey returns null for missing key', async () => {
      expect(await pm.readKey('missing')).toBeNull();
    });

    it('writeKey → commit persists to storage', async () => {
      pm.writeKey('k', { data: 42 });
      await pm.commit();
      expect(await storage.get<{ data: number }>('k')).toEqual({ data: 42 });
    });

    it('deleteKey → commit removes from storage', async () => {
      await storage.put('k', 'val');
      pm.deleteKey('k');
      await pm.commit();
      expect(await storage.get('k')).toBeNull();
    });

    it('deleteKey → readKey returns null', async () => {
      await storage.put('k', 'val');
      pm.deleteKey('k');
      expect(await pm.readKey('k')).toBeNull();
    });
  });

  describe('commit', () => {
    it('flushes WAL to storage', async () => {
      pm.writeKey('a', 1);
      pm.writeKey('b', 2);
      await pm.commit();
      expect(await storage.get('a')).toBe(1);
      expect(await storage.get('b')).toBe(2);
    });

    it('noop when WAL is empty', async () => {
      await pm.commit(); // should not throw
    });
  });

  describe('rollback', () => {
    it('discards uncommitted writes', async () => {
      pm.writeKey('k', 'val');
      pm.rollback();
      expect(await pm.readKey('k')).toBeNull();
    });

    it('does not affect committed data', async () => {
      pm.writeKey('k', 'committed');
      await pm.commit();

      pm.writeKey('k', 'uncommitted');
      pm.rollback();

      expect(await pm.readKey<string>('k')).toBe('committed');
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

  describe('LRU cache', () => {
    it('caches reads from storage', async () => {
      await storage.put('k', { data: 1 });
      expect(await pm.readKey<{ data: number }>('k')).toEqual({ data: 1 });

      // Delete from storage directly — cache should still serve
      await storage.delete('k');
      expect(await pm.readKey<{ data: number }>('k')).toEqual({ data: 1 });
    });

    it('commit promotes WAL to cache', async () => {
      pm.writeKey('k', 'v');
      await pm.commit();

      await storage.delete('k');
      expect(await pm.readKey<string>('k')).toBe('v');
    });

    it('evicts oldest entries', async () => {
      const pm2 = new PageManager(storage, 2); // capacity=2
      await storage.put('a', 1);
      await storage.put('b', 2);
      await storage.put('c', 3);

      await pm2.readKey('a'); // cached
      await pm2.readKey('b'); // cached, evicts 'a'
      await pm2.readKey('c'); // cached, evicts 'a' (already evicted) or 'a' was already evicted

      // 'a' was first, should be evicted after reading b and c with capacity=2
      await storage.delete('a');
      expect(await pm2.readKey('a')).toBeNull(); // evicted from cache, deleted from storage
    });
  });
});

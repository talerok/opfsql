import { describe, it, expect, beforeEach } from 'vitest';
import { SyncPageManager } from '../page-manager.js';
import { MemoryStorage } from '../memory-storage.js';

describe('SyncPageManager (KV store)', () => {
  let storage: MemoryStorage;
  let pm: SyncPageManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new SyncPageManager(storage);
  });

  describe('readKey / writeKey', () => {
    it('writeKey → readKey returns value from WAL', () => {
      pm.writeKey('k', { data: 42 });
      expect(pm.readKey<{ data: number }>('k')).toEqual({ data: 42 });
    });

    it('readKey returns null for missing key', () => {
      expect(pm.readKey('missing')).toBeNull();
    });

    it('writeKey → commit persists to storage', () => {
      pm.writeKey('k', { data: 42 });
      pm.commit();
      expect(storage.get<{ data: number }>('k')).toEqual({ data: 42 });
    });

    it('deleteKey → commit removes from storage', () => {
      storage.putMany([['k', 'val']]);
      pm.deleteKey('k');
      pm.commit();
      expect(storage.get('k')).toBeNull();
    });

    it('deleteKey → readKey returns null', () => {
      storage.putMany([['k', 'val']]);
      pm.deleteKey('k');
      expect(pm.readKey('k')).toBeNull();
    });
  });

  describe('commit', () => {
    it('flushes WAL to storage', () => {
      pm.writeKey('a', 1);
      pm.writeKey('b', 2);
      pm.commit();
      expect(storage.get('a')).toBe(1);
      expect(storage.get('b')).toBe(2);
    });

    it('noop when WAL is empty', () => {
      pm.commit(); // should not throw
    });
  });

  describe('rollback', () => {
    it('discards uncommitted writes', () => {
      pm.writeKey('k', 'val');
      pm.rollback();
      expect(pm.readKey('k')).toBeNull();
    });

    it('does not affect committed data', () => {
      pm.writeKey('k', 'committed');
      pm.commit();

      pm.writeKey('k', 'uncommitted');
      pm.rollback();

      expect(pm.readKey<string>('k')).toBe('committed');
    });
  });

  describe('getAllKeys', () => {
    it('merges WAL and storage keys', () => {
      storage.putMany([['idx:test:a', 'val']]);
      pm.writeKey('idx:test:b', 'val');
      const keys = pm.getAllKeys('idx:test:');
      expect(keys).toEqual(['idx:test:a', 'idx:test:b']);
    });

    it('WAL deletes remove storage keys', () => {
      storage.putMany([['idx:test:a', 'val']]);
      pm.deleteKey('idx:test:a');
      const keys = pm.getAllKeys('idx:test:');
      expect(keys).toEqual([]);
    });
  });

  describe('LRU cache', () => {
    it('caches reads from storage', () => {
      storage.putMany([['k', { data: 1 }]]);
      expect(pm.readKey<{ data: number }>('k')).toEqual({ data: 1 });

      // Delete from storage directly — cache should still serve
      storage.putMany([['k', null]]);
      expect(pm.readKey<{ data: number }>('k')).toEqual({ data: 1 });
    });

    it('commit promotes WAL to cache', () => {
      pm.writeKey('k', 'v');
      pm.commit();

      storage.putMany([['k', null]]);
      expect(pm.readKey<string>('k')).toBe('v');
    });

    it('evicts oldest entries', () => {
      const pm2 = new SyncPageManager(storage, 2); // capacity=2
      storage.putMany([['a', 1], ['b', 2], ['c', 3]]);

      pm2.readKey('a'); // cached
      pm2.readKey('b'); // cached, evicts nothing yet
      pm2.readKey('c'); // cached, evicts 'a' (capacity=2)

      // 'a' was evicted from cache, now delete from storage too
      storage.putMany([['a', null]]);
      expect(pm2.readKey('a')).toBeNull(); // not in cache, not in storage
    });
  });
});

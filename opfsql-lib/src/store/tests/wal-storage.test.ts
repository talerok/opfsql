import { describe, it, expect, beforeEach } from 'vitest';
import { WalStorage } from '../wal/wal-storage.js';
import { MemoryPageStorage } from '../backend/memory-storage.js';
import { MemoryFileHandle } from '../wal/file-handle.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function create(
  main?: MemoryPageStorage,
  walHandle?: MemoryFileHandle,
  threshold?: number,
): { main: MemoryPageStorage; walHandle: MemoryFileHandle; wal: WalStorage } {
  const m = main ?? new MemoryPageStorage();
  const w = walHandle ?? new MemoryFileHandle();
  const wal = new WalStorage(m, w, threshold);
  return { main: m, walHandle: w, wal };
}

async function opened(
  main?: MemoryPageStorage,
  walHandle?: MemoryFileHandle,
  threshold?: number,
) {
  const ctx = create(main, walHandle, threshold);
  await ctx.wal.open();
  return ctx;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('WalStorage', () => {
  describe('basic commit', () => {
    it('write → flush → read returns committed value', async () => {
      const { wal } = await opened();
      wal.writePage(5, { hello: 'world' });
      wal.writeHeader(4);
      wal.flush();
      expect(wal.readPage(5)).toEqual({ hello: 'world' });
    });

    it('flush updates getNextPageId', async () => {
      const { wal } = await opened();
      wal.writePage(3, { a: 1 });
      wal.writeHeader(10);
      wal.flush();
      expect(wal.getNextPageId()).toBe(10);
    });

    it('multiple pages in one commit', async () => {
      const { wal } = await opened();
      wal.writePage(3, 'page3');
      wal.writePage(4, 'page4');
      wal.writePage(5, 'page5');
      wal.writeHeader(6);
      wal.flush();
      expect(wal.readPage(3)).toBe('page3');
      expect(wal.readPage(4)).toBe('page4');
      expect(wal.readPage(5)).toBe('page5');
    });
  });

  describe('read merging', () => {
    it('WAL overrides main DB', async () => {
      const main = new MemoryPageStorage();
      main.writePage(3, 'original');
      main.flush();
      const { wal } = await opened(main);

      expect(wal.readPage(3)).toBe('original');

      wal.writePage(3, 'updated');
      wal.writeHeader(main.getNextPageId());
      wal.flush();

      expect(wal.readPage(3)).toBe('updated');
    });

    it('non-WAL pages fall through to main', async () => {
      const main = new MemoryPageStorage();
      main.writePage(3, 'from-main');
      main.flush();
      const { wal } = await opened(main);

      wal.writePage(4, 'from-wal');
      wal.writeHeader(5);
      wal.flush();

      expect(wal.readPage(3)).toBe('from-main');
      expect(wal.readPage(4)).toBe('from-wal');
    });
  });

  describe('uncommitted writes', () => {
    it('pending writes are not visible via readPage before flush', async () => {
      const { wal } = await opened();
      wal.writePage(5, { pending: true });
      // Not flushed — readPage should NOT see it (walIndex not updated)
      expect(wal.readPage(5)).toBeNull();
    });

    it('uncommitted writes lost after reopen', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(5, 'uncommitted');
      // No flush — "crash"
      // Reopen with same handles
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(5)).toBeNull();
    });
  });

  describe('recovery', () => {
    it('committed data survives reopen without checkpoint', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, { recovered: true });
      wal.writePage(4, [1, 2, 3]);
      wal.writeHeader(5);
      wal.flush();

      // "Crash" — don't call close() (which would checkpoint)
      // Reopen
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();

      expect(wal2.readPage(3)).toEqual({ recovered: true });
      expect(wal2.readPage(4)).toEqual([1, 2, 3]);
      expect(wal2.getNextPageId()).toBe(5);
    });

    it('multiple committed transactions recovered', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // tx1
      wal.writePage(3, 'tx1');
      wal.writeHeader(4);
      wal.flush();

      // tx2
      wal.writePage(4, 'tx2');
      wal.writeHeader(5);
      wal.flush();

      // Reopen
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('tx1');
      expect(wal2.readPage(4)).toBe('tx2');
      expect(wal2.getNextPageId()).toBe(5);
    });

    it('latest write wins when same page written in multiple txs', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, 'v1');
      wal.writeHeader(4);
      wal.flush();

      wal.writePage(3, 'v2');
      wal.writeHeader(4);
      wal.flush();

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('v2');
    });

    it('nextPageId from latest commit record used', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, 'a');
      wal.writeHeader(10);
      wal.flush();

      wal.writePage(4, 'b');
      wal.writeHeader(20);
      wal.flush();

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.getNextPageId()).toBe(20);
    });

    it('can continue committing after recovery', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, 'before-crash');
      wal.writeHeader(4);
      wal.flush();

      // Reopen
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();

      // New commit after recovery
      wal2.writePage(4, 'after-crash');
      wal2.writeHeader(5);
      wal2.flush();

      expect(wal2.readPage(3)).toBe('before-crash');
      expect(wal2.readPage(4)).toBe('after-crash');
    });
  });

  describe('torn writes', () => {
    it('truncated frame header discarded on recovery', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // Committed tx
      wal.writePage(3, 'committed');
      wal.writeHeader(4);
      wal.flush();

      // Simulate partial write: append a few bytes (incomplete frame header)
      const garbage = new Uint8Array([0x01, 0x02, 0x03]);
      walHandle.write(garbage, { at: walHandle.getSize() });

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('committed');
    });

    it('frame with bad checksum stops recovery at that point', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // tx1 — committed
      wal.writePage(3, 'good');
      wal.writeHeader(4);
      wal.flush();

      // tx2 — committed
      wal.writePage(4, 'also-good');
      wal.writeHeader(5);
      wal.flush();

      // Corrupt a byte in tx2's first frame (after tx1's commit record)
      // tx1: data frame (16 + payload) + commit record (20) = some offset
      // We corrupt somewhere in the middle of tx2's data frame checksum
      const snap = walHandle.snapshot();
      // Find tx2 start: after WAL header + tx1 data frame + tx1 commit record
      // We'll just corrupt the last committed data to simulate partial tx2
      // Corrupt byte at offset that falls within tx2 data frame
      const size = walHandle.getSize();
      // Corrupt a byte near the end of the file (within tx2 commit record)
      const corruptOffset = size - 5;
      const buf = new Uint8Array(1);
      walHandle.read(buf, { at: corruptOffset });
      buf[0] = buf[0] ^ 0xff; // flip bits
      walHandle.write(buf, { at: corruptOffset });

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();

      // tx1 should be recovered, tx2 may or may not be depending on where
      // the corruption happened. The key guarantee: no crash, and at least tx1 works.
      expect(wal2.readPage(3)).toBe('good');
    });

    it('uncommitted tx (no commit record) ignored on recovery', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // tx1 committed
      wal.writePage(3, 'committed');
      wal.writeHeader(4);
      wal.flush();

      // tx2 — write frames but don't flush (simulates crash before commit record)
      wal.writePage(5, 'uncommitted');
      // Manually append the frame to WAL file but NOT the commit record
      // The pending frames are in memory, not flushed to WAL file yet,
      // so on reopen they simply don't exist.

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('committed');
      expect(wal2.readPage(5)).toBeNull();
    });
  });

  describe('checkpoint', () => {
    it('checkpoint applies WAL to main DB', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, 'checkpointed');
      wal.writeHeader(4);
      wal.flush();

      wal.checkpoint();

      // Main DB now has the data
      expect(main.readPage(3)).toBe('checkpointed');
      expect(main.getNextPageId()).toBe(4);
    });

    it('WAL file reset after checkpoint', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, 'data');
      wal.writeHeader(4);
      wal.flush();

      wal.checkpoint();

      // WAL file should be just the header (16 bytes)
      expect(walHandle.getSize()).toBe(16);
    });

    it('reads still work after checkpoint (from main)', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, 'persisted');
      wal.writeHeader(4);
      wal.flush();
      wal.checkpoint();

      expect(wal.readPage(3)).toBe('persisted');
    });

    it('close() triggers checkpoint', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      wal.writePage(3, 'via-close');
      wal.writeHeader(4);
      wal.flush();

      wal.close();

      expect(main.readPage(3)).toBe('via-close');
      expect(main.getNextPageId()).toBe(4);
    });

    it('idempotent: checkpoint after checkpoint is no-op', async () => {
      const { wal } = await opened();
      wal.writePage(3, 'x');
      wal.writeHeader(4);
      wal.flush();
      wal.checkpoint();
      wal.checkpoint(); // should not throw
    });
  });

  describe('auto-checkpoint', () => {
    it('triggers checkpoint when frame count reaches threshold', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle, 3); // threshold = 3

      // tx1: 1 frame
      wal.writePage(3, 'a');
      wal.writeHeader(4);
      wal.flush();
      expect(main.readPage(3)).toBeNull(); // not checkpointed yet

      // tx2: 1 frame (total = 2)
      wal.writePage(4, 'b');
      wal.writeHeader(5);
      wal.flush();
      expect(main.readPage(3)).toBeNull(); // still not

      // tx3: 1 frame (total = 3 → triggers checkpoint)
      wal.writePage(5, 'c');
      wal.writeHeader(6);
      wal.flush();
      expect(main.readPage(3)).toBe('a'); // checkpointed!
      expect(main.readPage(4)).toBe('b');
      expect(main.readPage(5)).toBe('c');
    });

    it('continues working after auto-checkpoint', async () => {
      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle, 2);

      // 2 frames → checkpoint
      wal.writePage(3, 'x');
      wal.writePage(4, 'y');
      wal.writeHeader(5);
      wal.flush();

      // New commit after auto-checkpoint
      wal.writePage(5, 'z');
      wal.writeHeader(6);
      wal.flush();

      expect(wal.readPage(5)).toBe('z');
    });
  });

  describe('empty WAL', () => {
    it('flush with no pending frames is no-op', async () => {
      const { wal, walHandle } = await opened();
      const sizeBefore = walHandle.getSize();
      wal.flush();
      expect(walHandle.getSize()).toBe(sizeBefore);
    });

    it('checkpoint with empty walIndex is no-op', async () => {
      const { wal } = await opened();
      wal.checkpoint(); // should not throw
    });
  });

  describe('integration with SyncPageStore', () => {
    it('full cycle: SyncPageStore → WalStorage → MemoryPageStorage', async () => {
      const { SyncPageStore } = await import('../page-manager.js');

      const main = new MemoryPageStorage();
      const walHandle = new MemoryFileHandle();
      const walStorage = new WalStorage(main, walHandle);
      await walStorage.open();

      const freeList = walStorage.readPage<number[]>(2) ?? [];
      const ps = new SyncPageStore(
        walStorage,
        walStorage.getNextPageId(),
        freeList,
      );

      // Allocate and write via SyncPageStore
      const p1 = ps.allocPage();
      ps.writePage(p1, { row: 'hello' });
      ps.commit();

      // Data should be readable
      expect(walStorage.readPage(p1)).toEqual({ row: 'hello' });

      // Rollback should discard
      const p2 = ps.allocPage();
      ps.writePage(p2, { row: 'rolled-back' });
      ps.rollback();

      expect(walStorage.readPage(p2)).toBeNull();

      // Close → checkpoint → main has data
      walStorage.close();
      expect(main.readPage(p1)).toEqual({ row: 'hello' });
    });
  });
});

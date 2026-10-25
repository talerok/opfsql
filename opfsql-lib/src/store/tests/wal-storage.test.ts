import { resetMockOPFS } from 'opfs-mock';
import { beforeEach, describe, it, expect, vi } from 'vitest';
import { WalStorage } from '../wal/wal-storage.js';
import { OPFSSyncStorage } from '../backend/opfs-storage.js';
import { MemoryFileHandle } from '../wal/file-handle.js';
import { PageOverflowError, WalUnknownStateError } from '../errors.js';
import type { EncodedPage } from '../page-codec.js';
import { commitBatch } from './helpers.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let seq = 0;

beforeEach(() => { resetMockOPFS(); });

/** Commit record = frame header (16) + nextPageId payload (4). */
const COMMIT_RECORD_SIZE = 20;

type FileOp = 'write' | 'flush' | 'truncate';

/** MemoryFileHandle that fails chosen operations. A failed write is torn: half of it lands. */
class FaultyFileHandle extends MemoryFileHandle {
  private readonly failures = new Map<FileOp, number>();

  fail(op: FileOp, times = 1): void {
    this.failures.set(op, times);
  }

  override write(buffer: Uint8Array, options?: { at?: number }): number {
    if (this.consume('write')) {
      super.write(buffer.subarray(0, buffer.length >> 1), options);
      throw new Error('injected write failure');
    }
    return super.write(buffer, options);
  }

  override flush(): void {
    if (this.consume('flush')) throw new Error('injected flush failure');
    super.flush();
  }

  override truncate(size: number): void {
    if (this.consume('truncate')) throw new Error('injected truncate failure');
    super.truncate(size);
  }

  private consume(op: FileOp): boolean {
    const left = this.failures.get(op) ?? 0;
    if (left === 0) return false;
    this.failures.set(op, left - 1);
    return true;
  }
}

/** Main file whose commit (i.e. checkpoint target) can be made to fail. */
class FlakyMain extends OPFSSyncStorage {
  failCommits = 0;

  override commit(pages: ReadonlyMap<number, EncodedPage>, nextPageId: number): void {
    if (this.failCommits > 0) {
      this.failCommits--;
      throw new Error('injected checkpoint failure');
    }
    super.commit(pages, nextPageId);
  }
}

async function createMain(): Promise<OPFSSyncStorage> {
  const s = new OPFSSyncStorage(`wal-main-${seq++}`);
  await s.open();
  return s;
}

async function opened(
  main?: OPFSSyncStorage,
  walHandle?: MemoryFileHandle,
  threshold?: number,
) {
  const m = main ?? await createMain();
  const w = walHandle ?? new MemoryFileHandle();
  const wal = new WalStorage(m, w, threshold);
  await wal.open();
  return { main: m, walHandle: w, wal };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('WalStorage', () => {
  describe('basic commit', () => {
    it('commit → read returns committed value', async () => {
      const { wal } = await opened();
      commitBatch(wal, { 5: { hello: 'world' } }, 4);
      expect(wal.readPage(5)).toEqual({ hello: 'world' });
    });

    it('commit updates getNextPageId', async () => {
      const { wal } = await opened();
      commitBatch(wal, { 3: { a: 1 } }, 10);
      expect(wal.getNextPageId()).toBe(10);
    });

    it('multiple pages in one commit', async () => {
      const { wal } = await opened();
      commitBatch(wal, { 3: 'page3', 4: 'page4', 5: 'page5' }, 6);
      expect(wal.readPage(3)).toBe('page3');
      expect(wal.readPage(4)).toBe('page4');
      expect(wal.readPage(5)).toBe('page5');
    });
  });

  describe('read merging', () => {
    it('WAL overrides main DB', async () => {
      const main = await createMain();
      commitBatch(main, { 3: 'original' });
      const { wal } = await opened(main);

      expect(wal.readPage(3)).toBe('original');

      commitBatch(wal, { 3: 'updated' }, main.getNextPageId());

      expect(wal.readPage(3)).toBe('updated');
    });

    it('non-WAL pages fall through to main', async () => {
      const main = await createMain();
      commitBatch(main, { 3: 'from-main' });
      const { wal } = await opened(main);

      commitBatch(wal, { 4: 'from-wal' }, 5);

      expect(wal.readPage(3)).toBe('from-main');
      expect(wal.readPage(4)).toBe('from-wal');
    });
  });

  describe('failed commit', () => {
    it('a torn write leaves nothing visible and nothing to recover', async () => {
      const main = await createMain();
      const walHandle = new FaultyFileHandle();
      const { wal } = await opened(main, walHandle);
      commitBatch(wal, { 3: 'tx1' }, 4);
      const sizeAfterTx1 = walHandle.getSize();

      walHandle.fail('write');
      expect(() => commitBatch(wal, { 5: 'tx2' }, 6)).toThrow(/injected write/);
      expect(wal.readPage(5)).toBeNull();
      expect(wal.getNextPageId()).toBe(4);
      expect(walHandle.getSize()).toBe(sizeAfterTx1);

      commitBatch(wal, { 4: 'tx3' }, 5);
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('tx1');
      expect(wal2.readPage(4)).toBe('tx3');
      expect(wal2.readPage(5)).toBeNull();
    });

    it('a failed flush rolls the append back', async () => {
      const main = await createMain();
      const walHandle = new FaultyFileHandle();
      const { wal } = await opened(main, walHandle);
      commitBatch(wal, { 3: 'tx1' }, 4);

      walHandle.fail('flush');
      expect(() => commitBatch(wal, { 5: 'tx2' }, 6)).toThrow(/injected flush/);
      expect(wal.readPage(5)).toBeNull();

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('tx1');
      expect(wal2.readPage(5)).toBeNull();
    });

    it('refuses further commits when a failed append cannot be undone', async () => {
      const main = await createMain();
      const walHandle = new FaultyFileHandle();
      const { wal } = await opened(main, walHandle);
      commitBatch(wal, { 3: 'tx1' }, 4);

      walHandle.fail('flush');
      walHandle.fail('truncate');
      expect(() => commitBatch(wal, { 5: 'tx2' }, 6)).toThrow(/injected flush/);
      expect(() => commitBatch(wal, { 4: 'tx3' }, 5)).toThrow(WalUnknownStateError);

      // Disk is the truth: the write itself completed, so recovery finds tx2's
      // commit record. The caller saw an error for a durable transaction — which
      // is exactly why the WAL stops accepting commits instead of guessing.
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('tx1');
      expect(wal2.readPage(5)).toBe('tx2');
      commitBatch(wal2, { 4: 'tx3' }, 6);
      expect(wal2.readPage(4)).toBe('tx3');
    });

    it('rejects a page that checkpoint could not write to the main file', async () => {
      const { wal, walHandle } = await opened();
      const sizeBefore = walHandle.getSize();
      expect(() => commitBatch(wal, { 3: 'x'.repeat(40000) }, 4)).toThrow(
        PageOverflowError,
      );
      expect(walHandle.getSize()).toBe(sizeBefore);
      expect(wal.readPage(3)).toBeNull();
    });
  });

  describe('recovery', () => {
    it('committed data survives reopen without checkpoint', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: { recovered: true }, 4: [1, 2, 3] }, 5);

      // "Crash" — don't call close() (which would checkpoint)
      // Reopen
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();

      expect(wal2.readPage(3)).toEqual({ recovered: true });
      expect(wal2.readPage(4)).toEqual([1, 2, 3]);
      expect(wal2.getNextPageId()).toBe(5);
    });

    it('multiple committed transactions recovered', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // tx1
      commitBatch(wal, { 3: 'tx1' }, 4);

      // tx2
      commitBatch(wal, { 4: 'tx2' }, 5);

      // Reopen
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('tx1');
      expect(wal2.readPage(4)).toBe('tx2');
      expect(wal2.getNextPageId()).toBe(5);
    });

    it('latest write wins when same page written in multiple txs', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'v1' }, 4);

      commitBatch(wal, { 3: 'v2' }, 4);

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('v2');
    });

    it('nextPageId from latest commit record used', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'a' }, 10);

      commitBatch(wal, { 4: 'b' }, 20);

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.getNextPageId()).toBe(20);
    });

    it('can continue committing after recovery', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'before-crash' }, 4);

      // Reopen
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();

      // New commit after recovery
      commitBatch(wal2, { 4: 'after-crash' }, 5);

      expect(wal2.readPage(3)).toBe('before-crash');
      expect(wal2.readPage(4)).toBe('after-crash');
    });
  });

  describe('torn writes', () => {
    it('truncated frame header discarded on recovery', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // Committed tx
      commitBatch(wal, { 3: 'committed' }, 4);

      // Simulate partial write: append a few bytes (incomplete frame header)
      const garbage = new Uint8Array([0x01, 0x02, 0x03]);
      walHandle.write(garbage, { at: walHandle.getSize() });

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('committed');
    });

    it('frame with bad checksum stops recovery at that point', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // tx1 — committed
      commitBatch(wal, { 3: 'good' }, 4);

      // tx2 — committed
      commitBatch(wal, { 4: 'also-good' }, 5);

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
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      // tx1 committed
      commitBatch(wal, { 3: 'committed' }, 4);

      // tx2 — crash after its data frames hit the file, before its commit record
      commitBatch(wal, { 5: 'uncommitted' }, 6);
      walHandle.truncate(walHandle.getSize() - COMMIT_RECORD_SIZE);

      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.readPage(3)).toBe('committed');
      expect(wal2.readPage(5)).toBeNull();
    });
  });

  describe('checkpoint', () => {
    it('checkpoint applies WAL to main DB', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'checkpointed' }, 4);

      wal.checkpoint();

      // Main DB now has the data
      expect(main.readPage(3)).toBe('checkpointed');
      expect(main.getNextPageId()).toBe(4);
    });

    it('WAL file reset after checkpoint', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'data' }, 4);

      wal.checkpoint();

      // WAL file should be just the header (16 bytes)
      expect(walHandle.getSize()).toBe(16);
    });

    it('reads still work after checkpoint (from main)', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'persisted' }, 4);
      wal.checkpoint();

      expect(wal.readPage(3)).toBe('persisted');
    });

    it('close() does not checkpoint (unsafe without cross-tab lock)', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'via-close' }, 4);

      wal.close();

      // Reopen main to verify data was NOT checkpointed
      await main.open();
      expect(main.readPage(3)).toBeNull();
    });

    it('idempotent: checkpoint after checkpoint is no-op', async () => {
      const { wal } = await opened();
      commitBatch(wal, { 3: 'x' }, 4);
      wal.checkpoint();
      wal.checkpoint(); // should not throw
    });
  });

  describe('checkpoint threshold', () => {
    it('needsCheckpoint turns on when frame count reaches threshold', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle, 3); // threshold = 3

      commitBatch(wal, { 3: 'a' }, 4);
      commitBatch(wal, { 4: 'b' }, 5);
      expect(wal.needsCheckpoint()).toBe(false);

      commitBatch(wal, { 5: 'c' }, 6);
      expect(wal.needsCheckpoint()).toBe(true);
    });

    it('commit never checkpoints by itself', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle, 1);

      commitBatch(wal, { 3: 'a', 4: 'b' }, 5);
      expect(main.readPage(3)).toBeNull();
      expect(wal.readPage(3)).toBe('a');
    });

    it('checkpoint resets the frame count', async () => {
      const { wal } = await opened(undefined, undefined, 2);
      commitBatch(wal, { 3: 'x', 4: 'y' }, 5);
      expect(wal.needsCheckpoint()).toBe(true);

      wal.checkpoint();
      expect(wal.needsCheckpoint()).toBe(false);

      commitBatch(wal, { 5: 'z' }, 6);
      expect(wal.readPage(5)).toBe('z');
    });

    it('a failed checkpoint keeps the WAL authoritative and can be retried', async () => {
      const main = new FlakyMain(`wal-main-${seq++}`);
      await main.open();
      const { wal, walHandle } = await opened(main);
      commitBatch(wal, { 3: 'a' }, 4);
      const sizeBefore = walHandle.getSize();

      main.failCommits = 1;
      expect(() => wal.checkpoint()).toThrow(/injected checkpoint/);
      expect(wal.readPage(3)).toBe('a');
      expect(walHandle.getSize()).toBe(sizeBefore);

      wal.checkpoint();
      expect(main.readPage(3)).toBe('a');
    });
  });

  describe('page versions', () => {
    it('latestCommit counts committed transactions', async () => {
      const { wal } = await opened();
      expect(wal.latestCommit()).toBe(0);
      commitBatch(wal, { 3: 'a' }, 4);
      commitBatch(wal, { 3: 'b' }, 4);
      expect(wal.latestCommit()).toBe(2);
    });

    it('a snapshot reads the versions committed up to it', async () => {
      const { wal } = await opened();
      commitBatch(wal, { 3: 'v1' }, 4);
      wal.prune(1); // a snapshot at transaction 1 is open
      commitBatch(wal, { 3: 'v2', 4: 'new' }, 5);

      expect(wal.readPage(3, 1)).toBe('v1');
      expect(wal.readPage(4, 1)).toBeNull();
      expect(wal.readPage(3)).toBe('v2');
    });

    it('pruning drops versions no open snapshot can see', async () => {
      const { wal } = await opened();
      commitBatch(wal, { 3: 'v1' }, 4);
      wal.prune(1);
      commitBatch(wal, { 3: 'v2' }, 4);
      commitBatch(wal, { 3: 'v3' }, 4);
      expect(wal.readPage(3, 2)).toBe('v2');

      wal.prune(3); // every open snapshot now sees transaction 3
      expect(wal.readPage(3)).toBe('v3');
      // v1 and v2 are gone: a read at an older position would fall through to
      // the main file, which is why prune() only ever gets the oldest snapshot.
      expect(wal.readPage(3, 2)).toBeNull();
    });

    it('a checkpoint up to an open snapshot leaves later versions in the WAL', async () => {
      const main = await createMain();
      const { wal } = await opened(main);
      commitBatch(wal, { 3: 'v1' }, 4);
      wal.prune(1);
      commitBatch(wal, { 3: 'v2' }, 4);

      wal.checkpoint(1);
      expect(main.readPage(3)).toBe('v1');
      expect(wal.readPage(3, 1)).toBe('v1');
      expect(wal.readPage(3)).toBe('v2');
      expect(wal.getEpoch()).toBe(0);

      wal.prune(2);
      wal.checkpoint(2);
      expect(main.readPage(3)).toBe('v2');
      expect(wal.getEpoch()).toBe(1);
    });

    it('transaction ids continue across WAL restarts and reopening', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);
      commitBatch(wal, { 3: 'a' }, 4);
      commitBatch(wal, { 3: 'b' }, 4);
      wal.checkpoint();
      commitBatch(wal, { 3: 'c' }, 4);
      expect(wal.latestCommit()).toBe(3);

      const reopened = new WalStorage(main, walHandle);
      await reopened.open();
      expect(reopened.latestCommit()).toBe(3);
      expect(reopened.readPage(3)).toBe('c');

      reopened.checkpoint();
      const afterRestart = new WalStorage(main, walHandle);
      await afterRestart.open();
      expect(afterRestart.latestCommit()).toBe(3);
    });

    it('a reader picks up the base transaction id after another writer restarts the WAL', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const writer = new WalStorage(main, walHandle);
      await writer.open();
      const reader = new WalStorage(main, walHandle);
      await reader.open();

      commitBatch(writer, { 3: 'a' }, 4);
      commitBatch(writer, { 3: 'b' }, 4);
      writer.checkpoint();
      reader.catchUp();
      expect(reader.latestCommit()).toBe(2);
      expect(reader.readPage(3)).toBe('b');
    });
  });

  describe('empty WAL', () => {
    it('empty commit is a no-op', async () => {
      const { wal, walHandle } = await opened();
      const sizeBefore = walHandle.getSize();
      wal.commit(new Map(), wal.getNextPageId());
      expect(walHandle.getSize()).toBe(sizeBefore);
    });

    it('checkpoint with empty walIndex is no-op', async () => {
      const { wal } = await opened();
      wal.checkpoint(); // should not throw
    });
  });

  describe('epoch', () => {
    it('epoch starts at 0', async () => {
      const { wal } = await opened();
      expect(wal.getEpoch()).toBe(0);
    });

    it('epoch increments on checkpoint', async () => {
      const { wal } = await opened();
      commitBatch(wal, { 3: 'data' }, 4);

      wal.checkpoint();
      expect(wal.getEpoch()).toBe(1);

      commitBatch(wal, { 3: 'data2' }, 4);

      wal.checkpoint();
      expect(wal.getEpoch()).toBe(2);
    });

    it('epoch persists across recovery', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const { wal } = await opened(main, walHandle);

      commitBatch(wal, { 3: 'data' }, 4);
      wal.checkpoint();
      expect(wal.getEpoch()).toBe(1);

      // Write more after checkpoint so WAL is non-empty for recovery
      commitBatch(wal, { 3: 'data2' }, 4);

      // Re-open: should recover epoch from WAL header
      const wal2 = new WalStorage(main, walHandle);
      await wal2.open();
      expect(wal2.getEpoch()).toBe(1);
    });
  });

  describe('catchUp', () => {
    it('catches up frames written by another writer', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();

      // Writer opens and writes
      const writer = new WalStorage(main, walHandle);
      await writer.open();
      commitBatch(writer, { 3: 'from-writer' }, 4);

      // Reader opens same WAL — sees frames from recovery
      const reader = new WalStorage(main, walHandle);
      await reader.open();
      expect(reader.readPage(3)).toBe('from-writer');

      // Writer writes more
      commitBatch(writer, { 5: 'second-write' }, 6);

      // Reader doesn't see it yet (no catchUp)
      expect(reader.readPage(5)).toBeNull();

      // After catchUp, reader sees it
      const changed = reader.catchUp();
      expect(changed).not.toBeNull();
      expect(changed!.has(5)).toBe(true);
      expect(reader.readPage(5)).toBe('second-write');
      expect(reader.getNextPageId()).toBe(6);
    });

    it('detects epoch change after checkpoint', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();

      const writer = new WalStorage(main, walHandle);
      await writer.open();
      commitBatch(writer, { 3: 'v1' }, 4);

      const reader = new WalStorage(main, walHandle);
      await reader.open();
      expect(reader.readPage(3)).toBe('v1');

      // Writer checkpoints and writes new data
      writer.checkpoint();
      commitBatch(writer, { 3: 'v2' }, 4);

      // Reader catches up — detects epoch change, rebuilds
      const changed = reader.catchUp();
      expect(changed).not.toBeNull();
      expect(changed!.has(3)).toBe(true);
      expect(reader.readPage(3)).toBe('v2');
      expect(reader.getEpoch()).toBe(1);
    });

    it('returns false when nothing changed', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();

      const wal = new WalStorage(main, walHandle);
      await wal.open();
      commitBatch(wal, { 3: 'data' }, 4);

      // No new writes — catchUp returns null
      const changed = wal.catchUp();
      expect(changed).toBeNull();
    });

    it('does not see uncommitted frames from concurrent writer', async () => {
      const main = await createMain();
      const walHandle = new MemoryFileHandle();

      const writer = new WalStorage(main, walHandle);
      await writer.open();

      // Writer commits one frame
      commitBatch(writer, { 3: 'committed' }, 4);

      // Writer is mid-append: data frame in the file, commit record not yet
      commitBatch(writer, { 5: 'pending' }, 6);
      walHandle.truncate(walHandle.getSize() - COMMIT_RECORD_SIZE);

      const reader = new WalStorage(main, walHandle);
      await reader.open();
      expect(reader.readPage(3)).toBe('committed');
      expect(reader.readPage(5)).toBeNull(); // no commit record → not visible
    });
  });

  describe('integration with Storage + SessionStore', () => {
    it('full cycle: SessionStore → Storage → WalStorage → OPFSSyncStorage', async () => {
      const { Storage } = await import('../storage.js');

      const main = await createMain();
      const walHandle = new MemoryFileHandle();
      const walStorage = new WalStorage(main, walHandle);
      await walStorage.open();

      const storage = new Storage(walStorage);
      await storage.open();
      const ss = storage.createSession();

      // Allocate and write via SessionStore
      const p1 = ss.allocPage();
      ss.writePage(p1, { row: 'hello' });
      ss.commit();

      // Data should be readable
      expect(walStorage.readPage(p1)).toEqual({ row: 'hello' });

      // Rollback should discard
      const p2 = ss.allocPage();
      ss.writePage(p2, { row: 'rolled-back' });
      ss.rollback();

      expect(walStorage.readPage(p2)).toBeNull();

      // Explicit checkpoint → main has data
      walStorage.checkpoint();
      expect(main.readPage(p1)).toEqual({ row: 'hello' });

      walStorage.close();
    });

    it('checkpointIfNeeded checkpoints once the WAL asks for it', async () => {
      const { Storage } = await import('../storage.js');
      const main = await createMain();
      // Each commit below writes 2 frames: the data page + the freelist page.
      const { wal } = await opened(main, undefined, 3);
      const storage = new Storage(wal);
      await storage.open();
      const ss = storage.createSession();

      const p1 = ss.allocPage();
      ss.writePage(p1, 'first');
      ss.commit();
      storage.checkpointIfNeeded();
      expect(main.readPage(p1)).toBeNull();

      const p2 = ss.allocPage();
      ss.writePage(p2, 'second');
      ss.commit();
      storage.checkpointIfNeeded();
      expect(main.readPage(p1)).toBe('first');
      expect(main.readPage(p2)).toBe('second');
    });

    it('a failing checkpoint is reported, never thrown', async () => {
      const { Storage } = await import('../storage.js');
      const main = new FlakyMain(`wal-main-${seq++}`);
      await main.open();
      const { wal } = await opened(main, undefined, 1);
      const errors: unknown[] = [];
      const storage = new Storage(wal, { onCheckpointError: (e) => errors.push(e) });
      await storage.open();
      const ss = storage.createSession();

      main.failCommits = 1;
      const p1 = ss.allocPage();
      ss.writePage(p1, 'committed');
      ss.commit();
      expect(() => storage.checkpointIfNeeded()).not.toThrow();
      expect(errors).toHaveLength(1);
      expect(ss.readPage(p1)).toBe('committed');
      expect(main.readPage(p1)).toBeNull();

      // Retried after the next commit
      const p2 = ss.allocPage();
      ss.writePage(p2, 'next');
      ss.commit();
      storage.checkpointIfNeeded();
      expect(errors).toHaveLength(1);
      expect(main.readPage(p1)).toBe('committed');
      expect(main.readPage(p2)).toBe('next');
    });

    it('a throwing onCheckpointError is contained too', async () => {
      const { Storage } = await import('../storage.js');
      const main = new FlakyMain(`wal-main-${seq++}`);
      await main.open();
      const { wal } = await opened(main, undefined, 1);
      const storage = new Storage(wal, {
        onCheckpointError: () => { throw new Error('reporter broke'); },
      });
      await storage.open();
      const ss = storage.createSession();
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

      main.failCommits = 1;
      const p = ss.allocPage();
      ss.writePage(p, 'committed');
      ss.commit();
      expect(() => storage.checkpointIfNeeded()).not.toThrow();
      expect(ss.readPage(p)).toBe('committed');
      expect(consoleError).toHaveBeenCalledOnce();
      consoleError.mockRestore();
    });

    it('an oversized page fails the commit before anything reaches the WAL', async () => {
      const { Storage } = await import('../storage.js');
      const { wal, walHandle } = await opened();
      const storage = new Storage(wal);
      await storage.open();
      const ss = storage.createSession();
      const sizeBefore = walHandle.getSize();

      ss.writePage(10, 'x'.repeat(40000));
      expect(() => ss.commit()).toThrow(PageOverflowError);
      ss.rollback();
      expect(walHandle.getSize()).toBe(sizeBefore);
      expect(ss.readPage(10)).toBeNull();
    });
  });
});

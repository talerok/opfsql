import { Encoder, decode } from 'cbor-x';
import { crc32 } from './crc32.js';
import type { ISyncFileHandle } from './file-handle.js';
import type { SyncIPageStorage } from '../types.js';

const encoder = new Encoder({ structuredClone: false, useRecords: true });

// ---------------------------------------------------------------------------
// WAL file format
// ---------------------------------------------------------------------------
//
// Header (16 bytes):
//   [0..7]    magic "OPFSWAL1"
//   [8..15]   reserved (zeros)
//
// Data frame (16 + payloadLen bytes):
//   [0..3]    pageNo       u32 BE
//   [4..7]    payloadLen   u32 BE
//   [8..11]   txId         u32 BE
//   [12..15]  checksum     u32 BE  (CRC32 of [0..11] + payload)
//   [16..]    payload      CBOR bytes (payloadLen bytes)
//
// Commit record (20 bytes — frame with pageNo = 0xFFFFFFFF):
//   [0..3]    0xFFFFFFFF   sentinel
//   [4..7]    0x00000004   payloadLen = 4
//   [8..11]   txId         u32 BE
//   [12..15]  checksum     u32 BE
//   [16..19]  nextPageId   u32 BE
// ---------------------------------------------------------------------------

const WAL_MAGIC = new Uint8Array([
  0x4f, 0x50, 0x46, 0x53, 0x57, 0x41, 0x4c, 0x31, // "OPFSWAL1"
]);
const WAL_HEADER_SIZE = 16;
const FRAME_HEADER_SIZE = 16;
const COMMIT_SENTINEL = 0xffffffff;
const COMMIT_PAYLOAD_SIZE = 4;

interface PendingFrame {
  pageNo: number;
  cbor: Uint8Array;
  value: unknown;
}

export class WalStorage implements SyncIPageStorage {
  /** Committed WAL pages: pageNo → decoded value. */
  private walIndex = new Map<number, unknown>();

  /** Frames buffered during current uncommitted batch (between writePage calls and flush). */
  private pendingFrames: PendingFrame[] = [];

  /** Current write position in WAL file. */
  private walOffset = WAL_HEADER_SIZE;

  /** Next transaction id to assign. */
  private currentTxId = 1;

  /** Committed nextPageId (from the latest commit record or main DB). */
  private nextPageId = 3;

  /** Number of committed data frames in the WAL (for auto-checkpoint). */
  private walFrameCount = 0;

  constructor(
    private readonly main: SyncIPageStorage,
    private readonly walHandle: ISyncFileHandle,
    private readonly checkpointThreshold = 1000,
  ) {}

  // -------------------------------------------------------------------------
  // SyncIPageStorage
  // -------------------------------------------------------------------------

  async open(): Promise<void> {
    await this.main.open();
    this.nextPageId = this.main.getNextPageId();
    if (this.walHandle.getSize() > 0) {
      this.recover();
    } else {
      this.initWalHeader();
    }
  }

  close(): void {
    this.checkpoint();
    this.walHandle.close();
    this.main.close();
  }

  /** Returns committed data only. Pending (unflushed) writes are not visible. */
  readPage<T>(pageNo: number): T | null {
    const val = this.walIndex.get(pageNo);
    if (val !== undefined) return val as T;
    return this.main.readPage<T>(pageNo);
  }

  writePage(pageNo: number, value: unknown): void {
    const cbor = encoder.encode(value);
    this.pendingFrames.push({ pageNo, cbor, value });
  }

  getNextPageId(): number {
    return this.nextPageId;
  }

  writeHeader(nextPageId: number): void {
    this.nextPageId = nextPageId;
  }

  flush(): void {
    if (this.pendingFrames.length === 0) return;

    // 1. Build single buffer: all data frames + commit record
    const buf = this.buildCommitBuffer(
      this.pendingFrames,
      this.currentTxId,
      this.nextPageId,
    );

    // 2. Single write + durability point
    this.walHandle.write(buf, { at: this.walOffset });
    this.walHandle.flush();
    this.walOffset += buf.length;

    // 3. Promote to walIndex (already decoded — no decode overhead)
    for (const f of this.pendingFrames) {
      this.walIndex.set(f.pageNo, f.value);
    }

    this.walFrameCount += this.pendingFrames.length;
    this.pendingFrames = [];
    this.currentTxId++;

    // 4. Auto-checkpoint
    if (this.walFrameCount >= this.checkpointThreshold) {
      this.checkpoint();
    }
  }

  // -------------------------------------------------------------------------
  // Checkpoint
  // -------------------------------------------------------------------------

  checkpoint(): void {
    if (this.walIndex.size === 0) return;

    // 1. Apply all committed WAL pages to main DB
    for (const [pageNo, value] of this.walIndex) {
      this.main.writePage(pageNo, value);
    }
    this.main.writeHeader(this.nextPageId);
    this.main.flush();

    // 2. Shrink main DB file (safe — data already on disk after flush)
    this.main.truncateToSize?.();

    // 3. Reset WAL file
    this.walHandle.truncate(0);
    this.initWalHeader();
    this.walHandle.flush();

    this.walIndex.clear();
    this.walFrameCount = 0;
  }

  // -------------------------------------------------------------------------
  // Recovery
  // -------------------------------------------------------------------------

  private recover(): void {
    // Verify header
    const headerBuf = new Uint8Array(WAL_HEADER_SIZE);
    this.walHandle.read(headerBuf, { at: 0 });
    for (let i = 0; i < WAL_MAGIC.length; i++) {
      if (headerBuf[i] !== WAL_MAGIC[i]) {
        throw new Error('Invalid WAL file: bad magic');
      }
    }

    const fileSize = this.walHandle.getSize();
    let offset = WAL_HEADER_SIZE;

    // Two-pass: first collect all frames, then filter by committed txIds
    const committedTxIds = new Set<number>();
    const allFrames: Array<{
      pageNo: number;
      txId: number;
      payload: Uint8Array;
    }> = [];
    let lastCommittedNextPageId = this.nextPageId;

    while (offset + FRAME_HEADER_SIZE <= fileSize) {
      // Read frame header
      const hdr = new Uint8Array(FRAME_HEADER_SIZE);
      this.walHandle.read(hdr, { at: offset });
      const hv = new DataView(hdr.buffer, hdr.byteOffset, hdr.byteLength);
      const pageNo = hv.getUint32(0, false);
      const payloadLen = hv.getUint32(4, false);
      const txId = hv.getUint32(8, false);
      const storedChecksum = hv.getUint32(12, false);

      // Bounds check
      if (offset + FRAME_HEADER_SIZE + payloadLen > fileSize) break;

      // Read payload
      const payload = new Uint8Array(payloadLen);
      if (payloadLen > 0) {
        this.walHandle.read(payload, { at: offset + FRAME_HEADER_SIZE });
      }

      // Verify checksum
      const computed = this.computeChecksum(hdr.subarray(0, 12), payload);
      if (computed !== storedChecksum) break; // Torn write — stop

      if (pageNo === COMMIT_SENTINEL) {
        // Commit record
        const pv = new DataView(
          payload.buffer,
          payload.byteOffset,
          payload.byteLength,
        );
        lastCommittedNextPageId = pv.getUint32(0, false);
        committedTxIds.add(txId);
      } else {
        allFrames.push({ pageNo, txId, payload });
      }

      offset += FRAME_HEADER_SIZE + payloadLen;
    }

    // Build walIndex from committed frames only (in order, latest wins)
    for (const frame of allFrames) {
      if (committedTxIds.has(frame.txId)) {
        this.walIndex.set(frame.pageNo, decode(frame.payload));
        this.walFrameCount++;
      }
    }

    this.nextPageId = lastCommittedNextPageId;
    this.currentTxId =
      committedTxIds.size > 0 ? Math.max(...committedTxIds) + 1 : 1;
    this.walOffset = offset;
  }

  // -------------------------------------------------------------------------
  // Frame I/O
  // -------------------------------------------------------------------------

  /** Build a single buffer containing all data frames + commit record. */
  private buildCommitBuffer(
    frames: PendingFrame[],
    txId: number,
    nextPageId: number,
  ): Uint8Array {
    // Calculate total size
    let totalSize = FRAME_HEADER_SIZE + COMMIT_PAYLOAD_SIZE; // commit record
    for (const f of frames) totalSize += FRAME_HEADER_SIZE + f.cbor.length;

    const buf = new Uint8Array(totalSize);
    const view = new DataView(buf.buffer);
    let off = 0;

    // Data frames
    for (const f of frames) {
      view.setUint32(off, f.pageNo, false);
      view.setUint32(off + 4, f.cbor.length, false);
      view.setUint32(off + 8, txId, false);
      view.setUint32(
        off + 12,
        this.computeChecksum(
          buf.subarray(off, off + 12),
          f.cbor,
        ),
        false,
      );
      buf.set(f.cbor, off + FRAME_HEADER_SIZE);
      off += FRAME_HEADER_SIZE + f.cbor.length;
    }

    // Commit record
    const commitPayload = new Uint8Array(COMMIT_PAYLOAD_SIZE);
    new DataView(commitPayload.buffer).setUint32(0, nextPageId, false);

    view.setUint32(off, COMMIT_SENTINEL, false);
    view.setUint32(off + 4, COMMIT_PAYLOAD_SIZE, false);
    view.setUint32(off + 8, txId, false);
    view.setUint32(
      off + 12,
      this.computeChecksum(
        buf.subarray(off, off + 12),
        commitPayload,
      ),
      false,
    );
    buf.set(commitPayload, off + FRAME_HEADER_SIZE);

    return buf;
  }

  private initWalHeader(): void {
    const hdr = new Uint8Array(WAL_HEADER_SIZE);
    hdr.set(WAL_MAGIC, 0);
    // bytes 8..15 are reserved zeros
    this.walHandle.write(hdr, { at: 0 });
    this.walOffset = WAL_HEADER_SIZE;
  }

  private computeChecksum(
    headerPrefix: Uint8Array,
    payload: Uint8Array,
  ): number {
    const combined = new Uint8Array(headerPrefix.length + payload.length);
    combined.set(headerPrefix);
    combined.set(payload, headerPrefix.length);
    return crc32(combined);
  }
}

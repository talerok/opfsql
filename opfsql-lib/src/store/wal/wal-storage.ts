import { WalUnknownStateError } from "../errors.js";
import { assertPagesFit, decodePage, type EncodedPage } from "../page-codec.js";
import type { SyncIPageStorage } from "../types.js";
import { crc32 } from "./crc32.js";
import type { ISyncFileHandle } from "./file-handle.js";

// ---------------------------------------------------------------------------
// WAL file format
// ---------------------------------------------------------------------------
//
// Header (16 bytes):
//   [0..7]    magic "OPFSWAL2"
//   [8..11]   epoch    u32 BE  (incremented on each checkpoint)
//   [12..15]  reserved (zeros)
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
  0x4f,
  0x50,
  0x46,
  0x53,
  0x57,
  0x41,
  0x4c,
  0x32, // "OPFSWAL2"
]);
const WAL_HEADER_SIZE = 16;
const FRAME_HEADER_SIZE = 16;
const COMMIT_SENTINEL = 0xffffffff;
const COMMIT_PAYLOAD_SIZE = 4;

export class WalStorage implements SyncIPageStorage {
  /** Committed WAL pages: pageNo → page (decoded value for reads, bytes for checkpoint). */
  private walIndex = new Map<number, EncodedPage>();

  /** Set when a failed append could not be undone: the WAL tail is unknown until reopen. */
  private unknownState: WalUnknownStateError | null = null;

  /** Current write position in WAL file. */
  private walOffset = WAL_HEADER_SIZE;

  /** Next transaction id to assign. */
  private currentTxId = 1;

  /** Committed nextPageId (from the latest commit record or main DB). */
  private nextPageId = 3;

  /** Number of committed data frames in the WAL (for auto-checkpoint). */
  private walFrameCount = 0;

  /** WAL epoch — incremented on each checkpoint. */
  private epoch = 0;

  constructor(
    private readonly main: SyncIPageStorage,
    private readonly walHandle: ISyncFileHandle,
    private readonly checkpointThreshold = 1000,
  ) {}

  // -------------------------------------------------------------------------
  // SyncIPageStorage
  // -------------------------------------------------------------------------

  get pageCapacity(): number {
    return this.main.pageCapacity;
  }

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
    this.walHandle.close();
    this.main.close();
  }

  /** Returns committed data only. */
  readPage<T>(pageNo: number): T | null {
    const page = this.walIndex.get(pageNo);
    if (page !== undefined) return page.value as T;
    return this.main.readPage<T>(pageNo);
  }

  getNextPageId(): number {
    return this.nextPageId;
  }

  /**
   * Appends the batch as one transaction: data frames + commit record, one
   * write, one flush. The flush is the commit point — once it returns, the
   * batch is durable and visible; if anything before it throws, the appended
   * bytes are truncated away and nothing becomes visible.
   */
  commit(pages: ReadonlyMap<number, EncodedPage>, nextPageId: number): void {
    if (this.unknownState) throw this.unknownState;
    if (pages.size === 0 && nextPageId === this.nextPageId) return;

    // Never let the WAL hold a page that checkpoint could not write to main.
    assertPagesFit(pages, this.pageCapacity);

    const buf = this.buildCommitBuffer(pages, this.currentTxId, nextPageId);
    try {
      this.walHandle.write(buf, { at: this.walOffset });
      this.walHandle.flush();
    } catch (err) {
      this.discardTail();
      throw err;
    }

    this.walOffset += buf.length;
    for (const [pageNo, page] of pages) this.walIndex.set(pageNo, page);
    this.walFrameCount += pages.size;
    this.nextPageId = nextPageId;
    this.currentTxId++;
  }

  needsCheckpoint(): boolean {
    return this.walFrameCount >= this.checkpointThreshold;
  }

  // -------------------------------------------------------------------------
  // Epoch
  // -------------------------------------------------------------------------

  getEpoch(): number {
    return this.epoch;
  }

  // -------------------------------------------------------------------------
  // Checkpoint
  // -------------------------------------------------------------------------

  checkpoint(): void {
    if (this.walIndex.size === 0) return;

    // 1. Apply all committed WAL pages to main DB. If this throws, the WAL is
    //    untouched and still authoritative, so a later checkpoint can retry.
    this.main.commit(this.walIndex, this.nextPageId);

    // 2. Shrink main DB file (safe — data already on disk after commit)
    this.main.truncateToSize?.();

    // 3. Reset WAL file with incremented epoch
    this.epoch++;
    this.walHandle.truncate(0);
    this.initWalHeader();
    this.walHandle.flush();

    this.walIndex.clear();
    this.walFrameCount = 0;
  }

  // -------------------------------------------------------------------------
  // Catch-up — read changes made by other writers
  // -------------------------------------------------------------------------

  /** Check if WAL has changed and catch up. Returns set of changed page numbers, or null if nothing changed. */
  catchUp(): Set<number> | null {
    const fileEpoch = this.readEpochFromFile();
    const epochChanged = fileEpoch !== this.epoch;

    if (epochChanged) {
      this.resetAfterCheckpoint(fileEpoch);
    }

    const walSize = this.walHandle.getSize();
    const hasNewFrames = walSize > this.walOffset;

    if (!epochChanged && !hasNewFrames) return null;

    return hasNewFrames
      ? this.readFramesFrom(this.walOffset, walSize)
      : new Set<number>();
  }

  private resetAfterCheckpoint(newEpoch: number): void {
    this.walIndex.clear();
    this.walOffset = WAL_HEADER_SIZE;
    this.walFrameCount = 0;
    this.epoch = newEpoch;
    this.nextPageId = this.main.getNextPageId();
  }

  // -------------------------------------------------------------------------
  // Recovery
  // -------------------------------------------------------------------------

  private recover(): void {
    this.readAndVerifyHeader();
    const fileSize = this.walHandle.getSize();
    this.readFramesFrom(WAL_HEADER_SIZE, fileSize); // return value unused during recovery
  }

  private readAndVerifyHeader(): void {
    const headerBuf = new Uint8Array(WAL_HEADER_SIZE);
    this.walHandle.read(headerBuf, { at: 0 });
    for (let i = 0; i < WAL_MAGIC.length; i++) {
      if (headerBuf[i] !== WAL_MAGIC[i]) {
        throw new Error("Invalid WAL file: bad magic");
      }
    }
    const hv = new DataView(headerBuf.buffer);
    this.epoch = hv.getUint32(8, false);
  }

  /** Read epoch directly from WAL file header (for catch-up checks). */
  private readEpochFromFile(): number {
    const buf = new Uint8Array(4);
    this.walHandle.read(buf, { at: 8 });
    return new DataView(buf.buffer).getUint32(0, false);
  }

  /**
   * Read frames from startOffset to endOffset.
   * Only advances walOffset to the end of the last valid commit record.
   * Updates walIndex, nextPageId, currentTxId, walFrameCount.
   * Returns set of committed page numbers.
   */
  private readFramesFrom(startOffset: number, endOffset: number): Set<number> {
    let offset = startOffset;

    // Two-pass: first collect all frames, then filter by committed txIds
    const committedTxIds = new Set<number>();
    const allFrames: Array<{
      pageNo: number;
      txId: number;
      payload: Uint8Array;
    }> = [];

    let lastCommittedNextPageId = this.nextPageId;
    let lastCommitEnd = startOffset;

    while (offset + FRAME_HEADER_SIZE <= endOffset) {
      // Read frame header
      const hdr = new Uint8Array(FRAME_HEADER_SIZE);
      this.walHandle.read(hdr, { at: offset });
      const hv = new DataView(hdr.buffer, hdr.byteOffset, hdr.byteLength);
      const pageNo = hv.getUint32(0, false);
      const payloadLen = hv.getUint32(4, false);
      const txId = hv.getUint32(8, false);
      const storedChecksum = hv.getUint32(12, false);

      // Bounds check
      if (offset + FRAME_HEADER_SIZE + payloadLen > endOffset) {
        break;
      }

      // Read payload
      const payload = new Uint8Array(payloadLen);
      if (payloadLen > 0) {
        this.walHandle.read(payload, { at: offset + FRAME_HEADER_SIZE });
      }

      // Verify checksum
      const computed = this.computeChecksum(hdr.subarray(0, 12), payload);
      if (computed !== storedChecksum) break; // Torn write — stop

      const frameEnd = offset + FRAME_HEADER_SIZE + payloadLen;

      if (pageNo === COMMIT_SENTINEL) {
        // Commit record
        const pv = new DataView(
          payload.buffer,
          payload.byteOffset,
          payload.byteLength,
        );
        lastCommittedNextPageId = pv.getUint32(0, false);
        committedTxIds.add(txId);
        lastCommitEnd = frameEnd;
      } else {
        allFrames.push({ pageNo, txId, payload });
      }

      offset = frameEnd;
    }

    // Build walIndex from committed frames only (in order, latest wins)
    const changedPages = new Set<number>();
    for (const frame of allFrames) {
      if (committedTxIds.has(frame.txId)) {
        this.walIndex.set(frame.pageNo, {
          value: decodePage(frame.payload),
          bytes: frame.payload,
        });
        this.walFrameCount++;
        changedPages.add(frame.pageNo);
      }
    }

    this.nextPageId = lastCommittedNextPageId;
    if (committedTxIds.size > 0) {
      this.currentTxId = Math.max(...committedTxIds) + 1;
    }
    // Only advance offset to end of last valid commit record
    this.walOffset = lastCommitEnd;
    return changedPages;
  }

  // -------------------------------------------------------------------------
  // Frame I/O
  // -------------------------------------------------------------------------

  /** Build a single buffer containing all data frames + commit record. */
  private buildCommitBuffer(
    pages: ReadonlyMap<number, EncodedPage>,
    txId: number,
    nextPageId: number,
  ): Uint8Array {
    // Calculate total size
    let totalSize = FRAME_HEADER_SIZE + COMMIT_PAYLOAD_SIZE; // commit record
    for (const page of pages.values()) {
      totalSize += FRAME_HEADER_SIZE + page.bytes.length;
    }

    const buf = new Uint8Array(totalSize);
    const view = new DataView(buf.buffer);
    let off = 0;

    // Data frames
    for (const [pageNo, { bytes }] of pages) {
      view.setUint32(off, pageNo, false);
      view.setUint32(off + 4, bytes.length, false);
      view.setUint32(off + 8, txId, false);

      const checksum = this.computeChecksum(buf.subarray(off, off + 12), bytes);

      view.setUint32(off + 12, checksum, false);
      buf.set(bytes, off + FRAME_HEADER_SIZE);
      off += FRAME_HEADER_SIZE + bytes.length;
    }

    // Commit record
    const commitPayload = new Uint8Array(COMMIT_PAYLOAD_SIZE);
    new DataView(commitPayload.buffer).setUint32(0, nextPageId, false);

    view.setUint32(off, COMMIT_SENTINEL, false);
    view.setUint32(off + 4, COMMIT_PAYLOAD_SIZE, false);
    view.setUint32(off + 8, txId, false);

    const checkSum = this.computeChecksum(
      buf.subarray(off, off + 12),
      commitPayload,
    );

    view.setUint32(off + 12, checkSum, false);
    buf.set(commitPayload, off + FRAME_HEADER_SIZE);

    return buf;
  }

  /**
   * Undo a failed append, so a torn or unflushed commit can never be recovered
   * as committed. If even that fails, the tail is unknown: refuse further
   * commits until the WAL is reopened and recovered from disk.
   */
  private discardTail(): void {
    try {
      this.walHandle.truncate(this.walOffset);
      this.walHandle.flush();
    } catch (err) {
      this.unknownState = new WalUnknownStateError(err);
    }
  }

  private initWalHeader(): void {
    const hdr = new Uint8Array(WAL_HEADER_SIZE);
    hdr.set(WAL_MAGIC, 0);
    new DataView(hdr.buffer).setUint32(8, this.epoch, false);
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

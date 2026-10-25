import { WalUnknownStateError } from "../errors.js";
import {
  checkFormatPrefix,
  FORMAT_PREFIX_SIZE,
  writeFormatPrefix,
} from "../file-format.js";
import { assertPagesFit, decodePage, type EncodedPage } from "../page-codec.js";
import type { SyncIPageStorage } from "../types.js";
import { crc32 } from "./crc32.js";
import type { ISyncFileHandle } from "./file-handle.js";
import type { ReadMarks } from "./read-marks.js";

// ---------------------------------------------------------------------------
// WAL file format
// ---------------------------------------------------------------------------
//
// Header (20 bytes):
//   [0..11]   format prefix: magic "OPFSQLWL" + format version (file-format.ts)
//   [12..15]  epoch    u32 BE  (incremented on each WAL restart)
//   [16..19]  baseTxId u32 BE  (last transaction before this WAL generation;
//                               transaction ids continue from it)
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
//
// Snapshots
//
// Transaction ids increase monotonically across WAL generations, so a txId is
// a position in the database's history. A snapshot is the latest txId it can
// see. The WAL index keeps every page version a live snapshot may still need;
// a read returns the newest version not after the snapshot, else the main
// file. The main file is only ever brought up to a txId no live snapshot is
// older than, so whatever a snapshot reads from it is still what it saw.
//
// With read marks, "live" covers every tab: each tab publishes its oldest
// snapshot, and a checkpoint announces its target (the intent) before reading
// the marks. A tab that publishes an older snapshot after that read sees the
// intent and takes a newer snapshot instead — the SQLite WAL protocol.
// ---------------------------------------------------------------------------

const EPOCH_OFFSET = FORMAT_PREFIX_SIZE;
const BASE_TX_ID_OFFSET = FORMAT_PREFIX_SIZE + 4;
const WAL_HEADER_SIZE = FORMAT_PREFIX_SIZE + 8;
const FRAME_HEADER_SIZE = 16;
const COMMIT_SENTINEL = 0xffffffff;
const COMMIT_PAYLOAD_SIZE = 4;

interface PageVersion {
  readonly txId: number;
  readonly page: EncodedPage;
}

interface CommitRecord {
  readonly txId: number;
  readonly nextPageId: number;
}

/** Newest version a snapshot at `txId` can see, or undefined. */
function visibleVersion(
  versions: readonly PageVersion[],
  txId: number,
): PageVersion | undefined {
  for (let i = versions.length - 1; i >= 0; i--) {
    if (versions[i].txId <= txId) return versions[i];
  }
  return undefined;
}

export interface WalOptions {
  /** WAL frames after which needsCheckpoint() turns on. Default 1000. */
  checkpointThreshold?: number;
  /** Snapshots of other tabs sharing these files; omit for a single engine. */
  marks?: ReadMarks;
  /** The WAL's file name, for error messages. */
  fileName?: string;
}

export class WalStorage implements SyncIPageStorage {
  /** Committed WAL pages: pageNo → versions, oldest first. */
  private walIndex = new Map<number, PageVersion[]>();

  /** Pages holding more than one version — the only ones pruning visits. */
  private readonly layeredPages = new Set<number>();

  /** Commits in this WAL generation, in order — for nextPageId at any txId. */
  private commits: CommitRecord[] = [];

  /** No snapshot older than this is open; older page versions can go. */
  private pruneBound = Infinity;

  /** Set when a failed append could not be undone: the WAL tail is unknown until reopen. */
  private unknownState: WalUnknownStateError | null = null;

  /** Current write position in WAL file. */
  private walOffset = WAL_HEADER_SIZE;

  /** Last transaction before this WAL generation. */
  private baseTxId = 0;

  /** Next transaction id to assign. */
  private currentTxId = 1;

  /** The main file reflects every transaction up to this one. */
  private backfilledTxId = 0;

  /** Committed nextPageId (from the latest commit record or main DB). */
  private nextPageId = 3;

  /** Number of committed data frames in the WAL (for auto-checkpoint). */
  private walFrameCount = 0;

  /** WAL epoch — incremented on each WAL restart. */
  private epoch = 0;

  private readonly checkpointThreshold: number;
  private readonly marks: ReadMarks | undefined;
  private readonly fileName: string;

  constructor(
    private readonly main: SyncIPageStorage,
    private readonly walHandle: ISyncFileHandle,
    options: WalOptions = {},
  ) {
    this.checkpointThreshold = options.checkpointThreshold ?? 1000;
    this.marks = options.marks;
    this.fileName = options.fileName ?? "WAL";
  }

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
    this.marks?.close();
    this.walHandle.close();
    this.main.close();
  }

  /** Committed data only, as of `snapshot` (default: the latest commit). */
  readPage<T>(pageNo: number, snapshot = this.latestCommit()): T | null {
    const versions = this.walIndex.get(pageNo);
    const version = versions && visibleVersion(versions, snapshot);
    if (version) return version.page.value as T;
    return this.main.readPage<T>(pageNo);
  }

  getNextPageId(): number {
    return this.nextPageId;
  }

  latestCommit(): number {
    return this.currentTxId - 1;
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

    const txId = this.currentTxId;
    const buf = this.buildCommitBuffer(pages, txId, nextPageId);
    try {
      this.walHandle.write(buf, { at: this.walOffset });
      this.walHandle.flush();
    } catch (err) {
      this.discardTail();
      throw err;
    }

    this.walOffset += buf.length;
    for (const [pageNo, page] of pages) this.addVersion(pageNo, { txId, page });
    this.commits.push({ txId, nextPageId });
    this.walFrameCount += pages.size;
    this.nextPageId = nextPageId;
    this.currentTxId++;
  }

  /**
   * The oldest snapshot open in this engine (null: none). Page versions no
   * open snapshot can see are dropped, and other tabs learn the snapshot
   * through the read marks.
   */
  setOldestSnapshot(oldest: number | null): void {
    this.pruneBound = oldest ?? Infinity;
    for (const pageNo of this.layeredPages) {
      this.dropInvisible(pageNo, this.walIndex.get(pageNo)!);
    }
    this.marks?.publish(oldest);
  }

  /**
   * False when another tab's checkpoint may already be bringing the main file
   * past `snapshot`, which was taken before catching up with it: catch up and
   * take a newer snapshot.
   */
  isSnapshotSafe(snapshot: number): boolean {
    return !this.marks || this.marks.checkpointIntent() <= snapshot;
  }

  /** Clears read marks of tabs that died with a snapshot open. */
  async releaseAbandonedSnapshots(): Promise<void> {
    await this.marks?.reclaimDeadSlots();
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

  /**
   * Brings the main file up to `upTo` — which must be no newer than the
   * oldest open snapshot — and restarts the WAL once everything in it is in
   * the main file. If writing the main file throws, the WAL is untouched and
   * still authoritative, so a later checkpoint can retry.
   */
  checkpoint(upTo = this.latestCommit()): void {
    const target = this.agreeOnTarget(Math.min(upTo, this.latestCommit()));
    if (target > this.backfilledTxId) {
      const pages = new Map<number, EncodedPage>();
      for (const [pageNo, versions] of this.walIndex) {
        const version = visibleVersion(versions, target);
        if (version && version.txId > this.backfilledTxId) {
          pages.set(pageNo, version.page);
        }
      }
      this.main.commit(pages, this.nextPageIdAt(target));
      this.main.truncateToSize?.();
      this.backfilledTxId = target;
    }
    if (this.backfilledTxId === this.latestCommit() && this.commits.length) {
      this.restart();
    }
  }

  /**
   * Lowers the target to the oldest snapshot any tab has open. The intent is
   * written before each read of the marks, so a tab that publishes an older
   * snapshot afterwards sees it and takes a newer snapshot instead.
   */
  private agreeOnTarget(target: number): number {
    if (!this.marks) return target;
    for (;;) {
      this.marks.setCheckpointIntent(target);
      const oldest = this.marks.oldestMark();
      if (oldest === null || oldest >= target) return target;
      target = oldest;
    }
  }

  /** Every commit is in the main file: start an empty WAL generation. */
  private restart(): void {
    this.baseTxId = this.latestCommit();
    this.epoch++;
    this.walHandle.truncate(0);
    this.initWalHeader();
    this.walHandle.flush();

    this.walIndex.clear();
    this.layeredPages.clear();
    this.commits = [];
    this.walFrameCount = 0;
  }

  private nextPageIdAt(txId: number): number {
    for (let i = this.commits.length - 1; i >= 0; i--) {
      if (this.commits[i].txId <= txId) return this.commits[i].nextPageId;
    }
    return this.main.getNextPageId();
  }

  // -------------------------------------------------------------------------
  // Catch-up — read changes made by other writers
  // -------------------------------------------------------------------------

  /** Check if WAL has changed and catch up. Returns set of changed page numbers, or null if nothing changed. */
  catchUp(): Set<number> | null {
    const fileEpoch = this.readEpochFromFile();
    const epochChanged = fileEpoch !== this.epoch;

    if (epochChanged) {
      this.resetAfterRestart();
    }

    const walSize = this.walHandle.getSize();
    const hasNewFrames = walSize > this.walOffset;

    if (!epochChanged && !hasNewFrames) return null;

    return hasNewFrames
      ? this.readFramesFrom(this.walOffset, walSize)
      : new Set<number>();
  }

  /** Another writer restarted the WAL: everything so far is in the main file. */
  private resetAfterRestart(): void {
    this.walIndex.clear();
    this.layeredPages.clear();
    this.commits = [];
    this.walOffset = WAL_HEADER_SIZE;
    this.walFrameCount = 0;
    this.readHeader();
    this.currentTxId = this.baseTxId + 1;
    this.backfilledTxId = this.baseTxId;
    this.nextPageId = this.main.getNextPageId();
  }

  // -------------------------------------------------------------------------
  // Recovery
  // -------------------------------------------------------------------------

  private recover(): void {
    this.readHeader();
    this.currentTxId = this.baseTxId + 1;
    this.backfilledTxId = this.baseTxId;
    const fileSize = this.walHandle.getSize();
    this.readFramesFrom(WAL_HEADER_SIZE, fileSize); // return value unused during recovery
  }

  private readHeader(): void {
    const headerBuf = new Uint8Array(WAL_HEADER_SIZE);
    this.walHandle.read(headerBuf, { at: 0 });
    checkFormatPrefix(headerBuf, "wal", this.fileName);
    const hv = new DataView(headerBuf.buffer);
    this.epoch = hv.getUint32(EPOCH_OFFSET, false);
    this.baseTxId = hv.getUint32(BASE_TX_ID_OFFSET, false);
  }

  /** Read epoch directly from WAL file header (for catch-up checks). */
  private readEpochFromFile(): number {
    const buf = new Uint8Array(4);
    this.walHandle.read(buf, { at: EPOCH_OFFSET });
    return new DataView(buf.buffer).getUint32(0, false);
  }

  /**
   * Read frames from startOffset to endOffset.
   * Only advances walOffset to the end of the last valid commit record.
   * Updates walIndex, commits, nextPageId, currentTxId, walFrameCount.
   * Returns set of committed page numbers.
   */
  private readFramesFrom(startOffset: number, endOffset: number): Set<number> {
    let offset = startOffset;

    // Two-pass: first collect all frames, then filter by committed txIds
    const committed = new Map<number, number>(); // txId → nextPageId
    const allFrames: Array<{
      pageNo: number;
      txId: number;
      payload: Uint8Array;
    }> = [];

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
        committed.set(txId, pv.getUint32(0, false));
        lastCommitEnd = frameEnd;
      } else {
        allFrames.push({ pageNo, txId, payload });
      }

      offset = frameEnd;
    }

    // Add versions from committed frames only, in commit order
    const changedPages = new Set<number>();
    for (const frame of allFrames) {
      if (committed.has(frame.txId)) {
        this.addVersion(frame.pageNo, {
          txId: frame.txId,
          page: { value: decodePage(frame.payload), bytes: frame.payload },
        });
        this.walFrameCount++;
        changedPages.add(frame.pageNo);
      }
    }

    for (const [txId, nextPageId] of committed) {
      this.commits.push({ txId, nextPageId });
      this.nextPageId = nextPageId;
      this.currentTxId = txId + 1;
    }
    // Only advance offset to end of last valid commit record
    this.walOffset = lastCommitEnd;
    return changedPages;
  }

  // -------------------------------------------------------------------------
  // Page versions
  // -------------------------------------------------------------------------

  private addVersion(pageNo: number, version: PageVersion): void {
    const versions = this.walIndex.get(pageNo);
    if (!versions) {
      this.walIndex.set(pageNo, [version]);
      return;
    }
    versions.push(version);
    this.dropInvisible(pageNo, versions);
  }

  /** Keeps the newest version the oldest snapshot can see, and all newer ones. */
  private dropInvisible(pageNo: number, versions: PageVersion[]): void {
    let keepFrom = 0;
    for (let i = versions.length - 1; i > 0; i--) {
      if (versions[i].txId <= this.pruneBound) {
        keepFrom = i;
        break;
      }
    }
    if (keepFrom > 0) versions.splice(0, keepFrom);
    if (versions.length > 1) this.layeredPages.add(pageNo);
    else this.layeredPages.delete(pageNo);
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
    writeFormatPrefix(hdr, "wal");
    const view = new DataView(hdr.buffer);
    view.setUint32(EPOCH_OFFSET, this.epoch, false);
    view.setUint32(BASE_TX_ID_OFFSET, this.baseTxId, false);
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

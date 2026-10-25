import { IoError } from "../../errors.js";
import { crc32 } from "../file/crc32.js";
import { checkPrefix, PREFIX_SIZE, writePrefix } from "../file/file-prefix.js";
import type { SyncFile } from "../file/sync-file.js";

// ---------------------------------------------------------------------------
// The write-ahead log (docs/file-format.md, WAL file), and an index of the
// page versions it holds.
//
// A transaction is appended as its page frames and a commit record, written
// and flushed at once: the flush is the commit point. Transaction ids
// continue across WAL generations (epochs), so an id is a position in the
// database's history and a snapshot is the latest id it sees. The index
// keeps every version an open snapshot may still need.
// ---------------------------------------------------------------------------

const EPOCH_OFFSET = PREFIX_SIZE;
const BASE_OFFSET = PREFIX_SIZE + 4;
const HEADER_SIZE = PREFIX_SIZE + 8;
const FRAME_HEADER_SIZE = 16;
const COMMIT_RECORD = 0xff_ff_ff_ff;
const COMMIT_PAYLOAD_SIZE = 8;

interface Version {
  readonly txId: number;
  readonly page: Uint8Array;
}

interface Commit {
  readonly txId: number;
  readonly pageCount: number;
}

interface Frame {
  readonly pageNo: number;
  readonly txId: number;
  readonly payload: Uint8Array;
}

export class Wal {
  private versions = new Map<number, Version[]>();
  /** Pages holding more than one version: the only ones pruning visits. */
  private readonly layered = new Set<number>();
  private commits: Commit[] = [];
  /** No open snapshot is older than this: older versions can go. */
  private pruneBound = Infinity;
  /** Set when a failed append could not be undone: the tail is unknown. */
  private failure: IoError | null = null;
  private offset = HEADER_SIZE;
  private epoch = 0;
  private base = 0;
  private latest = 0;
  private frames = 0;

  private constructor(
    private readonly file: SyncFile,
    readonly pageSize: number,
    readonly name: string,
  ) {}

  /** Opens the WAL, recovering the transactions it holds, or starts one. */
  static open(file: SyncFile, pageSize: number, name: string): Wal {
    const wal = new Wal(file, pageSize, name);
    if (file.getSize() === 0) wal.writeHeader(0, 0);
    else wal.recover();
    return wal;
  }

  /** The latest committed transaction. */
  get latestTxId(): number {
    return this.latest;
  }

  /** The last transaction before this generation: all of it is in the database file. */
  get baseTxId(): number {
    return this.base;
  }

  /** Page frames committed in this generation, for deciding to checkpoint. */
  get frameCount(): number {
    return this.frames;
  }

  /** The newest version of a page that a snapshot at `txId` sees, or null. */
  versionAt(pageNo: number, txId: number): Uint8Array | null {
    const versions = this.versions.get(pageNo);
    if (versions === undefined) return null;
    for (let index = versions.length - 1; index >= 0; index--) {
      const version = versions[index];
      if (version !== undefined && version.txId <= txId) return version.page;
    }
    return null;
  }

  /** The page count as of `txId`, or null if it predates this generation. */
  pageCountAt(txId: number): number | null {
    for (let index = this.commits.length - 1; index >= 0; index--) {
      const commit = this.commits[index];
      if (commit !== undefined && commit.txId <= txId) return commit.pageCount;
    }
    return null;
  }

  /**
   * Appends a transaction and flushes it. Once this returns, it is durable;
   * if it throws, the appended bytes are removed and nothing is visible.
   */
  append(pages: ReadonlyMap<number, Uint8Array>, pageCount: number): number {
    if (this.failure !== null) throw this.failure;
    const txId = this.latest + 1;
    const bytes = this.encodeTransaction(pages, txId, pageCount);
    try {
      this.file.write(bytes, { at: this.offset });
      this.file.flush();
    } catch (error) {
      this.discardTail();
      throw new IoError(`could not write to "${this.name}"`, { cause: error });
    }
    this.offset += bytes.length;
    this.addTransaction(txId, pages, pageCount);
    return txId;
  }

  /** The oldest snapshot open (null: none); versions no snapshot sees are dropped. */
  setOldestSnapshot(oldest: number | null): void {
    this.pruneBound = oldest ?? Infinity;
    for (const pageNo of this.layered) this.prune(pageNo);
  }

  /** Reads what other tabs appended since. True if anything changed. */
  catchUp(): boolean {
    const header = this.readHeader();
    if (header.epoch !== this.epoch) {
      // Another tab restarted the WAL: everything so far is in the database file.
      this.reset(header.epoch, header.base);
      this.readFrom(HEADER_SIZE);
      return true;
    }
    if (this.file.getSize() <= this.offset) return false;
    const before = this.latest;
    this.readFrom(this.offset);
    return this.latest !== before;
  }

  /** The newest version of every page committed after `after` and up to `upTo`. */
  pagesBetween(after: number, upTo: number): Map<number, Uint8Array> {
    const pages = new Map<number, Uint8Array>();
    for (const [pageNo, versions] of this.versions) {
      const version = versions.findLast((candidate) => candidate.txId <= upTo);
      if (version !== undefined && version.txId > after) {
        pages.set(pageNo, version.page);
      }
    }
    return pages;
  }

  /** Starts an empty generation; every transaction must be in the database file. */
  restart(): void {
    this.reset(this.epoch + 1, this.latest);
    this.file.truncate(0);
    this.writeHeader(this.epoch, this.base);
  }

  close(): void {
    this.file.close();
  }

  // --- Reading the file --------------------------------------------------------

  private recover(): void {
    const header = this.readHeader();
    this.reset(header.epoch, header.base);
    this.readFrom(HEADER_SIZE);
  }

  private readHeader(): { epoch: number; base: number } {
    const header = new Uint8Array(HEADER_SIZE);
    this.file.read(header, { at: 0 });
    checkPrefix(header, "wal", this.name);
    const view = new DataView(header.buffer);
    return {
      epoch: view.getUint32(EPOCH_OFFSET),
      base: view.getUint32(BASE_OFFSET),
    };
  }

  /**
   * Reads frames from `start` and keeps the transactions whose commit record
   * it reaches. It stops at the first frame that is torn, from another
   * generation or out of sequence.
   */
  private readFrom(start: number): void {
    const end = this.file.getSize();
    let pending = new Map<number, Uint8Array>();
    for (let offset = start; ;) {
      const frame = this.readFrame(offset, end);
      if (frame === null) return;
      offset += FRAME_HEADER_SIZE + frame.payload.length;
      if (frame.pageNo !== COMMIT_RECORD) {
        pending.set(frame.pageNo, frame.payload);
        continue;
      }
      if (frame.txId !== this.latest + 1) return;
      const pageCount = new DataView(frame.payload.buffer).getUint32(0);
      this.addTransaction(frame.txId, pending, pageCount);
      this.offset = offset;
      pending = new Map();
    }
  }

  private readFrame(offset: number, end: number): Frame | null {
    if (offset + FRAME_HEADER_SIZE > end) return null;
    const header = new Uint8Array(FRAME_HEADER_SIZE);
    this.file.read(header, { at: offset });
    const view = new DataView(header.buffer);
    const pageNo = view.getUint32(0);
    const size = pageNo === COMMIT_RECORD ? COMMIT_PAYLOAD_SIZE : this.pageSize;
    if (
      view.getUint32(8) !== this.epoch ||
      offset + FRAME_HEADER_SIZE + size > end
    ) {
      return null;
    }
    const payload = new Uint8Array(size);
    this.file.read(payload, { at: offset + FRAME_HEADER_SIZE });
    return crc32(header.subarray(0, 12), payload) === view.getUint32(12)
      ? { pageNo, txId: view.getUint32(4), payload }
      : null;
  }

  // --- Writing the file --------------------------------------------------------

  private writeHeader(epoch: number, base: number): void {
    const header = new Uint8Array(HEADER_SIZE);
    writePrefix(header, "wal");
    const view = new DataView(header.buffer);
    view.setUint32(EPOCH_OFFSET, epoch);
    view.setUint32(BASE_OFFSET, base);
    this.file.write(header, { at: 0 });
    this.file.flush();
    this.epoch = epoch;
    this.base = base;
    this.latest = Math.max(this.latest, base);
  }

  private encodeTransaction(
    pages: ReadonlyMap<number, Uint8Array>,
    txId: number,
    pageCount: number,
  ): Uint8Array {
    const frameSize = FRAME_HEADER_SIZE + this.pageSize;
    const bytes = new Uint8Array(
      pages.size * frameSize + FRAME_HEADER_SIZE + COMMIT_PAYLOAD_SIZE,
    );
    let offset = 0;
    for (const [pageNo, page] of pages) {
      this.encodeFrame(
        bytes.subarray(offset, offset + frameSize),
        pageNo,
        txId,
        page,
      );
      offset += frameSize;
    }
    const commit = new Uint8Array(COMMIT_PAYLOAD_SIZE);
    new DataView(commit.buffer).setUint32(0, pageCount);
    this.encodeFrame(bytes.subarray(offset), COMMIT_RECORD, txId, commit);
    return bytes;
  }

  private encodeFrame(
    target: Uint8Array,
    pageNo: number,
    txId: number,
    payload: Uint8Array,
  ): void {
    const view = new DataView(
      target.buffer,
      target.byteOffset,
      FRAME_HEADER_SIZE,
    );
    view.setUint32(0, pageNo);
    view.setUint32(4, txId);
    view.setUint32(8, this.epoch);
    view.setUint32(12, crc32(target.subarray(0, 12), payload));
    target.set(payload, FRAME_HEADER_SIZE);
  }

  /**
   * Undoes a failed append, so a torn transaction is never recovered as
   * committed. If even that fails, the tail is unknown: no more appends
   * until the database is reopened.
   */
  private discardTail(): void {
    try {
      this.file.truncate(this.offset);
      this.file.flush();
    } catch (error) {
      this.failure = new IoError(
        `"${this.name}" is in an unknown state after a failed write; reopen the database`,
        { cause: error },
      );
    }
  }

  // --- The index ---------------------------------------------------------------

  private reset(epoch: number, base: number): void {
    this.versions = new Map();
    this.layered.clear();
    this.commits = [];
    this.offset = HEADER_SIZE;
    this.frames = 0;
    this.epoch = epoch;
    this.base = base;
    this.latest = base;
  }

  private addTransaction(
    txId: number,
    pages: ReadonlyMap<number, Uint8Array>,
    pageCount: number,
  ): void {
    for (const [pageNo, page] of pages) {
      const versions = this.versions.get(pageNo);
      if (versions === undefined) {
        this.versions.set(pageNo, [{ txId, page }]);
        continue;
      }
      versions.push({ txId, page });
      this.prune(pageNo);
    }
    this.commits.push({ txId, pageCount });
    this.frames += pages.size;
    this.latest = txId;
  }

  /** Keeps the newest version the oldest snapshot sees, and every newer one. */
  private prune(pageNo: number): void {
    const versions = this.versions.get(pageNo) ?? [];
    const keepFrom = versions.findLastIndex(
      (version, index) => index > 0 && version.txId <= this.pruneBound,
    );
    if (keepFrom > 0) versions.splice(0, keepFrom);
    if (versions.length > 1) this.layered.add(pageNo);
    else this.layered.delete(pageNo);
  }
}

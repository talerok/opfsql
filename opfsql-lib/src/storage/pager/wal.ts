import { IoError } from "../../errors.js";
import { crc32 } from "../file/crc32.js";
import { readUint32, writeUint32 } from "../format/bytes.js";
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
const FRAME_PAGE_OFFSET = 0;
const FRAME_TX_OFFSET = 4;
const FRAME_EPOCH_OFFSET = 8;
const FRAME_CHECKSUM_OFFSET = 12;
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
    return versions?.findLast((version) => version.txId <= txId)?.page ?? null;
  }

  /** The page count as of `txId`, or null if it predates this generation. */
  pageCountAt(txId: number): number | null {
    const commit = this.commits.findLast((candidate) => candidate.txId <= txId);
    return commit?.pageCount ?? null;
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
    return {
      epoch: readUint32(header, EPOCH_OFFSET),
      base: readUint32(header, BASE_OFFSET),
    };
  }

  /**
   * Reads frames from `start` and keeps the transactions whose commit record
   * it reaches. It stops at the first frame that is torn, from another
   * generation or out of sequence.
   */
  private readFrom(start: number): void {
    let pending = new Map<number, Uint8Array>();
    for (const { frame, end } of this.framesFrom(start)) {
      if (frame.pageNo !== COMMIT_RECORD) {
        pending.set(frame.pageNo, frame.payload);
        continue;
      }
      if (frame.txId !== this.latest + 1) return;
      this.addTransaction(frame.txId, pending, pageCountOf(frame));
      this.offset = end;
      pending = new Map();
    }
  }

  /** The intact frames from `start` on, each with the offset it ends at. */
  private *framesFrom(start: number): Generator<{ frame: Frame; end: number }> {
    const fileSize = this.file.getSize();
    let offset = start;
    let frame = this.readFrame(offset, fileSize);
    while (frame !== null) {
      offset += frameSize(frame);
      yield { frame, end: offset };
      frame = this.readFrame(offset, fileSize);
    }
  }

  /** The frame at `offset`, or null if it is incomplete, torn or from another generation. */
  private readFrame(offset: number, fileSize: number): Frame | null {
    if (offset + FRAME_HEADER_SIZE > fileSize) return null;
    const header = new Uint8Array(FRAME_HEADER_SIZE);
    this.file.read(header, { at: offset });
    const pageNo = readUint32(header, FRAME_PAGE_OFFSET);
    const payloadSize =
      pageNo === COMMIT_RECORD ? COMMIT_PAYLOAD_SIZE : this.pageSize;
    const isComplete = offset + FRAME_HEADER_SIZE + payloadSize <= fileSize;
    const epoch = readUint32(header, FRAME_EPOCH_OFFSET);
    if (!isComplete || epoch !== this.epoch) return null;

    const payload = new Uint8Array(payloadSize);
    this.file.read(payload, { at: offset + FRAME_HEADER_SIZE });
    if (!isIntact(header, payload)) return null;
    return { pageNo, txId: readUint32(header, FRAME_TX_OFFSET), payload };
  }

  // --- Writing the file --------------------------------------------------------

  private writeHeader(epoch: number, base: number): void {
    const header = new Uint8Array(HEADER_SIZE);
    writePrefix(header, "wal");
    writeUint32(header, EPOCH_OFFSET, epoch);
    writeUint32(header, BASE_OFFSET, base);
    this.file.write(header, { at: 0 });
    this.file.flush();
    this.epoch = epoch;
    this.base = base;
    this.latest = Math.max(this.latest, base);
  }

  /** The frames of a transaction: its pages, then its commit record. */
  private encodeTransaction(
    pages: ReadonlyMap<number, Uint8Array>,
    txId: number,
    pageCount: number,
  ): Uint8Array {
    const frames: Frame[] = [];
    for (const [pageNo, payload] of pages) {
      frames.push({ pageNo, txId, payload });
    }
    const commit = commitPayload(pageCount);
    frames.push({ pageNo: COMMIT_RECORD, txId, payload: commit });

    const sizes = frames.map((frame) => frameSize(frame));
    const bytes = new Uint8Array(
      sizes.reduce((total, size) => total + size, 0),
    );
    let offset = 0;
    for (const frame of frames) {
      encodeFrame(bytes.subarray(offset), frame, this.epoch);
      offset += frameSize(frame);
    }
    return bytes;
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
    for (const [pageNo, page] of pages) this.addVersion(pageNo, { txId, page });
    this.commits.push({ txId, pageCount });
    this.frames += pages.size;
    this.latest = txId;
  }

  private addVersion(pageNo: number, version: Version): void {
    const versions = this.versions.get(pageNo);
    if (versions === undefined) {
      this.versions.set(pageNo, [version]);
      return;
    }
    versions.push(version);
    this.prune(pageNo);
  }

  /**
   * Drops the versions no snapshot can see: those older than the newest one
   * the oldest open snapshot sees.
   */
  private prune(pageNo: number): void {
    const versions = this.versions.get(pageNo) ?? [];
    const oldestVisible = versions.findLastIndex(
      (version) => version.txId <= this.pruneBound,
    );
    if (oldestVisible > 0) versions.splice(0, oldestVisible);
    if (versions.length > 1) this.layered.add(pageNo);
    else this.layered.delete(pageNo);
  }
}

function frameSize(frame: Frame): number {
  return FRAME_HEADER_SIZE + frame.payload.length;
}

/** Writes `frame`, header then payload, at the start of `target`. */
function encodeFrame(target: Uint8Array, frame: Frame, epoch: number): void {
  writeUint32(target, FRAME_PAGE_OFFSET, frame.pageNo);
  writeUint32(target, FRAME_TX_OFFSET, frame.txId);
  writeUint32(target, FRAME_EPOCH_OFFSET, epoch);
  const header = target.subarray(0, FRAME_HEADER_SIZE);
  writeUint32(target, FRAME_CHECKSUM_OFFSET, checksumOf(header, frame.payload));
  target.set(frame.payload, FRAME_HEADER_SIZE);
}

/** The checksum covers the header up to the checksum itself, and the payload. */
function checksumOf(header: Uint8Array, payload: Uint8Array): number {
  return crc32(header.subarray(0, FRAME_CHECKSUM_OFFSET), payload);
}

function isIntact(header: Uint8Array, payload: Uint8Array): boolean {
  const stored = readUint32(header, FRAME_CHECKSUM_OFFSET);
  return checksumOf(header, payload) === stored;
}

/** A commit record's payload: the page count, then zero. */
function commitPayload(pageCount: number): Uint8Array {
  const payload = new Uint8Array(COMMIT_PAYLOAD_SIZE);
  writeUint32(payload, 0, pageCount);
  return payload;
}

function pageCountOf(commit: Frame): number {
  return readUint32(commit.payload, 0);
}

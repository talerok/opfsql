import { CorruptError, fileFailure, IoError } from "../../errors.js";
import { crc32 } from "../file/crc32.js";
import { checkPrefix, PREFIX_SIZE, writePrefix } from "../file/file-prefix.js";
import type { SyncFile } from "../file/sync-file.js";
import { readUint32, writeUint32 } from "../format/bytes.js";
import { WalIndex } from "./wal-index.js";

// ---------------------------------------------------------------------------
// The write-ahead log (docs/file-format.md, WAL file): reading, appending and
// restarting it, and catching up with what other tabs wrote to it. What has
// been read of it is kept in a WalIndex.
//
// A transaction is appended as its page frames and a commit record, written
// and flushed at once: the flush is the commit point. Transaction ids
// continue across WAL generations (epochs), so an id is a position in the
// database's history and a snapshot is the latest id it sees.
// ---------------------------------------------------------------------------

const EPOCH_OFFSET = PREFIX_SIZE;
const BASE_OFFSET = PREFIX_SIZE + 4;
const HEADER_CHECKSUM_OFFSET = PREFIX_SIZE + 8;
const HEADER_SIZE = PREFIX_SIZE + 12;
/**
 * Reads of a header whose checksum fails before it counts as corrupt: one
 * that fails is being rewritten by another tab, which takes microseconds.
 */
const HEADER_READ_ATTEMPTS = 100;
const FRAME_PAGE_OFFSET = 0;
const FRAME_TX_OFFSET = 4;
const FRAME_EPOCH_OFFSET = 8;
const FRAME_CHECKSUM_OFFSET = 12;
const FRAME_HEADER_SIZE = 16;
/** The page number a commit record has in its frame header. */
const COMMIT_RECORD = 0xff_ff_ff_ff;
const COMMIT_PAYLOAD_SIZE = 8;

interface WalHeader {
  readonly epoch: number;
  /** The last transaction before this generation. */
  readonly base: number;
}

/** What a frame holds: a version of a page, or the record that commits a transaction. */
type Frame =
  | {
      readonly kind: "page";
      readonly txId: number;
      readonly pageNo: number;
      readonly page: Uint8Array;
    }
  | {
      readonly kind: "commit";
      readonly txId: number;
      readonly pageCount: number;
    };

export class Wal {
  private readonly index = new WalIndex();
  /** Where the next frame goes: the end of the last commit read or written. */
  private offset = HEADER_SIZE;
  private epoch = 0;
  /**
   * Why a failed append could not be undone, once one could not: the tail
   * is unknown, and every later append fails with its own error, caused by
   * this (an error object is never shared between calls).
   */
  private failure: { readonly cause: unknown } | null = null;
  /** Where the header is read to, every time: nothing read from it is kept. */
  private readonly headerBytes = new Uint8Array(HEADER_SIZE);

  private constructor(
    private readonly file: SyncFile,
    readonly pageSize: number,
    readonly name: string,
  ) {}

  /** Opens the WAL, recovering the transactions it holds, or starts one. */
  static open(file: SyncFile, pageSize: number, name: string): Wal {
    const wal = new Wal(file, pageSize, name);
    if (file.getSize() === 0) wal.writeHeader();
    else wal.recover();
    return wal;
  }

  // What has been read of the WAL, as the WalIndex keeps it.

  get latestTxId(): number {
    return this.index.latestTxId;
  }

  get baseTxId(): number {
    return this.index.baseTxId;
  }

  get frameCount(): number {
    return this.index.frameCount;
  }

  versionAt(pageNo: number, txId: number): Uint8Array | null {
    return this.index.versionAt(pageNo, txId);
  }

  pageCountAt(txId: number): number | null {
    return this.index.pageCountAt(txId);
  }

  pagesBetween(after: number, upTo: number): Map<number, Uint8Array> {
    return this.index.pagesBetween(after, upTo);
  }

  setOldestSnapshot(oldest: number | null): void {
    this.index.setOldestSnapshot(oldest);
  }

  /**
   * Appends a transaction and flushes it. Once this returns, it is durable;
   * if it throws, the appended bytes are removed and nothing is visible. The
   * index keeps the pages as they were written, its own bytes: no buffer a
   * transaction held becomes a committed version (docs/layers.md).
   */
  append(pages: ReadonlyMap<number, Uint8Array>, pageCount: number): number {
    if (this.failure !== null) throw this.unknownTail(this.failure.cause);
    const txId = this.index.latestTxId + 1;
    const frames = transactionFrames(pages, txId, pageCount);
    const { bytes, written } = this.encodeFrames(frames);
    try {
      this.file.write(bytes, { at: this.offset });
      this.file.flush();
    } catch (error) {
      this.discardTail();
      throw fileFailure(`could not write to "${this.name}"`, error);
    }
    this.offset += bytes.length;
    this.index.addTransaction(txId, written, pageCount);
    return txId;
  }

  /**
   * Reads what other tabs appended since, or the generation another tab
   * restarted the WAL into; true if anything changed. The header is read
   * again after the frames, and only an unchanged epoch makes what was read
   * one generation: a restart writes its header before it cuts the old
   * frames off (docs/concurrency.md, Catching up).
   */
  catchUp(): boolean {
    const { epoch } = this;
    const latest = this.index.latestTxId;
    for (;;) {
      const header = this.readHeader();
      if (header.epoch !== this.epoch) this.reset(header);
      this.readFrom(this.offset);
      if (this.readHeader().epoch === header.epoch) break;
    }
    return this.epoch !== epoch || this.index.latestTxId !== latest;
  }

  /**
   * Starts an empty generation; every transaction must be in the database
   * file. The new header is written before the old frames are cut off, so a
   * tab reading meanwhile finds one header or the other, never none, and
   * under the new one the old frames' epoch rejects them.
   */
  restart(): void {
    this.reset({ epoch: this.epoch + 1, base: this.index.latestTxId });
    this.writeHeader();
    this.file.truncate(HEADER_SIZE);
  }

  close(): void {
    this.file.close();
  }

  // --- Reading the file --------------------------------------------------------

  /** Reads the generation the WAL holds, as a tab opening it finds it. */
  private recover(): void {
    this.reset(this.readHeader());
    this.catchUp();
  }

  /** The header, read again while another tab is rewriting it. */
  private readHeader(): WalHeader {
    for (let attempt = 0; attempt < HEADER_READ_ATTEMPTS; attempt++) {
      const header = this.headerBytes;
      this.file.read(header, { at: 0 });
      checkPrefix(header, "wal", this.name);
      if (
        readUint32(header, HEADER_CHECKSUM_OFFSET) === headerChecksum(header)
      ) {
        return {
          epoch: readUint32(header, EPOCH_OFFSET),
          base: readUint32(header, BASE_OFFSET),
        };
      }
    }
    throw new CorruptError(
      `"${this.name}" has a WAL header that fails its checksum`,
    );
  }

  /**
   * Reads frames from `start` and keeps the transactions whose commit record
   * it reaches. It stops at the first frame that is torn, from another
   * generation or out of sequence.
   */
  private readFrom(start: number): void {
    let pending = new Map<number, Uint8Array>();
    for (const { frame, end } of this.framesFrom(start)) {
      if (frame.txId !== this.index.latestTxId + 1) return;
      if (frame.kind === "page") {
        pending.set(frame.pageNo, frame.page);
        continue;
      }
      this.index.addTransaction(frame.txId, pending, frame.pageCount);
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
    const header = this.readBytes(offset, FRAME_HEADER_SIZE);
    const isCommit = readUint32(header, FRAME_PAGE_OFFSET) === COMMIT_RECORD;
    const payloadSize = isCommit ? COMMIT_PAYLOAD_SIZE : this.pageSize;
    const isComplete = offset + FRAME_HEADER_SIZE + payloadSize <= fileSize;
    const epoch = readUint32(header, FRAME_EPOCH_OFFSET);
    if (!isComplete || epoch !== this.epoch) return null;

    const payload = this.readBytes(offset + FRAME_HEADER_SIZE, payloadSize);
    return isIntact(header, payload) ? decodeFrame(header, payload) : null;
  }

  private readBytes(offset: number, size: number): Uint8Array {
    const bytes = new Uint8Array(size);
    this.file.read(bytes, { at: offset });
    return bytes;
  }

  // --- Writing the file --------------------------------------------------------

  private writeHeader(): void {
    const header = new Uint8Array(HEADER_SIZE);
    writePrefix(header, "wal");
    writeUint32(header, EPOCH_OFFSET, this.epoch);
    writeUint32(header, BASE_OFFSET, this.index.baseTxId);
    writeUint32(header, HEADER_CHECKSUM_OFFSET, headerChecksum(header));
    this.file.write(header, { at: 0 });
    this.file.flush();
  }

  /** The frames as bytes, and each page as it lies among them. */
  private encodeFrames(frames: readonly Frame[]): {
    bytes: Uint8Array;
    written: Map<number, Uint8Array>;
  } {
    const size = frames.reduce((total, frame) => total + frameSize(frame), 0);
    const bytes = new Uint8Array(size);
    const written = new Map<number, Uint8Array>();
    let offset = 0;
    for (const frame of frames) {
      encodeFrame(bytes.subarray(offset), frame, this.epoch);
      if (frame.kind === "page") {
        const start = offset + FRAME_HEADER_SIZE;
        written.set(frame.pageNo, bytes.subarray(start, start + this.pageSize));
      }
      offset += frameSize(frame);
    }
    return { bytes, written };
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
      this.failure = { cause: error };
    }
  }

  private unknownTail(cause: unknown): IoError {
    return new IoError(
      `"${this.name}" is in an unknown state after a failed write; reopen the database`,
      { cause },
    );
  }

  /** Starts reading the generation `header` describes, from its first frame. */
  private reset({ epoch, base }: WalHeader): void {
    this.index.reset(base);
    this.offset = HEADER_SIZE;
    this.epoch = epoch;
  }
}

/** A transaction's frames: its pages, then its commit record. */
function transactionFrames(
  pages: ReadonlyMap<number, Uint8Array>,
  txId: number,
  pageCount: number,
): Frame[] {
  const frames: Frame[] = [];
  for (const [pageNo, page] of pages) {
    frames.push({ kind: "page", txId, pageNo, page });
  }
  frames.push({ kind: "commit", txId, pageCount });
  return frames;
}

function frameSize(frame: Frame): number {
  const payloadSize =
    frame.kind === "page" ? frame.page.length : COMMIT_PAYLOAD_SIZE;
  return FRAME_HEADER_SIZE + payloadSize;
}

/** Writes `frame`, header then payload, at the start of `target`. */
function encodeFrame(target: Uint8Array, frame: Frame, epoch: number): void {
  const pageNo = frame.kind === "page" ? frame.pageNo : COMMIT_RECORD;
  writeUint32(target, FRAME_PAGE_OFFSET, pageNo);
  writeUint32(target, FRAME_TX_OFFSET, frame.txId);
  writeUint32(target, FRAME_EPOCH_OFFSET, epoch);
  const payload = payloadOf(frame);
  const header = target.subarray(0, FRAME_HEADER_SIZE);
  writeUint32(target, FRAME_CHECKSUM_OFFSET, checksumOf(header, payload));
  target.set(payload, FRAME_HEADER_SIZE);
}

/** A page frame's payload is the page; a commit record's, the page count and a zero. */
function payloadOf(frame: Frame): Uint8Array {
  if (frame.kind === "page") return frame.page;
  const payload = new Uint8Array(COMMIT_PAYLOAD_SIZE);
  writeUint32(payload, 0, frame.pageCount);
  return payload;
}

function decodeFrame(header: Uint8Array, payload: Uint8Array): Frame {
  const pageNo = readUint32(header, FRAME_PAGE_OFFSET);
  const txId = readUint32(header, FRAME_TX_OFFSET);
  if (pageNo === COMMIT_RECORD) {
    return { kind: "commit", txId, pageCount: readUint32(payload, 0) };
  }
  return { kind: "page", txId, pageNo, page: payload };
}

/** The checksum covers the header up to the checksum itself, and the payload. */
function checksumOf(header: Uint8Array, payload: Uint8Array): number {
  return crc32(header.subarray(0, FRAME_CHECKSUM_OFFSET), payload);
}

function isIntact(header: Uint8Array, payload: Uint8Array): boolean {
  const stored = readUint32(header, FRAME_CHECKSUM_OFFSET);
  return checksumOf(header, payload) === stored;
}

/** CRC-32 of a WAL header's bytes before its checksum. */
function headerChecksum(header: Uint8Array): number {
  return crc32(header.subarray(0, HEADER_CHECKSUM_OFFSET));
}

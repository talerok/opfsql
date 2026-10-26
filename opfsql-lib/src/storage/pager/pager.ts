import { InternalError } from "../../errors.js";
import type { PageReader } from "../btree/page-store.js";
import { AllocationMapReader } from "./allocation.js";
import type { DatabaseFile } from "./database-file.js";
import type { ReadMarks } from "./read-marks.js";
import { ReadTransaction, WriteTransaction } from "./transaction.js";
import type { Wal } from "./wal.js";

// ---------------------------------------------------------------------------
// The pages of a database at any snapshot: the newest version in the WAL that
// the snapshot sees, else the database file. How tabs share the files — the
// intent, the read marks, who may change what — is docs/concurrency.md; this
// is its implementation, and the rule numbers below are that document's.
//
// Every operation reads what other tabs committed before it decides
// anything, so no caller has to remember to: a decision made on a view that
// missed a commit could cut that commit off the WAL. Writing, checkpointing
// and lowering an intent need the write lock, which the caller holds.
// ---------------------------------------------------------------------------

export interface PagerOptions {
  /** WAL frames after which a checkpoint is due. */
  readonly checkpointThreshold: number;
  /** Snapshots of other tabs sharing the files; null for a database alone. */
  readonly marks: ReadMarks | null;
}

export class Pager {
  /** Pinned snapshots: transaction id → how many transactions read it. */
  private readonly pins = new Map<number, number>();
  /** The database file holds every transaction up to this one. */
  private backfilled: number;

  constructor(
    private readonly file: DatabaseFile,
    private readonly wal: Wal,
    private readonly options: PagerOptions,
  ) {
    this.backfilled = wal.baseTxId;
  }

  get pageSize(): number {
    return this.file.pageSize;
  }

  /** The latest committed transaction this tab knows of. */
  latestCommit(): number {
    return this.wal.latestTxId;
  }

  /**
   * A transaction reading the latest commit, or null while a checkpoint
   * intent no commit backs refuses every snapshot: then take the write lock,
   * resetStaleIntent, and begin again.
   */
  beginRead(): ReadTransaction | null {
    this.catchUp();
    let snapshot = this.pin();
    // A checkpoint passed the snapshot meanwhile: its commits are in the
    // WAL, so catching up finds them, and a newer snapshot holds.
    while (snapshot === null && this.catchUp()) snapshot = this.pin();
    return snapshot === null ? null : new ReadTransaction(this, snapshot);
  }

  /**
   * A transaction writing on top of the latest commit. The caller holds the
   * write lock, so no other tab commits or checkpoints meanwhile.
   */
  beginWrite(): WriteTransaction {
    this.resetStaleIntent();
    const snapshot = this.pin();
    if (snapshot === null) {
      throw new InternalError("the latest commit was refused under the lock");
    }
    return new WriteTransaction(this, snapshot);
  }

  /**
   * The page as of `snapshot`, which must be pinned: only a pin keeps a
   * checkpoint, in this tab or another, from rewriting what it reads.
   * Shared: never modify it.
   */
  read(pageNo: number, snapshot: number): Uint8Array {
    if (!this.pins.has(snapshot)) {
      throw new InternalError(
        `page ${String(pageNo)} read at snapshot ${String(snapshot)}, which is not pinned`,
      );
    }
    return this.wal.versionAt(pageNo, snapshot) ?? this.file.read(pageNo);
  }

  /** Pages in the database as of `snapshot`. */
  pageCount(snapshot: number): number {
    return this.wal.pageCountAt(snapshot) ?? this.file.pageCount;
  }

  /**
   * Appends the pages of a write transaction begun at `snapshot` as the
   * next commit; returns its id. Called by WriteTransaction. The write lock
   * is not visible here, so it is proven where breaking it would do harm:
   * other tabs' commits are read first, and finding one means the lock was
   * not held — nothing is written over it.
   */
  append(
    pages: ReadonlyMap<number, Uint8Array>,
    pageCount: number,
    snapshot: number,
  ): number {
    this.catchUp();
    if (this.latestCommit() !== snapshot) {
      throw new InternalError(
        `a commit was made after snapshot ${String(snapshot)} while it was being written on: the write lock was not held`,
      );
    }
    return this.wal.append(pages, pageCount);
  }

  unpin(snapshot: number): void {
    this.release(snapshot);
  }

  /**
   * Lowers a checkpoint intent no commit backs: one left in the read-mark
   * file by an earlier database of the same name, whose files were removed
   * and the read marks kept. Such an intent is ahead of every commit, so
   * beginRead would refuse every snapshot. The caller holds the write lock,
   * so no checkpoint runs, and after catching up the latest commit is the
   * database's; true if the intent was lowered.
   */
  resetStaleIntent(): boolean {
    this.catchUp();
    const marks = this.options.marks;
    if (marks === null) return false;
    const latest = this.latestCommit();
    if (marks.checkpointIntent() <= latest) return false;
    marks.setCheckpointIntent(latest);
    return true;
  }

  isCheckpointDue(): boolean {
    return this.wal.frameCount >= this.options.checkpointThreshold;
  }

  /**
   * Copies committed pages into the database file, as far as the open
   * snapshots of every tab allow, and restarts the WAL once all of it is
   * there, truncating the free pages at the end of the file. The caller holds
   * the write lock.
   */
  checkpoint(): void {
    this.catchUp();
    const latest = this.latestCommit();
    this.backfill(this.copyLimit(latest));
    const isAllInFile = this.backfilled === latest;
    if (isAllInFile && this.wal.frameCount > 0) this.restartWal();
  }

  /** Clears read marks left by tabs that died with a snapshot open. */
  async releaseAbandonedSnapshots(): Promise<void> {
    await this.options.marks?.reclaimDeadSlots();
  }

  close(): void {
    this.options.marks?.close();
    this.wal.close();
    this.file.close();
  }

  // --- Snapshots ---------------------------------------------------------------

  /** Reads what other tabs committed. True if anything changed. */
  private catchUp(): boolean {
    const hasChanged = this.wal.catchUp();
    this.backfilled = Math.max(this.backfilled, this.wal.baseTxId);
    return hasChanged;
  }

  /**
   * Pins the latest commit. The pin is published before the checkpoint
   * intent is read, so a checkpoint that announces its target later sees it;
   * one that announced a target past the snapshot already wins.
   */
  private pin(): number | null {
    const snapshot = this.latestCommit();
    this.hold(snapshot);
    const intent = this.options.marks?.checkpointIntent() ?? 0;
    if (intent <= snapshot) return snapshot;
    this.release(snapshot);
    return null;
  }

  private hold(snapshot: number): void {
    this.pins.set(snapshot, (this.pins.get(snapshot) ?? 0) + 1);
    this.publishOldest();
  }

  private release(snapshot: number): void {
    const count = (this.pins.get(snapshot) ?? 0) - 1;
    if (count > 0) this.pins.set(snapshot, count);
    else this.pins.delete(snapshot);
    this.publishOldest();
  }

  /** Tells the WAL which versions it may drop, and other tabs what to keep. */
  private publishOldest(): void {
    const oldest = this.oldestPin();
    this.wal.setOldestSnapshot(oldest);
    this.options.marks?.publish(oldest);
  }

  private oldestPin(): number | null {
    return this.pins.size === 0 ? null : Math.min(...this.pins.keys());
  }

  /**
   * How far a checkpoint may copy: up to `latest`, past no snapshot a tab
   * has pinned or is pinning. The intent is raised to `latest` before the
   * marks are read, so a tab that publishes an older mark afterwards sees it
   * and pins a newer snapshot. It is never lowered (rule 1): a tab that had
   * published an old mark and then read a lowered intent would pin a
   * snapshot the file has passed.
   */
  private copyLimit(latest: number): number {
    const local = this.oldestPin() ?? latest;
    const marks = this.options.marks;
    if (marks === null) return Math.min(latest, local);
    if (marks.checkpointIntent() < latest) marks.setCheckpointIntent(latest);
    const oldest = marks.oldestMark() ?? latest;
    return Math.min(latest, local, oldest);
  }

  private backfill(target: number): void {
    if (target <= this.backfilled) return;
    this.file.write(this.wal.pagesBetween(this.backfilled, target));
    this.file.flush();
    this.backfilled = target;
  }

  private restartWal(): void {
    const latest = this.latestCommit();
    this.hold(latest);
    try {
      this.truncateFreeTail(latest);
    } finally {
      this.release(latest);
    }
    this.wal.restart();
  }

  private truncateFreeTail(snapshot: number): void {
    const map = new AllocationMapReader(this.pagesAt(snapshot));
    const lastInUse = map.highestInUse(this.pageCount(snapshot));
    const pageCount = lastInUse + 1;
    if (pageCount >= this.file.pageCount) return;
    this.file.resize(pageCount);
    this.file.flush();
  }

  private pagesAt(snapshot: number): PageReader {
    return {
      pageSize: this.pageSize,
      read: (pageNo) => this.read(pageNo, snapshot),
    };
  }
}

import { highestInUse } from "./allocation.js";
import type { DatabaseFile } from "./database-file.js";
import type { ReadMarks } from "./read-marks.js";
import { ReadTransaction, WriteTransaction } from "./transaction.js";
import type { Wal } from "./wal.js";

// ---------------------------------------------------------------------------
// The pages of a database at any snapshot: the newest version in the WAL that
// the snapshot sees, else the database file.
//
// Snapshots are pinned while a transaction reads them. A checkpoint copies
// committed pages into the database file, but never past the oldest
// snapshot open in any tab: each tab publishes its oldest in the read marks,
// and a checkpoint announces its target (the intent) before reading them, so
// a tab that publishes an older snapshot afterwards sees the intent and pins
// a newer one instead — the SQLite WAL protocol.
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
   * A transaction reading the latest commit, or null when another tab's
   * checkpoint may already have passed it: catch up and try again.
   */
  beginRead(): ReadTransaction | null {
    const snapshot = this.pin();
    return snapshot === null ? null : new ReadTransaction(this, snapshot);
  }

  /**
   * A transaction that writes on top of the latest commit. The caller holds
   * the write lock and has caught up, so no one else commits meanwhile.
   */
  beginWrite(): WriteTransaction | null {
    const snapshot = this.pin();
    return snapshot === null ? null : new WriteTransaction(this, snapshot);
  }

  /** The page as of `snapshot`. Shared: never modify it. */
  read(pageNo: number, snapshot: number): Uint8Array {
    return this.wal.versionAt(pageNo, snapshot) ?? this.file.read(pageNo);
  }

  /** Pages in the database as of `snapshot`. */
  pageCount(snapshot: number): number {
    return this.wal.pageCountAt(snapshot) ?? this.file.pageCount;
  }

  /** Appends a transaction's pages; returns its id. Called by WriteTransaction. */
  append(pages: ReadonlyMap<number, Uint8Array>, pageCount: number): number {
    return this.wal.append(pages, pageCount);
  }

  unpin(snapshot: number): void {
    this.addPin(snapshot, -1);
  }

  /** Reads what other tabs committed. True if anything changed. */
  catchUp(): boolean {
    const hasChanged = this.wal.catchUp();
    this.backfilled = Math.max(this.backfilled, this.wal.baseTxId);
    return hasChanged;
  }

  isCheckpointDue(): boolean {
    return this.wal.frameCount >= this.options.checkpointThreshold;
  }

  /**
   * Copies committed pages into the database file, as far as the open
   * snapshots of every tab allow, and restarts the WAL once all of it is
   * there, truncating the free pages at the end of the file.
   */
  checkpoint(): void {
    const target = this.agreeOnTarget(this.oldestPin() ?? this.latestCommit());
    if (target > this.backfilled) {
      this.file.write(this.wal.pagesBetween(this.backfilled, target));
      this.file.flush();
      this.backfilled = target;
    }
    if (this.backfilled !== this.latestCommit() || this.wal.frameCount === 0) {
      return;
    }
    this.truncateFreeTail(this.latestCommit());
    this.wal.restart();
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

  private pin(): number | null {
    const snapshot = this.latestCommit();
    this.addPin(snapshot, 1);
    const intent = this.options.marks?.checkpointIntent() ?? 0;
    if (intent <= snapshot) return snapshot;
    this.addPin(snapshot, -1);
    return null;
  }

  private addPin(snapshot: number, delta: 1 | -1): void {
    const count = (this.pins.get(snapshot) ?? 0) + delta;
    if (count > 0) this.pins.set(snapshot, count);
    else this.pins.delete(snapshot);
    const oldest = this.oldestPin();
    this.wal.setOldestSnapshot(oldest);
    this.options.marks?.publish(oldest);
  }

  private oldestPin(): number | null {
    let oldest: number | null = null;
    for (const snapshot of this.pins.keys()) {
      if (oldest === null || snapshot < oldest) oldest = snapshot;
    }
    return oldest;
  }

  /** Lowers `target` to the oldest snapshot any tab has open. */
  private agreeOnTarget(target: number): number {
    const marks = this.options.marks;
    if (marks === null) return target;
    for (let agreed = target; ;) {
      marks.setCheckpointIntent(agreed);
      const oldest = marks.oldestMark();
      if (oldest === null || oldest >= agreed) return agreed;
      agreed = oldest;
    }
  }

  private truncateFreeTail(snapshot: number): void {
    const pageCount =
      highestInUse(
        (pageNo) => this.read(pageNo, snapshot),
        this.pageSize,
        this.pageCount(snapshot),
      ) + 1;
    if (pageCount >= this.file.pageCount) return;
    this.file.resize(pageCount);
    this.file.flush();
  }
}

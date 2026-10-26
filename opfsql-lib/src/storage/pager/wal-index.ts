// ---------------------------------------------------------------------------
// The WAL's current generation as one tab has read it, kept in memory: each
// committed version of every page the generation changed, and each commit's
// page count. It is a cache of the WAL file (docs/concurrency.md, Shared
// state): the Wal fills it as it appends and catches up, and empties it when
// a generation starts. It keeps every version an open snapshot may still
// need, and drops the rest.
// ---------------------------------------------------------------------------

interface Version {
  readonly txId: number;
  readonly page: Uint8Array;
}

interface Commit {
  readonly txId: number;
  readonly pageCount: number;
}

export class WalIndex {
  private versions = new Map<number, Version[]>();
  /** Pages holding more than one version: the only ones pruning visits. */
  private readonly layered = new Set<number>();
  private commits: Commit[] = [];
  /** No open snapshot is older than this: older versions can go. */
  private pruneBound = Infinity;
  private frames = 0;
  private base = 0;
  private latest = 0;

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

  /** The oldest snapshot open (null: none); versions no snapshot sees are dropped. */
  setOldestSnapshot(oldest: number | null): void {
    this.pruneBound = oldest ?? Infinity;
    for (const pageNo of this.layered) this.prune(pageNo);
  }

  /** Empties the index for a generation whose base is `base`. */
  reset(base: number): void {
    this.versions = new Map();
    this.layered.clear();
    this.commits = [];
    this.frames = 0;
    this.base = base;
    this.latest = base;
  }

  /** Adds a committed transaction: the next after the latest. */
  addTransaction(
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

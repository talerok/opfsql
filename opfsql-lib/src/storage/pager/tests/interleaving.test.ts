import { describe, expect, it } from "vitest";
import type { SyncFile } from "../../file/sync-file.js";
import { MemoryFile } from "../../file/sync-file.js";
import { openPager, type StorageFiles } from "../../open.js";
import { AllocationMapReader } from "../allocation.js";
import type { Pager } from "../pager.js";
import type { ReadTransaction } from "../transaction.js";

// ---------------------------------------------------------------------------
// Tabs sharing one database's files, interleaved (docs/file-format.md, Read-
// mark file). Each tab is a pager over its own view of the shared files; at
// every file operation of one tab, the scheduler may first run a whole step
// of another — the closest one thread gets to tabs running at once. Two
// checks, the invariants of docs/concurrency.md: each tracked page a
// snapshot reads holds what the model says the commits up to it wrote; every
// page of a snapshot held across steps is byte for byte what it was when
// first read; and the intent never falls. A step that lets a checkpoint
// rewrite a page under a reader, or lets a reader see half a restart, fails
// with its seed.
// ---------------------------------------------------------------------------

const PAGE_SIZE = 4096;
const TABS = 3;
/** Pages whose stamps every read checks. */
const TRACKED_PAGES = 5;
const STEPS = 120;
/** How often a file operation lets another tab run first, and how deep. */
const INTERLEAVE_CHANCE = 0.2;
const MAX_NESTING = 2;
/** Low, so the WAL restarts often. */
const CHECKPOINT_THRESHOLD = 6;
/** docs/file-format.md, Read-mark file: the intent is at 16. */
const INTENT_OFFSET = 16;

/** mulberry32: a small seeded generator, so a failure replays from its seed. */
class Random {
  constructor(private state: number) {}

  next(): number {
    this.state = (this.state + 0x6d_2b_79_f5) >>> 0;
    let t = this.state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32;
  }

  chance(probability: number): boolean {
    return this.next() < probability;
  }

  below(count: number): number {
    return Math.floor(this.next() * count);
  }

  pick<T>(items: readonly T[]): T {
    const item = items[this.below(items.length)];
    if (item === undefined) throw new Error("picked from no items");
    return item;
  }
}

/** What each commit wrote: the stamp it left on each tracked page. */
class Model {
  private readonly commits: { txId: number; stamps: Map<number, number> }[] =
    [];

  record(txId: number, stamps: Map<number, number>): void {
    this.commits.push({ txId, stamps });
  }

  /** The stamp `pageNo` has as of `snapshot`. */
  stampAt(pageNo: number, snapshot: number): number | undefined {
    const writes = this.commits.filter(
      (commit) => commit.txId <= snapshot && commit.stamps.has(pageNo),
    );
    return writes.at(-1)?.stamps.get(pageNo);
  }
}

/** A read transaction kept open across steps, and its pages as first read. */
interface HeldRead {
  readonly transaction: ReadTransaction;
  readonly pages: readonly Uint8Array[];
}

interface Tab {
  readonly name: string;
  readonly pager: Pager;
  readonly held: HeldRead[];
}

function stampOf(page: Uint8Array): number {
  return new DataView(page.buffer, page.byteOffset).getUint32(0);
}

function setStamp(page: Uint8Array, stamp: number): void {
  new DataView(page.buffer, page.byteOffset).setUint32(0, stamp);
}

/** A copy of every page the snapshot uses; an empty array for a free one. */
function pagesOf(transaction: ReadTransaction): Uint8Array[] {
  const map = new AllocationMapReader(transaction);
  return Array.from({ length: transaction.pageCount }, (_, pageNo) =>
    map.isInUse(pageNo) ? transaction.read(pageNo).slice() : new Uint8Array(),
  );
}

function isSameBytes(a: Uint8Array, b: Uint8Array | undefined): boolean {
  return a.length === b?.length && a.every((byte, index) => byte === b[index]);
}

class Scenario {
  readonly tabs: Tab[] = [];
  readonly log: string[] = [];
  trackedPages: number[] = [];
  private readonly model = new Model();
  private writing: Tab | null = null;
  private nesting = 0;
  /** Tabs with a step running: a tab never runs two at once. */
  private readonly busy = new Set<Tab>();
  /**
   * The first failure of a step run inside another tab's file operation.
   * Tabs share no thread, so it must not surface in that tab's code — which
   * would take it for its own I/O error — but end the scenario instead.
   */
  private failure: Error | null = null;
  private stamps = 0;
  private isRunning = false;

  /** The highest intent seen so far: it may never fall (rule 1). */
  private intent = 0;

  constructor(
    private readonly random: Random,
    private readonly shouldTearWrites: boolean,
    /** The intent in the shared read-mark file, read without interleaving. */
    private readonly readIntent: () => number,
  ) {}

  /** `file` as tab `name` sees it: each operation may let another tab run first. */
  view(file: SyncFile, name: string): SyncFile {
    const turn = (): void => {
      this.interleave(name);
    };
    return {
      read: (buffer, options) => {
        turn();
        return file.read(buffer, options);
      },
      write: (buffer, options) => {
        turn();
        if (!this.shouldTearWrites || buffer.length < 2) {
          return file.write(buffer, options);
        }
        const half = Math.floor(buffer.length / 2);
        file.write(buffer.subarray(0, half), options);
        turn();
        file.write(buffer.subarray(half), { at: options.at + half });
        return buffer.length;
      },
      getSize: () => {
        turn();
        return file.getSize();
      },
      truncate: (size) => {
        turn();
        file.truncate(size);
      },
      flush: () => {
        file.flush();
      },
      close: () => {
        file.close();
      },
    };
  }

  /** Creates the tracked pages, then lets the steps interleave. */
  start(): void {
    const first = this.tabs[0];
    if (first === undefined) throw new Error("no tabs");
    this.writing = first;
    const transaction = first.pager.beginWrite();
    const stamps = new Map<number, number>();
    for (let page = 0; page < TRACKED_PAGES; page++) {
      const pageNo = transaction.allocate();
      const stamp = ++this.stamps;
      setStamp(transaction.write(pageNo), stamp);
      stamps.set(pageNo, stamp);
    }
    this.model.record(first.pager.latestCommit() + 1, stamps);
    transaction.commit();
    this.trackedPages = [...stamps.keys()];
    this.writing = null;
    this.isRunning = true;
  }

  run(steps: number): void {
    for (let step = 0; step < steps; step++) {
      this.stepOf(this.random.pick(this.tabs));
      if (this.failure !== null) throw this.failure;
    }
  }

  /** Ends every reader and checks the last state from a tab opened afresh. */
  finish(): void {
    this.isRunning = false;
    for (const tab of this.tabs) {
      for (const held of tab.held.splice(0)) this.checkAndEnd(tab, held);
    }
  }

  checkLatest(pager: Pager): void {
    const transaction = pager.beginRead();
    if (transaction === null) throw new Error("no snapshot after the run");
    this.check("fresh tab", transaction);
    transaction.end();
  }

  private interleave(name: string): void {
    const canInterleave =
      this.isRunning &&
      this.nesting < MAX_NESTING &&
      this.random.chance(INTERLEAVE_CHANCE);
    if (!canInterleave) return;
    const idle = this.tabs.filter(
      (tab) => !this.busy.has(tab) && tab.name !== name,
    );
    if (idle.length === 0) return;
    this.nesting++;
    try {
      this.stepOf(this.random.pick(idle));
    } catch (error) {
      this.failure ??=
        error instanceof Error ? error : new Error(String(error));
      this.isRunning = false;
    } finally {
      this.nesting--;
    }
  }

  private stepOf(tab: Tab): void {
    if (this.busy.has(tab)) return;
    this.busy.add(tab);
    try {
      this.takeStep(tab);
    } finally {
      this.busy.delete(tab);
    }
    this.checkIntent();
  }

  private checkIntent(): void {
    const intent = this.readIntent();
    if (intent < this.intent) {
      throw new Error(
        `the intent fell from ${String(this.intent)} to ${String(intent)}`,
      );
    }
    this.intent = intent;
  }

  private takeStep(tab: Tab): void {
    switch (this.random.below(5)) {
      case 0: {
        this.read(tab, "read");
        return;
      }
      case 1: {
        this.read(tab, "hold");
        return;
      }
      case 2: {
        const index = this.random.below(tab.held.length);
        const held = tab.held.splice(index, 1)[0];
        if (held !== undefined) this.checkAndEnd(tab, held);
        return;
      }
      case 3: {
        this.write(tab);
        return;
      }
      default: {
        this.underLock(tab, "checkpoint", () => {
          tab.pager.checkpoint();
        });
      }
    }
  }

  private read(tab: Tab, kind: "read" | "hold"): void {
    const transaction = tab.pager.beginRead();
    // No database here leaves a stale intent, so no snapshot is refused.
    if (transaction === null) throw new Error(`${tab.name} got no snapshot`);
    this.log.push(`${tab.name} ${kind} at ${String(transaction.snapshot)}`);
    this.check(tab.name, transaction);
    if (kind === "read") {
      transaction.end();
      return;
    }
    tab.held.push({ transaction, pages: pagesOf(transaction) });
  }

  private checkAndEnd(tab: Tab, held: HeldRead): void {
    const { transaction } = held;
    this.log.push(`${tab.name} ends ${String(transaction.snapshot)}`);
    this.check(tab.name, transaction);
    const pages = pagesOf(transaction);
    const changed = pages.findIndex(
      (page, pageNo) => !isSameBytes(page, held.pages[pageNo]),
    );
    if (changed !== -1 || pages.length !== held.pages.length) {
      throw new Error(
        `${tab.name} at ${String(transaction.snapshot)} found page ${String(changed)} changed under its snapshot`,
      );
    }
    transaction.end();
  }

  private write(tab: Tab): void {
    this.underLock(tab, "write", () => {
      const transaction = tab.pager.beginWrite();
      const stamps = new Map<number, number>();
      const count = 1 + this.random.below(TRACKED_PAGES);
      for (let write = 0; write < count; write++) {
        const pageNo = this.random.pick(this.trackedPages);
        const stamp = ++this.stamps;
        setStamp(transaction.write(pageNo), stamp);
        stamps.set(pageNo, stamp);
      }
      if (this.random.chance(0.3)) transaction.free(transaction.allocate());
      // Another tab may read the commit while it is still being written, so
      // the model has it first. Under the lock, its id is the next one.
      const txId = tab.pager.latestCommit() + 1;
      this.model.record(txId, stamps);
      transaction.commit();
      expect(tab.pager.latestCommit()).toBe(txId);
      this.log.push(`${tab.name} commits ${String(txId)}`);
      if (tab.pager.isCheckpointDue()) tab.pager.checkpoint();
    });
  }

  /** `action` under the write lock, which one tab holds at a time. */
  private underLock(tab: Tab, kind: string, action: () => void): void {
    if (this.writing !== null) return;
    this.writing = tab;
    this.log.push(`${tab.name} ${kind}`);
    try {
      action();
    } finally {
      this.writing = null;
    }
  }

  /** Each tracked page holds the stamp the model gives its snapshot, or is not in it yet. */
  private check(name: string, transaction: ReadTransaction): void {
    for (const pageNo of this.trackedPages) {
      const expected = this.model.stampAt(pageNo, transaction.snapshot);
      if (expected === undefined) {
        expect(pageNo).toBeGreaterThanOrEqual(transaction.pageCount);
        continue;
      }
      const actual = stampOf(transaction.read(pageNo));
      if (actual !== expected) {
        throw new Error(
          `${name} at ${String(transaction.snapshot)} read page ${String(pageNo)} stamped ${String(actual)}, expected ${String(expected)}`,
        );
      }
    }
  }
}

let databasesOpened = 0;

async function runScenario(
  seed: number,
  shouldTearWrites: boolean,
): Promise<void> {
  const shared = {
    database: new MemoryFile(),
    wal: new MemoryFile(),
    marks: new MemoryFile(),
  };
  const lockName = `interleaving-${String(databasesOpened++)}`;
  const options = {
    name: "interleaving",
    pageSize: PAGE_SIZE,
    lockName,
    checkpointThreshold: CHECKPOINT_THRESHOLD,
  };
  const intentBytes = new Uint8Array(8);
  const readIntent = (): number => {
    shared.marks.read(intentBytes, { at: INTENT_OFFSET });
    return new DataView(intentBytes.buffer).getFloat64(0);
  };
  const scenario = new Scenario(new Random(seed), shouldTearWrites, readIntent);
  for (let index = 0; index < TABS; index++) {
    const name = `tab${String(index)}`;
    const files: StorageFiles = {
      database: scenario.view(shared.database, name),
      wal: scenario.view(shared.wal, name),
      marks: scenario.view(shared.marks, name),
    };
    const pager = await openPager(files, options);
    scenario.tabs.push({ name, pager, held: [] });
  }
  try {
    scenario.start();
    scenario.run(STEPS);
    scenario.finish();
    scenario.checkLatest(await openPager({ ...shared }, options));
  } catch (error) {
    const steps = scenario.log.slice(-12).join("\n  ");
    throw new Error(
      `seed ${String(seed)}: ${String(error)}\n  last steps:\n  ${steps}`,
      { cause: error },
    );
  }
}

/**
 * Seeds per run: a few in every test run; set INTERLEAVING_SEEDS for a long
 * one before changing the protocol (docs/concurrency.md, How it is checked).
 */
const SEEDS = Number(process.env["INTERLEAVING_SEEDS"] ?? 8);
/**
 * About 0.3 s a seed with torn writes on an idle machine. The timeout only
 * catches a run that hangs, so it allows a busy one many times that.
 */
const TIMEOUT = Math.max(60_000, SEEDS * 3000);

/**
 * Every seed, letting the test runner's own messages through in between: a
 * long run holds the thread for minutes otherwise.
 */
async function runSeeds(shouldTearWrites: boolean): Promise<void> {
  for (let seed = 1; seed <= SEEDS; seed++) {
    await runScenario(seed, shouldTearWrites);
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

describe("tabs interleaved at every file operation", () => {
  it(
    "never let a reader see a page other than its snapshot's",
    async () => {
      await runSeeds(false);
    },
    TIMEOUT,
  );

  it(
    "hold with writes torn in two, another tab running in between",
    async () => {
      await runSeeds(true);
    },
    TIMEOUT,
  );
});

import { DatabaseInUseError } from "../../errors.js";
import { hasPrefix, PREFIX_SIZE, writePrefix } from "../file/file-prefix.js";
import type { SyncFile } from "../file/sync-file.js";

// ---------------------------------------------------------------------------
// Read marks — the oldest snapshot open in every tab, shared through a small
// file next to the WAL (the "-shm" file), so a checkpoint in one tab never
// brings the main file past a snapshot another tab still reads.
//
// The layout is in docs/file-format.md (Read-mark file).
//
// Each tab owns one slot for its lifetime by holding the Web Lock
// "<lockName>:slot:<i>". The lock is the slot's liveness: the browser releases
// it when the tab dies, so a slot whose lock can be taken has no owner and its
// mark is stale. A mark is only ever cleared under its slot's lock, so a live
// tab's mark is never lost.
// ---------------------------------------------------------------------------

/** Each value — the intent and every mark — is an aligned f64. */
const VALUE_SIZE = 8;
/** The prefix, padded to the alignment of the values after it. */
const INTENT_OFFSET = Math.ceil(PREFIX_SIZE / VALUE_SIZE) * VALUE_SIZE;
const SLOTS_OFFSET = INTENT_OFFSET + VALUE_SIZE;
const SLOT_COUNT = 64;
const FILE_SIZE = SLOTS_OFFSET + SLOT_COUNT * VALUE_SIZE;
const NO_MARK = -1;

function slotOffset(slot: number): number {
  return SLOTS_OFFSET + slot * VALUE_SIZE;
}

export class ReadMarks {
  /** One value, as it is read from and written to the file. */
  private readonly valueBytes = new Uint8Array(VALUE_SIZE);
  private readonly view = new DataView(this.valueBytes.buffer);

  private constructor(
    private readonly file: SyncFile,
    private readonly lockName: string,
    private readonly slot: number,
    private readonly releaseSlot: () => void,
  ) {}

  /**
   * Writes an empty read-mark file unless `file` already is one. Only while
   * holding the open lock: another tab's marks would be wiped.
   */
  static createIfMissing(file: SyncFile): void {
    if (!isReadMarkFile(file)) writeEmpty(file);
  }

  /**
   * Claims a free slot of the file, which createIfMissing has made. The
   * marks then own `file` and close it; if claiming fails, it stays the
   * caller's.
   */
  static async open(
    file: SyncFile,
    lockName: string,
    database: string,
  ): Promise<ReadMarks> {
    const { slot, release } = await claimSlot(lockName, database);
    const marks = new ReadMarks(file, lockName, slot, release);
    try {
      // Clears a mark that a dead previous owner of the slot left.
      marks.publish(null);
      await marks.reclaimDeadSlots();
      return marks;
    } catch (error) {
      release();
      throw error;
    }
  }

  /** Publishes the oldest snapshot open in this tab (null: none). */
  publish(oldestSnapshot: number | null): void {
    this.write(slotOffset(this.slot), oldestSnapshot ?? NO_MARK);
  }

  /** Oldest snapshot open in any tab, or null if none is. */
  oldestMark(): number | null {
    const marks = this.marks().filter((mark) => mark !== NO_MARK);
    return marks.length === 0 ? null : Math.min(...marks);
  }

  checkpointIntent(): number {
    return this.read(INTENT_OFFSET);
  }

  setCheckpointIntent(txId: number): void {
    this.write(INTENT_OFFSET, txId);
  }

  /** Clears marks left by tabs that died with a snapshot open. */
  async reclaimDeadSlots(): Promise<void> {
    for (const [slot, mark] of this.marks().entries()) {
      const isOtherTabsMark = slot !== this.slot && mark !== NO_MARK;
      if (isOtherTabsMark) await this.clearIfDead(slot);
    }
  }

  close(): void {
    this.publish(null);
    this.releaseSlot();
    this.file.close();
  }

  /** The mark in every slot, NO_MARK included. */
  private marks(): number[] {
    return Array.from({ length: SLOT_COUNT }, (_, slot) =>
      this.read(slotOffset(slot)),
    );
  }

  /** A slot whose lock can be taken has no owner, so its mark is stale. */
  private async clearIfDead(slot: number): Promise<void> {
    const lock = slotLock(this.lockName, slot);
    await navigator.locks.request(lock, { ifAvailable: true }, (held) => {
      if (held !== null) this.write(slotOffset(slot), NO_MARK);
    });
  }

  private read(offset: number): number {
    this.file.read(this.valueBytes, { at: offset });
    return this.view.getFloat64(0);
  }

  private write(offset: number, value: number): void {
    this.view.setFloat64(0, value);
    this.file.write(this.valueBytes, { at: offset });
  }
}

function slotLock(lockName: string, slot: number): string {
  return `${lockName}:slot:${String(slot)}`;
}

function isReadMarkFile(file: SyncFile): boolean {
  if (file.getSize() < FILE_SIZE) return false;
  const prefix = new Uint8Array(PREFIX_SIZE);
  file.read(prefix, { at: 0 });
  return hasPrefix(prefix, "shm");
}

function writeEmpty(file: SyncFile): void {
  const bytes = new Uint8Array(FILE_SIZE);
  const view = new DataView(bytes.buffer);
  writePrefix(bytes, "shm");
  view.setFloat64(INTENT_OFFSET, 0);
  for (let slot = 0; slot < SLOT_COUNT; slot++) {
    view.setFloat64(slotOffset(slot), NO_MARK);
  }
  file.truncate(0);
  file.write(bytes, { at: 0 });
  file.flush();
}

/** The first free slot, held until `release` is called. */
async function claimSlot(
  lockName: string,
  database: string,
): Promise<{ slot: number; release: () => void }> {
  for (let slot = 0; slot < SLOT_COUNT; slot++) {
    const release = await holdIfAvailable(slotLock(lockName, slot));
    if (release !== null) return { slot, release };
  }
  throw new DatabaseInUseError(database, "too-many-tabs");
}

/**
 * Takes the lock if it is free and holds it until the returned function is
 * called. Fails if the browser refuses the request.
 */
async function holdIfAvailable(name: string): Promise<(() => void) | null> {
  const { promise, resolve, reject } = Promise.withResolvers<
    (() => void) | null
  >();
  void holdUntilReleased(name, resolve, reject);
  return promise;
}

/** Gives `resolve` the lock's release, or null; gives `reject` a refusal. */
async function holdUntilReleased(
  name: string,
  resolve: (release: (() => void) | null) => void,
  reject: (error: unknown) => void,
): Promise<void> {
  try {
    await navigator.locks.request(name, { ifAvailable: true }, async (lock) => {
      if (lock === null) {
        resolve(null);
        return;
      }
      // The browser holds the lock until this promise settles.
      await new Promise<void>((release) => {
        resolve(release);
      });
    });
  } catch (error) {
    reject(error);
  }
}

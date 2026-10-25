import { StorageError } from "../errors.js";
import type { ISyncFileHandle } from "./file-handle.js";

// ---------------------------------------------------------------------------
// Read marks — the oldest snapshot open in every tab, shared through a small
// file next to the WAL (the "-shm" file), so a checkpoint in one tab never
// brings the main file past a snapshot another tab still reads.
//
// Layout (big-endian):
//   [0..7]    magic "OPFSSHM1"
//   [8..15]   checkpoint intent  f64  (a checkpoint may bring the main file
//                                      up to this txId)
//   [16..]    SLOT_COUNT slots   f64  (oldest snapshot open in the slot's
//                                      tab; -1 = none)
//
// Each tab owns one slot for its lifetime by holding the Web Lock
// "<lockName>:slot:<i>". The lock is the slot's liveness: the browser releases
// it when the tab dies, so a slot whose lock can be taken has no owner and its
// mark is stale. A mark is only ever cleared under its slot's lock, so a live
// tab's mark is never lost.
// ---------------------------------------------------------------------------

const MAGIC = new Uint8Array([0x4f, 0x50, 0x46, 0x53, 0x53, 0x48, 0x4d, 0x31]); // "OPFSSHM1"
const INTENT_OFFSET = 8;
const SLOTS_OFFSET = 16;
const SLOT_COUNT = 64;
const FILE_SIZE = SLOTS_OFFSET + SLOT_COUNT * 8;
const NO_MARK = -1;

const slotOffset = (slot: number) => SLOTS_OFFSET + slot * 8;

export class ReadMarks {
  private readonly cell = new Uint8Array(8);
  private readonly view = new DataView(this.cell.buffer);

  private constructor(
    private readonly handle: ISyncFileHandle,
    private readonly lockName: string,
    private readonly slot: number,
    private readonly releaseSlot: () => void,
  ) {}

  /** Opens the shared file (initialising it if needed) and claims a slot. */
  static async open(
    handle: ISyncFileHandle,
    lockName: string,
  ): Promise<ReadMarks> {
    await navigator.locks.request(`${lockName}:shm-init`, () => {
      if (!isInitialised(handle)) initialise(handle);
    });
    for (let slot = 0; slot < SLOT_COUNT; slot++) {
      const release = await holdIfAvailable(slotLock(lockName, slot));
      if (!release) continue;
      const marks = new ReadMarks(handle, lockName, slot, release);
      marks.publish(null); // whatever a dead previous owner left
      await marks.reclaimDeadSlots();
      return marks;
    }
    throw new StorageError(
      `all ${SLOT_COUNT} read-mark slots are taken: too many tabs have the database open`,
    );
  }

  /** Publishes the oldest snapshot open in this tab (null: none). */
  publish(oldestSnapshot: number | null): void {
    this.write(slotOffset(this.slot), oldestSnapshot ?? NO_MARK);
  }

  /** Oldest snapshot open in any tab, or null if none is. */
  oldestMark(): number | null {
    let oldest: number | null = null;
    for (let slot = 0; slot < SLOT_COUNT; slot++) {
      const mark = this.read(slotOffset(slot));
      if (mark !== NO_MARK && (oldest === null || mark < oldest)) oldest = mark;
    }
    return oldest;
  }

  checkpointIntent(): number {
    return this.read(INTENT_OFFSET);
  }

  setCheckpointIntent(txId: number): void {
    this.write(INTENT_OFFSET, txId);
  }

  /** Clears marks left by tabs that died with a snapshot open. */
  async reclaimDeadSlots(): Promise<void> {
    for (let slot = 0; slot < SLOT_COUNT; slot++) {
      if (slot === this.slot || this.read(slotOffset(slot)) === NO_MARK) {
        continue;
      }
      await navigator.locks.request(
        slotLock(this.lockName, slot),
        { ifAvailable: true },
        (lock) => {
          if (lock) this.write(slotOffset(slot), NO_MARK);
        },
      );
    }
  }

  close(): void {
    this.publish(null);
    this.releaseSlot();
    this.handle.close();
  }

  private read(offset: number): number {
    this.handle.read(this.cell, { at: offset });
    return this.view.getFloat64(0, false);
  }

  private write(offset: number, value: number): void {
    this.view.setFloat64(0, value, false);
    this.handle.write(this.cell, { at: offset });
  }
}

function slotLock(lockName: string, slot: number): string {
  return `${lockName}:slot:${slot}`;
}

function isInitialised(handle: ISyncFileHandle): boolean {
  if (handle.getSize() < FILE_SIZE) return false;
  const magic = new Uint8Array(MAGIC.length);
  handle.read(magic, { at: 0 });
  return magic.every((byte, i) => byte === MAGIC[i]);
}

function initialise(handle: ISyncFileHandle): void {
  const file = new Uint8Array(FILE_SIZE);
  const view = new DataView(file.buffer);
  file.set(MAGIC, 0);
  view.setFloat64(INTENT_OFFSET, 0, false);
  for (let slot = 0; slot < SLOT_COUNT; slot++) {
    view.setFloat64(slotOffset(slot), NO_MARK, false);
  }
  handle.truncate(0);
  handle.write(file, { at: 0 });
  handle.flush();
}

/** Takes the lock if it is free and holds it until the returned function is called. */
function holdIfAvailable(name: string): Promise<(() => void) | null> {
  return new Promise((claimed) => {
    void navigator.locks.request(name, { ifAvailable: true }, (lock) => {
      if (!lock) {
        claimed(null);
        return;
      }
      return new Promise<void>((release) => claimed(release));
    });
  });
}

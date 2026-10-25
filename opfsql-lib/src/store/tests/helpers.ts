import { encodePage } from "../page-codec.js";
import type { SyncIPageStorage } from "../types.js";
import type { ISyncFileHandle } from "../wal/file-handle.js";

/** Commit `pages` (pageNo → value) to a backend as one batch. */
export function commitBatch(
  backend: SyncIPageStorage,
  pages: Record<number, unknown>,
  nextPageId: number = backend.getNextPageId(),
): void {
  const batch = new Map(
    Object.entries(pages).map(([pageNo, value]) => [
      Number(pageNo),
      encodePage(value),
    ]),
  );
  backend.commit(batch, nextPageId);
}

/**
 * Writes a read mark straight into a slot (layout in read-marks.ts), as a tab
 * that later died would have.
 */
export function writeOrphanMark(
  shm: ISyncFileHandle,
  slot: number,
  mark: number,
): void {
  const cell = new Uint8Array(8);
  new DataView(cell.buffer).setFloat64(0, mark, false);
  shm.write(cell, { at: 24 + slot * 8 });
}

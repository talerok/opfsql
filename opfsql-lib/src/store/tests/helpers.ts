import { encodePage } from "../page-codec.js";
import type { SyncIPageStorage } from "../types.js";

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

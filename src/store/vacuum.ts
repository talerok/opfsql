import type { IStorage, IPageManager, IVacuum, PageMeta, PageRow } from './types.js';
import { PAGE_SIZE } from './types.js';

export class Vacuum implements IVacuum {
  constructor(
    private readonly storage: IStorage,
    private readonly pm: IPageManager,
  ) {}

  async shouldVacuum(tableId: string): Promise<boolean> {
    const meta = await this.pm.getPageMeta(tableId);
    if (meta.lastPageId < 0 || meta.totalRowCount === 0) return false;
    return meta.deadRowCount / meta.totalRowCount > 0.3;
  }

  async vacuumTable(tableId: string): Promise<void> {
    const meta = await this.pm.getPageMeta(tableId);
    const oldKeys = await this.pm.getAllPageKeys(tableId);

    const liveRows: PageRow[] = [];
    for (let pid = 0; pid <= meta.lastPageId; pid++) {
      const page = await this.pm.readPage(tableId, pid);
      if (!page) continue;
      for (const pr of page.rows) {
        if (!pr.deleted) liveRows.push(pr);
      }
    }

    const entries: Array<[string, unknown]> = [];

    // Delete all old page keys
    for (const key of oldKeys) {
      entries.push([key, null]);
    }

    // Write new compacted pages
    let pageId = 0;
    for (let i = 0; i < liveRows.length; i += PAGE_SIZE) {
      const chunk = liveRows.slice(i, i + PAGE_SIZE);
      const rows = chunk.map((pr, slotId) => ({
        slotId,
        deleted: false,
        data: pr.data,
      }));
      entries.push([
        this.pm.getPageKey(tableId, pageId),
        { pageId, tableId, rows },
      ]);
      pageId++;
    }

    const newMeta: PageMeta = {
      lastPageId: liveRows.length === 0 ? -1 : pageId - 1,
      totalRowCount: liveRows.length,
      deadRowCount: 0,
    };
    entries.push([this.pm.getMetaKey(tableId), newMeta]);

    await this.storage.putMany(entries);
  }

  vacuumIfNeeded(tableId: string): void {
    (async () => {
      try {
        if (await this.shouldVacuum(tableId)) {
          await this.vacuumTable(tableId);
        }
      } catch (err) {
        console.error('Vacuum error:', err);
      }
    })();
  }
}

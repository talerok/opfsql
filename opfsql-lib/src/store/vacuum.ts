import type { IPageManager, ICatalog, IVacuum, PageMeta, PageRow, RowId } from './types.js';
import { PAGE_SIZE } from './types.js';
import type { IIndexManager } from './index-manager.js';
import type { IndexKey } from './btree/types.js';
import { compareIndexKeys } from './btree/compare.js';

export class Vacuum implements IVacuum {
  catalog?: ICatalog;
  indexManager?: IIndexManager;

  constructor(
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

    // All writes go through WAL — atomic commit at the end.

    // Delete all old page keys
    for (const key of oldKeys) {
      this.pm.deleteKey(key);
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
      this.pm.writePage(tableId, { pageId, tableId, rows });
      pageId++;
    }

    const newMeta: PageMeta = {
      lastPageId: liveRows.length === 0 ? -1 : pageId - 1,
      totalRowCount: liveRows.length,
      deadRowCount: 0,
    };
    this.pm.writeMeta(tableId, newMeta);

    // Rebuild indexes (also writes through WAL)
    await this.rebuildIndexes(tableId, liveRows);

    // Single atomic commit: pages + indexes together
    await this.pm.commit();
  }

  async vacuumIfNeeded(tableId: string): Promise<void> {
    if (await this.shouldVacuum(tableId)) {
      await this.vacuumTable(tableId);
    }
  }

  // ---------------------------------------------------------------------------
  // Index rebuild after vacuum
  // ---------------------------------------------------------------------------

  private async rebuildIndexes(tableId: string, liveRows: PageRow[]): Promise<void> {
    if (!this.catalog || !this.indexManager) return;

    const indexes = this.catalog.getTableIndexes(tableId);
    if (indexes.length === 0) return;

    for (const idx of indexes) {
      // Drop old B-tree (writes deletions to WAL)
      await this.indexManager.dropIndex(idx.name);

      // Build new entries with correct RowIds from compacted pages
      const idxEntries: Array<{ key: IndexKey; rowId: RowId }> = [];
      for (let i = 0; i < liveRows.length; i++) {
        const row = liveRows[i].data;
        const key: IndexKey = idx.columns.map((col) => row[col] ?? null);
        const rowId: RowId = {
          pageId: Math.floor(i / PAGE_SIZE),
          slotId: i % PAGE_SIZE,
        };
        idxEntries.push({ key, rowId });
      }

      // Sort by key for bulkLoad
      idxEntries.sort((a, b) => compareIndexKeys(a.key, b.key));

      await this.indexManager.bulkLoad(idx.name, idxEntries, idx.unique);
    }
  }
}

import { StorageError, wrapStorageError } from './errors.js';
import type { IPageManager, IRowManager, Page, Row, RowId } from './types.js';
import { PAGE_SIZE } from './types.js';

export class RowManager implements IRowManager {
  constructor(private readonly pm: IPageManager) {}

  async prepareInsert(tableId: string, row: Row): Promise<RowId> {
    try {
      const meta = await this.pm.getPageMeta(tableId);

      let page: Page | null = null;
      if (meta.lastPageId >= 0) {
        page = await this.pm.readPage(tableId, meta.lastPageId);
      }
      if (!page || this.liveRowCount(page) >= PAGE_SIZE) {
        meta.lastPageId++;
        page = this.pm.createEmptyPage(tableId, meta.lastPageId);
      }

      const slotId = page.rows.length;
      page.rows.push({ slotId, deleted: false, data: row });
      meta.totalRowCount++;

      this.pm.writePage(tableId, page);
      this.pm.writeMeta(tableId, meta);

      return { pageId: page.pageId, slotId };
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  async prepareUpdate(
    tableId: string,
    rowId: RowId,
    row: Row,
  ): Promise<RowId> {
    try {
      const meta = await this.pm.getPageMeta(tableId);
      const oldPage = await this.requirePage(tableId, rowId.pageId);

      oldPage.rows[rowId.slotId].deleted = true;
      meta.deadRowCount++;

      let newPage: Page;
      if (rowId.pageId === meta.lastPageId) {
        newPage = oldPage;
      } else {
        newPage = await this.requirePage(tableId, meta.lastPageId);
      }

      if (this.liveRowCount(newPage) >= PAGE_SIZE) {
        meta.lastPageId++;
        newPage = this.pm.createEmptyPage(tableId, meta.lastPageId);
      }

      const slotId = newPage.rows.length;
      newPage.rows.push({ slotId, deleted: false, data: row });
      meta.totalRowCount++;

      this.pm.writePage(tableId, oldPage);
      if (oldPage !== newPage) this.pm.writePage(tableId, newPage);
      this.pm.writeMeta(tableId, meta);

      return { pageId: newPage.pageId, slotId };
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  async prepareDelete(tableId: string, rowId: RowId): Promise<void> {
    try {
      const meta = await this.pm.getPageMeta(tableId);
      const page = await this.requirePage(tableId, rowId.pageId);

      page.rows[rowId.slotId].deleted = true;
      meta.deadRowCount++;

      this.pm.writePage(tableId, page);
      this.pm.writeMeta(tableId, meta);
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  async *scanTable(
    tableId: string,
  ): AsyncGenerator<{ rowId: RowId; row: Row }> {
    const meta = await this.pm.getPageMeta(tableId);
    for (let pid = 0; pid <= meta.lastPageId; pid++) {
      const page = await this.pm.readPage(tableId, pid);
      if (!page) continue;
      for (const pr of page.rows) {
        if (!pr.deleted) {
          yield { rowId: { pageId: pid, slotId: pr.slotId }, row: pr.data };
        }
      }
    }
  }

  async readRow(tableId: string, rowId: RowId): Promise<Row | null> {
    try {
      const page = await this.pm.readPage(tableId, rowId.pageId);
      if (!page) return null;
      const pr = page.rows[rowId.slotId];
      if (!pr || pr.deleted) return null;
      return pr.data;
    } catch (err) {
      throw wrapStorageError(err);
    }
  }

  private liveRowCount(page: Page): number {
    return page.rows.filter((r) => !r.deleted).length;
  }

  private async requirePage(tableId: string, pageId: number): Promise<Page> {
    const page = await this.pm.readPage(tableId, pageId);
    if (!page) {
      throw new StorageError(`Page ${pageId} not found in table ${tableId}`);
    }
    return page;
  }
}

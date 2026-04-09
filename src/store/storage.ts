import { PageManager } from './page-manager.js';
import { RowManager } from './row-manager.js';
import { Vacuum } from './vacuum.js';
import { IndexManager, type IIndexManager } from './index-manager.js';
import type { ICatalog, IPageManager, IRowManager, IStorage } from './types.js';

export class Storage {
  readonly backend: IStorage;
  pageManager!: IPageManager;
  rowManager!: IRowManager;
  indexManager!: IIndexManager;
  vacuum!: Vacuum;

  constructor(backend: IStorage) {
    this.backend = backend;
  }

  async open(): Promise<void> {
    await this.backend.open();
    this.pageManager = new PageManager(this.backend);
    this.rowManager = new RowManager(this.pageManager);
    this.indexManager = new IndexManager(this.pageManager);
    this.vacuum = new Vacuum(this.pageManager);
  }

  close(): void {
    this.backend.close();
  }

  async initAndVacuum(tableIds: string[], catalog: ICatalog): Promise<void> {
    this.vacuum.catalog = catalog;
    this.vacuum.indexManager = this.indexManager;
    for (const id of tableIds) {
      await this.vacuum.vacuumIfNeeded(id);
    }
  }
}

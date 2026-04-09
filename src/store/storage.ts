import { PageManager } from './page-manager.js';
import { RowManager } from './row-manager.js';
import { Vacuum } from './vacuum.js';
import type { IPageManager, IRowManager, IStorage, IVacuum } from './types.js';

export class Storage {
  readonly backend: IStorage;
  pageManager!: IPageManager;
  rowManager!: IRowManager;
  vacuum!: IVacuum;

  constructor(backend: IStorage) {
    this.backend = backend;
  }

  async open(): Promise<void> {
    await this.backend.open();
    this.pageManager = new PageManager(this.backend);
    this.rowManager = new RowManager(this.pageManager);
    this.vacuum = new Vacuum(this.backend, this.pageManager);
  }

  close(): void {
    this.backend.close();
  }

  initAndVacuum(tableIds: string[]): void {
    for (const id of tableIds) {
      this.vacuum.vacuumIfNeeded(id);
    }
  }
}

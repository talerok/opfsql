import { PageManager } from './page-manager.js';
import { IndexManager, type IIndexManager } from './index-manager.js';
import type { IStorage } from './types.js';

export class Storage {
  readonly backend: IStorage;
  pageManager!: PageManager;
  indexManager!: IIndexManager;

  constructor(backend: IStorage) {
    this.backend = backend;
  }

  async open(): Promise<void> {
    await this.backend.open();
    this.pageManager = new PageManager(this.backend);
    this.indexManager = new IndexManager(this.pageManager);
  }

  close(): void {
    this.backend.close();
  }
}

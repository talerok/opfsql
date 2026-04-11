import { PageManager } from './page-manager.js';
import { TableManager } from './table-manager.js';
import { IndexManager, type IIndexManager } from './index-manager.js';
import type { IStorage } from './types.js';

export class Storage {
  readonly backend: IStorage;
  kv!: PageManager;
  rowManager!: TableManager;
  indexManager!: IIndexManager;

  constructor(backend: IStorage) {
    this.backend = backend;
  }

  async open(): Promise<void> {
    await this.backend.open();
    this.kv = new PageManager(this.backend);
    this.rowManager = new TableManager(this.kv);
    this.indexManager = new IndexManager(this.kv);
  }

  close(): void {
    this.backend.close();
  }
}

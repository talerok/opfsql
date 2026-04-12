import type { SyncIStorage } from './types.js';
import { SyncPageManager } from './page-manager.js';
import { SyncTableManager } from './table-manager.js';
import { SyncIndexManager } from './index-manager.js';

export class Storage {
  kv!: SyncPageManager;
  rowManager!: SyncTableManager;
  indexManager!: SyncIndexManager;

  constructor(private readonly backend: SyncIStorage) {}

  async open(): Promise<void> {
    await this.backend.open();
    this.kv = new SyncPageManager(this.backend);
    this.rowManager = new SyncTableManager(this.kv);
    this.indexManager = new SyncIndexManager(this.kv);
  }

  close(): void {
    this.backend.close();
  }
}

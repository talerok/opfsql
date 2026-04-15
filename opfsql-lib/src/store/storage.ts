import { SyncIndexManager } from "./index-manager.js";
import { SyncPageStore } from "./page-manager.js";
import { SyncTableManager } from "./table-manager.js";
import type { SyncIPageStorage } from "./types.js";

export class Storage {
  pageStore!: SyncPageStore;
  rowManager!: SyncTableManager;
  indexManager!: SyncIndexManager;

  constructor(private readonly backend: SyncIPageStorage) {}

  async open(): Promise<void> {
    await this.backend.open();
    const nextPageId = this.backend.getNextPageId();
    const freeList = this.backend.readPage<number[]>(2) ?? [];
    this.pageStore = new SyncPageStore(this.backend, nextPageId, freeList);
  }

  close(): void {
    this.backend.close();
  }
}

import { SyncPageStore } from "./page-manager.js";
import type { SyncIPageStorage } from "./types.js";

export class Storage {
  pageStore!: SyncPageStore;

  constructor(private readonly backend: SyncIPageStorage) {}

  async open(): Promise<void> {
    await this.backend.open();
    const nextPageId = this.backend.getNextPageId();
    const freeList = this.backend.readPage<number[]>(2) ?? [];
    this.pageStore = new SyncPageStore(this.backend, nextPageId, freeList);
  }

  catchUp(): boolean {
    const changed = this.backend.catchUp?.() ?? false;
    if (changed) this.pageStore.refreshFromStorage();
    return changed;
  }

  checkpoint(): void {
    this.backend.checkpoint?.();
  }

  close(): void {
    this.backend.close();
  }
}

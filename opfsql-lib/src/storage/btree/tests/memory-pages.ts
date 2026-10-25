import type { PageWriter } from "../page-store.js";

/**
 * Pages in memory with a writer's semantics: writes go to private copies,
 * which commit() publishes and rollback() drops. Page 0 is never handed out,
 * as in a database file, where it is the header.
 */
export class MemoryPages implements PageWriter {
  private readonly committed = new Map<number, Uint8Array>();
  private readonly dirty = new Map<number, Uint8Array>();
  private readonly freed = new Set<number>();
  private pageCount = 1;

  constructor(readonly pageSize: number) {}

  /** Pages in use, after the last commit. */
  get usedPages(): number[] {
    return [...this.committed.keys()].toSorted((a, b) => a - b);
  }

  read(pageNo: number): Uint8Array {
    const page = this.dirty.get(pageNo) ?? this.committed.get(pageNo);
    if (page === undefined) {
      throw new Error(`page ${String(pageNo)} is not in use`);
    }
    return page;
  }

  write(pageNo: number): Uint8Array {
    const dirty = this.dirty.get(pageNo);
    if (dirty !== undefined) return dirty;
    const copy = new Uint8Array(this.read(pageNo));
    this.dirty.set(pageNo, copy);
    return copy;
  }

  allocate(): number {
    const pageNo = this.pageCount++;
    this.dirty.set(pageNo, new Uint8Array(this.pageSize));
    return pageNo;
  }

  free(pageNo: number): void {
    this.requireInUse(pageNo);
    this.freed.add(pageNo);
  }

  commit(): void {
    for (const [pageNo, page] of this.dirty) this.committed.set(pageNo, page);
    for (const pageNo of this.freed) this.committed.delete(pageNo);
    this.dirty.clear();
    this.freed.clear();
  }

  /** A fingerprint of the committed pages, to prove writes never touch them. */
  committedFingerprint(): string {
    return [...this.committed]
      .map(([pageNo, page]) => `${String(pageNo)}:${String(checksum(page))}`)
      .join(" ");
  }

  /** Freeing a page twice, or one never allocated, is a bug in the tree. */
  private requireInUse(pageNo: number): void {
    this.read(pageNo);
  }
}

function checksum(bytes: Uint8Array): number {
  let hash = 0;
  for (const byte of bytes) hash = (hash * 31 + byte) % 2_147_483_647;
  return hash;
}

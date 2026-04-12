import type { ColumnBinding } from '../../binder/types.js';
import type { SyncPhysicalOperator, Tuple } from '../types.js';
import { serializeKey } from './utils.js';

export class PhysicalDistinct implements SyncPhysicalOperator {
  private readonly seen = new Set<string>();

  constructor(private readonly child: SyncPhysicalOperator) {}

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    while (true) {
      const batch = this.child.next();
      if (!batch) return null;

      const result: Tuple[] = [];
      for (const tuple of batch) {
        const key = serializeKey(tuple);
        if (!this.seen.has(key)) {
          this.seen.add(key);
          result.push(tuple);
        }
      }

      if (result.length > 0) return result;
    }
  }

  reset(): void {
    this.seen.clear();
    this.child.reset();
  }
}

export class PhysicalUnion implements SyncPhysicalOperator {
  private leftDone = false;
  private readonly seen: Set<string> | null;

  constructor(
    private readonly left: SyncPhysicalOperator,
    private readonly right: SyncPhysicalOperator,
    private readonly all: boolean,
  ) {
    this.seen = all ? null : new Set();
  }

  getLayout(): ColumnBinding[] {
    return this.left.getLayout();
  }

  next(): Tuple[] | null {
    while (!this.leftDone) {
      const batch = this.left.next();
      if (!batch) { this.leftDone = true; break; }
      const result = this.dedup(batch);
      if (result) return result;
    }

    while (true) {
      const batch = this.right.next();
      if (!batch) return null;
      const result = this.dedup(batch);
      if (result) return result;
    }
  }

  reset(): void {
    this.leftDone = false;
    this.seen?.clear();
    this.left.reset();
    this.right.reset();
  }

  private dedup(batch: Tuple[]): Tuple[] | null {
    if (!this.seen) return batch;

    const result: Tuple[] = [];
    for (const tuple of batch) {
      const key = serializeKey(tuple);
      if (!this.seen.has(key)) {
        this.seen.add(key);
        result.push(tuple);
      }
    }
    return result.length > 0 ? result : null;
  }
}

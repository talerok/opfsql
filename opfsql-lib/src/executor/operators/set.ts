import type { ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple } from '../types.js';
import { serializeKey } from './utils.js';

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

export class PhysicalDistinct implements PhysicalOperator {
  private readonly seen = new Set<string>();

  constructor(private readonly child: PhysicalOperator) {}

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  async next(): Promise<Tuple[] | null> {
    while (true) {
      const batch = await this.child.next();
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

  async reset(): Promise<void> {
    this.seen.clear();
    await this.child.reset();
  }
}

// ---------------------------------------------------------------------------
// Union
// ---------------------------------------------------------------------------

export class PhysicalUnion implements PhysicalOperator {
  private leftDone = false;
  private readonly seen: Set<string> | null;

  constructor(
    private readonly left: PhysicalOperator,
    private readonly right: PhysicalOperator,
    private readonly all: boolean,
  ) {
    this.seen = all ? null : new Set();
  }

  getLayout(): ColumnBinding[] {
    return this.left.getLayout();
  }

  async next(): Promise<Tuple[] | null> {
    while (!this.leftDone) {
      const batch = await this.left.next();
      if (!batch) {
        this.leftDone = true;
        break;
      }
      const result = this.dedup(batch);
      if (result) return result;
    }

    while (true) {
      const batch = await this.right.next();
      if (!batch) return null;
      const result = this.dedup(batch);
      if (result) return result;
    }
  }

  async reset(): Promise<void> {
    this.leftDone = false;
    this.seen?.clear();
    await this.left.reset();
    await this.right.reset();
  }

  private dedup(batch: Tuple[]): Tuple[] | null {
    if (!this.seen) return batch; // UNION ALL

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

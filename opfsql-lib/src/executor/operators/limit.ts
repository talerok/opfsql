import type { LogicalLimit, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple } from '../types.js';

export class PhysicalLimit implements PhysicalOperator {
  private skipped = 0;
  private emitted = 0;
  private readonly maxRows: number;

  constructor(
    private readonly child: PhysicalOperator,
    private readonly op: LogicalLimit,
  ) {
    this.maxRows = op.limitVal ?? Infinity;
  }

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  async next(): Promise<Tuple[] | null> {
    if (this.emitted >= this.maxRows) return null;

    while (true) {
      const batch = await this.child.next();
      if (!batch) return null;

      let start = 0;

      // Skip offset rows
      if (this.skipped < this.op.offsetVal) {
        const toSkip = Math.min(
          batch.length,
          this.op.offsetVal - this.skipped,
        );
        this.skipped += toSkip;
        start = toSkip;
      }

      if (start >= batch.length) continue;

      // Take up to remaining limit
      const remaining = this.maxRows - this.emitted;
      const end = Math.min(batch.length, start + remaining);
      const result = batch.slice(start, end);
      this.emitted += result.length;

      return result.length > 0 ? result : null;
    }
  }

  async reset(): Promise<void> {
    this.skipped = 0;
    this.emitted = 0;
    await this.child.reset();
  }
}

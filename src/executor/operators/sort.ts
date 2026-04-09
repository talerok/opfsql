import type { LogicalOrderBy, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { drainOperator } from './utils.js';

export class PhysicalSort implements PhysicalOperator {
  private sorted: Tuple[] | null = null;
  private offset = 0;

  constructor(
    private readonly child: PhysicalOperator,
    private readonly op: LogicalOrderBy,
    private readonly ctx: EvalContext,
  ) {}

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  async next(): Promise<Tuple[] | null> {
    if (!this.sorted) {
      await this.sortAll();
    }

    if (this.offset >= this.sorted!.length) return null;

    const batch = this.sorted!.slice(this.offset, this.offset + 500);
    this.offset += batch.length;
    return batch;
  }

  async reset(): Promise<void> {
    this.sorted = null;
    this.offset = 0;
    await this.child.reset();
  }

  private async sortAll(): Promise<void> {
    const tuples = await drainOperator(this.child);
    const resolver = buildResolver(this.child.getLayout());

    // Pre-compute sort keys for each tuple
    const keyed: { tuple: Tuple; keys: Value[] }[] = [];
    for (const tuple of tuples) {
      const keys: Value[] = [];
      for (const order of this.op.orders) {
        keys.push(
          await evaluateExpression(order.expression, tuple, resolver, this.ctx),
        );
      }
      keyed.push({ tuple, keys });
    }

    keyed.sort((a, b) => {
      for (let i = 0; i < this.op.orders.length; i++) {
        const order = this.op.orders[i];
        const va = a.keys[i];
        const vb = b.keys[i];

        // NULL handling
        if (va === null && vb === null) continue;
        if (va === null)
          return order.nullOrder === 'NULLS_FIRST' ? -1 : 1;
        if (vb === null)
          return order.nullOrder === 'NULLS_FIRST' ? 1 : -1;

        // Compare non-null values
        let cmp: number;
        if (typeof va === 'number' && typeof vb === 'number') {
          cmp = va - vb;
        } else {
          const sa = String(va);
          const sb = String(vb);
          cmp = sa < sb ? -1 : sa > sb ? 1 : 0;
        }

        if (cmp !== 0) {
          return order.orderType === 'DESCENDING' ? -cmp : cmp;
        }
      }
      return 0;
    });

    this.sorted = keyed.map((k) => k.tuple);
  }
}

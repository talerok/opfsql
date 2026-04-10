import type { LogicalOrderBy, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { drainOperator } from './utils.js';

type KeyedTuple = { tuple: Tuple; keys: Value[] };

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
      if (this.op.topN !== undefined) {
        await this.topKSort();
      } else {
        await this.sortAll();
      }
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

  /** Compare two keyed tuples according to sort order. Negative = a before b. */
  private compare(a: KeyedTuple, b: KeyedTuple): number {
    for (let i = 0; i < this.op.orders.length; i++) {
      const order = this.op.orders[i];
      const va = a.keys[i];
      const vb = b.keys[i];

      if (va === null && vb === null) continue;
      if (va === null)
        return order.nullOrder === 'NULLS_FIRST' ? -1 : 1;
      if (vb === null)
        return order.nullOrder === 'NULLS_FIRST' ? 1 : -1;

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
  }

  private async computeKeys(
    tuple: Tuple,
    resolver: ReturnType<typeof buildResolver>,
  ): Promise<KeyedTuple> {
    const keys: Value[] = [];
    for (const order of this.op.orders) {
      keys.push(
        await evaluateExpression(order.expression, tuple, resolver, this.ctx),
      );
    }
    return { tuple, keys };
  }

  /** Full sort — used when no topN is set. */
  private async sortAll(): Promise<void> {
    const tuples = await drainOperator(this.child);
    const resolver = buildResolver(this.child.getLayout());

    const keyed: KeyedTuple[] = [];
    for (const tuple of tuples) {
      keyed.push(await this.computeKeys(tuple, resolver));
    }

    keyed.sort((a, b) => this.compare(a, b));
    this.sorted = keyed.map((k) => k.tuple);
  }

  /**
   * Top-K sort using a binary max-heap of size K.
   * The heap root is the "worst" element (last in sort order among K kept).
   * For each incoming row: if better than root, replace and sift down.
   * Result: only K rows kept in memory, O(N log K) instead of O(N log N).
   */
  private async topKSort(): Promise<void> {
    const K = this.op.topN!;
    const resolver = buildResolver(this.child.getLayout());

    // Max-heap: root is the "worst" of the kept K (compare inverted)
    const heap: KeyedTuple[] = [];

    const siftDown = (i: number) => {
      const n = heap.length;
      while (true) {
        let worst = i;
        const l = 2 * i + 1;
        const r = 2 * i + 2;
        // "worst" = appears later in sort order = compare > 0
        if (l < n && this.compare(heap[l], heap[worst]) > 0) worst = l;
        if (r < n && this.compare(heap[r], heap[worst]) > 0) worst = r;
        if (worst === i) break;
        [heap[i], heap[worst]] = [heap[worst], heap[i]];
        i = worst;
      }
    };

    const siftUp = (i: number) => {
      while (i > 0) {
        const parent = (i - 1) >> 1;
        if (this.compare(heap[i], heap[parent]) <= 0) break;
        [heap[i], heap[parent]] = [heap[parent], heap[i]];
        i = parent;
      }
    };

    // Stream through child batches
    while (true) {
      const batch = await this.child.next();
      if (!batch) break;

      for (const tuple of batch) {
        const entry = await this.computeKeys(tuple, resolver);

        if (heap.length < K) {
          heap.push(entry);
          siftUp(heap.length - 1);
        } else if (this.compare(entry, heap[0]) < 0) {
          // New entry is "better" (earlier in sort order) than worst in heap
          heap[0] = entry;
          siftDown(0);
        }
      }
    }

    // Extract sorted result from heap
    const result: KeyedTuple[] = [];
    while (heap.length > 0) {
      result.push(heap[0]);
      heap[0] = heap[heap.length - 1];
      heap.pop();
      if (heap.length > 0) siftDown(0);
    }
    // Heap extraction gives reverse order (worst first), so reverse
    result.reverse();

    this.sorted = result.map((k) => k.tuple);
  }
}

import type { LogicalOrderBy, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { drainOperator, SCAN_BATCH } from './utils.js';

type KeyedTuple = { tuple: Tuple; keys: Value[] };
type Comparator = (a: KeyedTuple, b: KeyedTuple) => number;

// ---------------------------------------------------------------------------
// Max-heap helpers (standalone — no closures, easy to test in isolation)
// ---------------------------------------------------------------------------

function heapSiftDown(heap: KeyedTuple[], i: number, cmp: Comparator): void {
  const n = heap.length;
  while (true) {
    let worst = i;
    const left = 2 * i + 1;
    const right = 2 * i + 2;
    if (left < n && cmp(heap[left], heap[worst]) > 0) worst = left;
    if (right < n && cmp(heap[right], heap[worst]) > 0) worst = right;
    if (worst === i) break;
    [heap[i], heap[worst]] = [heap[worst], heap[i]];
    i = worst;
  }
}

function heapSiftUp(heap: KeyedTuple[], i: number, cmp: Comparator): void {
  while (i > 0) {
    const parent = Math.floor((i - 1) / 2);
    if (cmp(heap[i], heap[parent]) <= 0) break;
    [heap[i], heap[parent]] = [heap[parent], heap[i]];
    i = parent;
  }
}

/** Extract all elements from a max-heap in ascending sort order. */
function heapExtractSorted(heap: KeyedTuple[], cmp: Comparator): KeyedTuple[] {
  const sorted: KeyedTuple[] = [];
  while (heap.length > 0) {
    sorted.push(heap[0]);
    heap[0] = heap[heap.length - 1];
    heap.pop();
    if (heap.length > 0) heapSiftDown(heap, 0, cmp);
  }
  sorted.reverse();
  return sorted;
}

// ---------------------------------------------------------------------------
// PhysicalSort operator
// ---------------------------------------------------------------------------

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
      this.sorted = this.op.topN !== undefined
        ? await this.topKSort()
        : await this.sortAll();
    }

    if (this.offset >= this.sorted.length) return null;

    const batch = this.sorted.slice(this.offset, this.offset + SCAN_BATCH);
    this.offset += batch.length;
    return batch;
  }

  async reset(): Promise<void> {
    this.sorted = null;
    this.offset = 0;
    await this.child.reset();
  }

  private compare(a: KeyedTuple, b: KeyedTuple): number {
    for (let i = 0; i < this.op.orders.length; i++) {
      const order = this.op.orders[i];
      const va = a.keys[i];
      const vb = b.keys[i];

      // Null handling
      if (va === null && vb === null) continue;
      if (va === null) return order.nullOrder === 'NULLS_FIRST' ? -1 : 1;
      if (vb === null) return order.nullOrder === 'NULLS_FIRST' ? 1 : -1;

      // Value comparison
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

  private async sortAll(): Promise<Tuple[]> {
    const tuples = await drainOperator(this.child);
    const resolver = buildResolver(this.child.getLayout());

    const keyed: KeyedTuple[] = [];
    for (const tuple of tuples) {
      keyed.push(await this.computeKeys(tuple, resolver));
    }

    keyed.sort((a, b) => this.compare(a, b));
    return keyed.map((k) => k.tuple);
  }

  /**
   * Top-K sort using a binary max-heap of size K.
   * The heap root is the "worst" element (last in sort order among K kept).
   * For each incoming row: if better than root, replace and sift down.
   * O(N log K) instead of O(N log N).
   */
  private async topKSort(): Promise<Tuple[]> {
    const K = this.op.topN!;
    const resolver = buildResolver(this.child.getLayout());
    const cmp: Comparator = (a, b) => this.compare(a, b);
    const heap: KeyedTuple[] = [];

    while (true) {
      const batch = await this.child.next();
      if (!batch) break;

      for (const tuple of batch) {
        const entry = await this.computeKeys(tuple, resolver);

        if (heap.length < K) {
          heap.push(entry);
          heapSiftUp(heap, heap.length - 1, cmp);
        } else if (cmp(entry, heap[0]) < 0) {
          heap[0] = entry;
          heapSiftDown(heap, 0, cmp);
        }
      }
    }

    return heapExtractSorted(heap, cmp).map((k) => k.tuple);
  }
}

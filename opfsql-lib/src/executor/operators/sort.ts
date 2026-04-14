import type { LogicalOrderBy, ColumnBinding } from '../../binder/types.js';
import type { SyncPhysicalOperator, Tuple, Value } from '../types.js';
import type { SyncEvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { compareValues } from '../evaluate/utils/compare.js';
import { drainOperator, SCAN_BATCH } from './utils.js';

type KeyedTuple = { tuple: Tuple; keys: Value[] };
type Comparator = (a: KeyedTuple, b: KeyedTuple) => number;

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

export class PhysicalSort implements SyncPhysicalOperator {
  private sorted: Tuple[] | null = null;
  private offset = 0;

  constructor(
    private readonly child: SyncPhysicalOperator,
    private readonly op: LogicalOrderBy,
    private readonly ctx: SyncEvalContext,
  ) {}

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    if (!this.sorted) {
      this.sorted = this.op.topN !== undefined ? this.topKSort() : this.sortAll();
    }

    if (this.offset >= this.sorted.length) return null;
    const batch = this.sorted.slice(this.offset, this.offset + SCAN_BATCH);
    this.offset += batch.length;
    return batch;
  }

  reset(): void {
    this.sorted = null;
    this.offset = 0;
    this.child.reset();
  }

  private compare(a: KeyedTuple, b: KeyedTuple): number {
    for (let i = 0; i < this.op.orders.length; i++) {
      const order = this.op.orders[i];
      const va = a.keys[i];
      const vb = b.keys[i];

      if (va === null && vb === null) continue;
      if (va === null) return order.nullOrder === 'NULLS_FIRST' ? -1 : 1;
      if (vb === null) return order.nullOrder === 'NULLS_FIRST' ? 1 : -1;

      const cmp = compareValues(va, vb);

      if (cmp !== 0) return order.orderType === 'DESCENDING' ? -cmp : cmp;
    }
    return 0;
  }

  private computeKeys(tuple: Tuple, resolver: ReturnType<typeof buildResolver>): KeyedTuple {
    const keys: Value[] = [];
    for (const order of this.op.orders) {
      keys.push(evaluateExpression(order.expression, tuple, resolver, this.ctx));
    }
    return { tuple, keys };
  }

  private sortAll(): Tuple[] {
    const tuples = drainOperator(this.child);
    const resolver = buildResolver(this.child.getLayout());
    const keyed = tuples.map((t) => this.computeKeys(t, resolver));
    keyed.sort((a, b) => this.compare(a, b));
    return keyed.map((k) => k.tuple);
  }

  private topKSort(): Tuple[] {
    const K = this.op.topN!;
    const resolver = buildResolver(this.child.getLayout());
    const cmp: Comparator = (a, b) => this.compare(a, b);
    const heap: KeyedTuple[] = [];

    while (true) {
      const batch = this.child.next();
      if (!batch) break;

      for (const tuple of batch) {
        const entry = this.computeKeys(tuple, resolver);
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

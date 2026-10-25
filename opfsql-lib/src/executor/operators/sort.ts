import type {
  BoundOrderByNode,
  ColumnBinding,
  LogicalOrderBy,
} from "../../sql/binder/types.js";
import { compareValues, type Value } from "../../values/index.js";
import {
  type CompiledExpression,
  compileExpression,
  type ExecutionContext,
} from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { drainOperator, SCAN_BATCH } from "./shared.js";

// ---------------------------------------------------------------------------
// ORDER BY: rows sorted by the total order of values (values/order.ts),
// with NULLs placed as each key's NULLS FIRST / NULLS LAST says. With a
// LIMIT pushed down (topN), a bounded heap keeps only the first rows.
// ---------------------------------------------------------------------------

interface KeyedRow {
  readonly row: Tuple;
  readonly keys: readonly Value[];
  /** Arrival order: rows with equal keys keep it, heap or not. */
  readonly sequence: number;
}

type Comparator = (a: KeyedRow, b: KeyedRow) => number;

interface SortKey {
  readonly value: CompiledExpression;
  readonly isDescending: boolean;
  readonly areNullsFirst: boolean;
}

export class PhysicalSort implements SyncPhysicalOperator {
  private sorted: Tuple[] | null = null;

  private offset = 0;

  private arrivals = 0;

  private readonly keys: readonly SortKey[];

  private readonly compare: Comparator = (a, b) => {
    for (const [index, key] of this.keys.entries()) {
      const order = compareKeys(key, a.keys[index], b.keys[index]);
      if (order !== 0) return order;
    }
    return a.sequence - b.sequence;
  };

  constructor(
    private readonly child: SyncPhysicalOperator,
    private readonly operator: LogicalOrderBy,
    context: ExecutionContext,
  ) {
    const resolver = buildResolver(child.getLayout());
    this.keys = operator.orders.map((order) =>
      sortKey(order, compileExpression(order.expression, resolver, context)),
    );
  }

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    this.sorted ??=
      this.operator.topN === undefined
        ? this.sortAll()
        : this.sortFirst(this.operator.topN);
    if (this.offset >= this.sorted.length) return null;
    const batch = this.sorted.slice(this.offset, this.offset + SCAN_BATCH);
    this.offset += batch.length;
    return batch;
  }

  reset(): void {
    this.sorted = null;
    this.offset = 0;
    this.arrivals = 0;
    this.child.reset();
  }

  private keyed(row: Tuple): KeyedRow {
    const keys = this.keys.map((key) => key.value(row));
    return { row, keys, sequence: this.arrivals++ };
  }

  private sortAll(): Tuple[] {
    return drainOperator(this.child)
      .map((row) => this.keyed(row))
      .toSorted(this.compare)
      .map((keyed) => keyed.row);
  }

  /** The first `limit` rows in order, holding no more than that many. */
  private sortFirst(limit: number): Tuple[] {
    const heap = new BoundedHeap(limit, this.compare);
    for (
      let batch = this.child.next();
      batch !== null;
      batch = this.child.next()
    ) {
      for (const row of batch) heap.offer(this.keyed(row));
    }
    return heap.sorted().map((keyed) => keyed.row);
  }
}

function sortKey(order: BoundOrderByNode, value: CompiledExpression): SortKey {
  return {
    value,
    isDescending: order.orderType === "DESCENDING",
    areNullsFirst: order.nullOrder === "NULLS_FIRST",
  };
}

/** NULLs where the key puts them, other values by the total order. */
function compareKeys(key: SortKey, a: Value, b: Value): number {
  if (a === null || b === null) {
    if (a === b) return 0;
    return (a === null) === key.areNullsFirst ? -1 : 1;
  }
  const order = compareValues(a, b);
  return key.isDescending ? -order : order;
}

/**
 * The `capacity` smallest rows offered so far: a max-heap whose root is the
 * largest of them, replaced when a smaller row arrives.
 */
class BoundedHeap {
  private readonly items: KeyedRow[] = [];

  constructor(
    private readonly capacity: number,
    private readonly compare: Comparator,
  ) {}

  offer(row: KeyedRow): void {
    if (this.items.length < this.capacity) {
      this.items.push(row);
      this.siftUp(this.items.length - 1);
    } else if (this.capacity > 0 && this.compare(row, this.items[0]) < 0) {
      this.items[0] = row;
      this.siftDown(0);
    }
  }

  sorted(): KeyedRow[] {
    return this.items.toSorted(this.compare);
  }

  private isAfter(a: number, b: number): boolean {
    return this.compare(this.items[a], this.items[b]) > 0;
  }

  private swap(a: number, b: number): void {
    const first = this.items[a];
    this.items[a] = this.items[b];
    this.items[b] = first;
  }

  private siftUp(start: number): void {
    let index = start;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (!this.isAfter(index, parent)) return;
      this.swap(index, parent);
      index = parent;
    }
  }

  private siftDown(start: number): void {
    let index = start;
    for (;;) {
      let largest = index;
      for (const child of [2 * index + 1, 2 * index + 2]) {
        if (child < this.items.length && this.isAfter(child, largest)) {
          largest = child;
        }
      }
      if (largest === index) return;
      this.swap(index, largest);
      index = largest;
    }
  }
}

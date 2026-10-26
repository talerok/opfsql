import {
  type CompiledExpression,
  compileExpression,
  type Scope,
  type Tuple,
} from "../../expressions/index.js";
import { physical } from "../../sql/plan/index.js";
import { compareValues, type Value } from "../../values/index.js";
import { type Context, scopeOf } from "../context.js";
import { drain, type Operator, WalkOperator } from "../operator.js";
import { compileWindow, type Window } from "./limit.js";

// ---------------------------------------------------------------------------
// ORDER BY: rows by the total order of values (docs/semantics.md), NULLs
// placed as each key says, rows with equal keys in the order they came. A
// top-n keeps only the rows its window can reach, in a bounded heap.
// ---------------------------------------------------------------------------

/** A row, its sort key values, and when it arrived, which breaks ties. */
interface SortEntry {
  readonly row: Tuple;
  readonly keys: readonly Value[];
  readonly arrival: number;
}

/** The keys of one ORDER BY, compiled. */
interface Ordering {
  readonly entryOf: (row: Tuple, arrival: number) => SortEntry;
  readonly compare: (a: SortEntry, b: SortEntry) => number;
}

interface SortKey {
  readonly value: CompiledExpression;
  readonly isDescending: boolean;
  readonly areNullsFirst: boolean;
}

/** Every row of the child, in key order. */
export function sort(
  plan: physical.Sort,
  child: Operator,
  context: Context,
): Operator {
  const ordering = compileOrdering(plan, context);
  return new WalkOperator(() => sortedRows(child, ordering).values(), [child]);
}

/** The rows of the child a sort and the limit above it would return. */
export function topN(
  plan: physical.TopN,
  child: Operator,
  context: Context,
): Operator {
  const ordering = compileOrdering(plan, context);
  const window = compileWindow(plan.limit, plan.offset, context);
  return new WalkOperator(
    () => firstRows(child, ordering, window()).values(),
    [child],
  );
}

function sortedRows(child: Operator, ordering: Ordering): Tuple[] {
  const rows: SortEntry[] = [];
  for (const row of drain(child)) {
    rows.push(ordering.entryOf(row, rows.length));
  }
  // Array sorting is stable, and the arrival order breaks ties anyway.
  return rows.toSorted(ordering.compare).map(({ row }) => row);
}

/** The window's rows in key order, holding no more rows than it can reach. */
function firstRows(
  child: Operator,
  ordering: Ordering,
  { offset, limit }: Window,
): Tuple[] {
  if (limit === Infinity) return sortedRows(child, ordering).slice(offset);
  if (limit === 0) return [];
  const heap = new BoundedHeap(offset + limit, ordering.compare);
  let arrival = 0;
  for (const row of drain(child)) {
    heap.offer(ordering.entryOf(row, arrival++));
  }
  return heap
    .sorted()
    .slice(offset)
    .map(({ row }) => row);
}

function compileOrdering(
  plan: physical.Sort | physical.TopN,
  context: Context,
): Ordering {
  const scope = scopeOf(context, physical.layoutOf(plan.child));
  const keys = plan.keys.map((key) => compileSortKey(key, scope));
  return {
    entryOf: (row, arrival) => ({
      row,
      keys: keys.map((key) => key.value(row)),
      arrival,
    }),
    compare: (a, b) => compareSorted(keys, a, b),
  };
}

function compileSortKey(key: physical.OrderKey, scope: Scope): SortKey {
  return {
    value: compileExpression(key.expression, scope),
    isDescending: key.direction === "descending",
    areNullsFirst: key.nulls === "first",
  };
}

function compareSorted(
  keys: readonly SortKey[],
  a: SortEntry,
  b: SortEntry,
): number {
  for (const [position, key] of keys.entries()) {
    const order = compareKey(
      key,
      a.keys[position] ?? null,
      b.keys[position] ?? null,
    );
    if (order !== 0) return order;
  }
  return a.arrival - b.arrival;
}

/** NULLs where the key puts them; other values by the total order, maybe reversed. */
function compareKey(key: SortKey, a: Value, b: Value): number {
  if (a === null && b === null) return 0;
  const nullOrder = key.areNullsFirst ? -1 : 1;
  if (a === null) return nullOrder;
  if (b === null) return -nullOrder;
  const order = compareValues(a, b);
  return key.isDescending ? -order : order;
}

/**
 * The `capacity` first rows offered so far: a max-heap whose root is the
 * last of them, replaced when an earlier row arrives.
 */
class BoundedHeap {
  private readonly items: SortEntry[] = [];

  constructor(
    private readonly capacity: number,
    private readonly compare: (a: SortEntry, b: SortEntry) => number,
  ) {}

  offer(row: SortEntry): void {
    if (this.items.length < this.capacity) {
      this.items.push(row);
      this.siftUp(this.items.length - 1);
      return;
    }
    const last = this.items[0];
    if (last === undefined || this.compare(row, last) >= 0) return;
    this.items[0] = row;
    this.siftDown(0);
  }

  sorted(): SortEntry[] {
    return this.items.toSorted(this.compare);
  }

  private siftUp(start: number): void {
    let position = start;
    while (position > 0) {
      const parent = Math.floor((position - 1) / 2);
      if (!this.isAfter(position, parent)) return;
      this.swap(position, parent);
      position = parent;
    }
  }

  private siftDown(start: number): void {
    let position = start;
    let last = this.lastOf(position);
    while (last !== position) {
      this.swap(position, last);
      position = last;
      last = this.lastOf(position);
    }
  }

  /** Whichever of the item at `position` and its children sorts last. */
  private lastOf(position: number): number {
    let last = position;
    for (const child of [2 * position + 1, 2 * position + 2]) {
      const isLater = child < this.items.length && this.isAfter(child, last);
      if (isLater) last = child;
    }
    return last;
  }

  private isAfter(a: number, b: number): boolean {
    const first = this.items[a];
    const second = this.items[b];
    if (first === undefined || second === undefined) return false;
    return this.compare(first, second) > 0;
  }

  private swap(a: number, b: number): void {
    const first = this.items[a];
    const second = this.items[b];
    if (first === undefined || second === undefined) return;
    this.items[a] = second;
    this.items[b] = first;
  }
}

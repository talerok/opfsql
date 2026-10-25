import type { ColumnBinding } from "../../sql/binder/types.js";
import { rowKey } from "../../values/index.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { nextNonEmpty } from "./shared.js";

// DISTINCT and UNION keep the first of rows equal under rowKey — the
// equality of compareValues, with NULLs equal to each other.

export class PhysicalDistinct implements SyncPhysicalOperator {
  private readonly seen = new Set<string>();

  constructor(private readonly child: SyncPhysicalOperator) {}

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    return nextNonEmpty(this.child, (batch) => keepUnseen(batch, this.seen));
  }

  reset(): void {
    this.seen.clear();
    this.child.reset();
  }
}

export class PhysicalUnion implements SyncPhysicalOperator {
  private isLeftDone = false;

  private readonly seen: Set<string> | null;

  constructor(
    private readonly left: SyncPhysicalOperator,
    private readonly right: SyncPhysicalOperator,
    isAll: boolean,
  ) {
    this.seen = isAll ? null : new Set();
  }

  getLayout(): ColumnBinding[] {
    return this.left.getLayout();
  }

  next(): Tuple[] | null {
    const deduplicate = (batch: Tuple[]): Tuple[] => this.deduplicate(batch);
    if (!this.isLeftDone) {
      const batch = nextNonEmpty(this.left, deduplicate);
      if (batch !== null) return batch;
      this.isLeftDone = true;
    }
    return nextNonEmpty(this.right, deduplicate);
  }

  reset(): void {
    this.isLeftDone = false;
    this.seen?.clear();
    this.left.reset();
    this.right.reset();
  }

  private deduplicate(batch: Tuple[]): Tuple[] {
    return this.seen === null ? batch : keepUnseen(batch, this.seen);
  }
}

/** The rows of `batch` whose key is not in `seen`, adding their keys. */
function keepUnseen(batch: readonly Tuple[], seen: Set<string>): Tuple[] {
  return batch.filter((row) => {
    const key = rowKey(row);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

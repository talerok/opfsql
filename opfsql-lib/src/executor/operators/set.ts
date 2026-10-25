import type { ColumnBinding } from "../../sql/binder/types.js";
import { rowKey } from "../../values/index.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";

// DISTINCT and UNION keep the first of rows equal under rowKey — the
// equality of compareValues, with NULLs equal to each other.

export class PhysicalDistinct implements SyncPhysicalOperator {
  private readonly seen = new Set<string>();

  constructor(private readonly child: SyncPhysicalOperator) {}

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    for (
      let batch = this.child.next();
      batch !== null;
      batch = this.child.next()
    ) {
      const result = keepUnseen(batch, this.seen);
      if (result.length > 0) return result;
    }
    return null;
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
    while (!this.isLeftDone) {
      const batch = this.left.next();
      if (batch === null) {
        this.isLeftDone = true;
        break;
      }
      const result = this.deduplicate(batch);
      if (result.length > 0) return result;
    }
    for (
      let batch = this.right.next();
      batch !== null;
      batch = this.right.next()
    ) {
      const result = this.deduplicate(batch);
      if (result.length > 0) return result;
    }
    return null;
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

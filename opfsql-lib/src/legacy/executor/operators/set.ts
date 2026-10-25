import type { ColumnBinding } from "../../binder/types.js";
import { firstSighting } from "../../../values/index.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { nextNonEmpty } from "./shared.js";

// DISTINCT and UNION keep the first of rows equal under rowKey — the
// equality of compareValues, with NULLs equal to each other.

export class PhysicalDistinct implements SyncPhysicalOperator {
  private isFirstSighting = firstSighting();

  constructor(private readonly child: SyncPhysicalOperator) {}

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    return nextNonEmpty(this.child, (batch) =>
      batch.filter((row) => this.isFirstSighting(row)),
    );
  }

  reset(): void {
    this.isFirstSighting = firstSighting();
    this.child.reset();
  }
}

export class PhysicalUnion implements SyncPhysicalOperator {
  private isLeftDone = false;

  private isFirstSighting = firstSighting();

  constructor(
    private readonly left: SyncPhysicalOperator,
    private readonly right: SyncPhysicalOperator,
    private readonly isAll: boolean,
  ) {}

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
    this.isFirstSighting = firstSighting();
    this.left.reset();
    this.right.reset();
  }

  private deduplicate(batch: Tuple[]): Tuple[] {
    if (this.isAll) return batch;
    return batch.filter((row) => this.isFirstSighting(row));
  }
}

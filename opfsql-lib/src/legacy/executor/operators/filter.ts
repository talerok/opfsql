import type { BoundExpression, ColumnBinding } from "../../binder/types.js";
import { compilePredicate, type ExecutionContext } from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { nextNonEmpty, type RowFilter } from "./shared.js";

export class PhysicalFilter implements SyncPhysicalOperator {
  private readonly condition: RowFilter;

  constructor(
    private readonly child: SyncPhysicalOperator,
    condition: BoundExpression,
    context: ExecutionContext,
  ) {
    this.condition = compilePredicate(
      condition,
      buildResolver(child.getLayout()),
      context,
    );
  }

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    return nextNonEmpty(this.child, (batch) =>
      batch.filter((row) => this.condition(row)),
    );
  }

  reset(): void {
    this.child.reset();
  }
}

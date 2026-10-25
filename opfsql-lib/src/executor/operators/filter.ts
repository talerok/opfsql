import type { BoundExpression, ColumnBinding } from "../../sql/binder/types.js";
import { compilePredicate, type ExecutionContext } from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import type { RowFilter } from "./shared.js";

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
    for (
      let batch = this.child.next();
      batch !== null;
      batch = this.child.next()
    ) {
      const result = batch.filter((row) => this.condition(row));
      if (result.length > 0) return result;
    }
    return null;
  }

  reset(): void {
    this.child.reset();
  }
}

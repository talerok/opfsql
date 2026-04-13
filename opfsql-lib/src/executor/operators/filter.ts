import type { BoundExpression, ColumnBinding } from "../../binder/types.js";
import { isTruthy } from "../evaluate/utils/compare.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { evaluateExpression } from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";

export class PhysicalFilter implements SyncPhysicalOperator {
  private readonly resolver;

  constructor(
    private readonly child: SyncPhysicalOperator,
    private readonly condition: BoundExpression,
    private readonly ctx: SyncEvalContext,
  ) {
    this.resolver = buildResolver(child.getLayout());
  }

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  next(): Tuple[] | null {
    while (true) {
      const batch = this.child.next();
      if (!batch) return null;

      const result: Tuple[] = [];
      for (const tuple of batch) {
        const val = evaluateExpression(
          this.condition,
          tuple,
          this.resolver,
          this.ctx,
        );
        if (isTruthy(val)) result.push(tuple);
      }

      if (result.length > 0) return result;
    }
  }

  reset(): void {
    this.child.reset();
  }
}

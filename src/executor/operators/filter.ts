import type { BoundExpression, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { isTruthy } from '../evaluate/helpers.js';

export class PhysicalFilter implements PhysicalOperator {
  private readonly resolver;

  constructor(
    private readonly child: PhysicalOperator,
    private readonly condition: BoundExpression,
    private readonly ctx: EvalContext,
  ) {
    this.resolver = buildResolver(child.getLayout());
  }

  getLayout(): ColumnBinding[] {
    return this.child.getLayout();
  }

  async next(): Promise<Tuple[] | null> {
    while (true) {
      const batch = await this.child.next();
      if (!batch) return null;

      const result: Tuple[] = [];
      for (const tuple of batch) {
        const val = await evaluateExpression(
          this.condition,
          tuple,
          this.resolver,
          this.ctx,
        );
        if (isTruthy(val)) {
          result.push(tuple);
        }
      }

      if (result.length > 0) return result;
    }
  }

  async reset(): Promise<void> {
    await this.child.reset();
  }
}

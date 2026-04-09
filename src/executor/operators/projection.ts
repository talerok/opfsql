import type { LogicalProjection, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';

export class PhysicalProjection implements PhysicalOperator {
  private readonly resolver;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly child: PhysicalOperator,
    private readonly op: LogicalProjection,
    private readonly ctx: EvalContext,
  ) {
    this.resolver = buildResolver(child.getLayout());
    this.layout = op.getColumnBindings();
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    const batch = await this.child.next();
    if (!batch) return null;

    const result: Tuple[] = [];
    for (const tuple of batch) {
      const projected: Tuple = [];
      for (const expr of this.op.expressions) {
        projected.push(
          await evaluateExpression(expr, tuple, this.resolver, this.ctx),
        );
      }
      result.push(projected);
    }
    return result;
  }

  async reset(): Promise<void> {
    await this.child.reset();
  }
}

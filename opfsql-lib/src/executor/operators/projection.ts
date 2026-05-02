import type { LogicalProjection, ColumnBinding } from '../../binder/types.js';
import type { SyncPhysicalOperator, Tuple } from '../types.js';
import type { SyncEvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';

export class PhysicalProjection implements SyncPhysicalOperator {
  private readonly resolver;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly child: SyncPhysicalOperator,
    private readonly op: LogicalProjection,
    private readonly ctx: SyncEvalContext,
  ) {
    this.resolver = buildResolver(child.getLayout());
    this.layout = op.columnBindings;
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    const batch = this.child.next();
    if (!batch) return null;

    const result: Tuple[] = [];
    for (const tuple of batch) {
      const projected: Tuple = [];
      for (const expr of this.op.expressions) {
        projected.push(evaluateExpression(expr, tuple, this.resolver, this.ctx));
      }
      result.push(projected);
    }
    return result;
  }

  reset(): void {
    this.child.reset();
  }
}

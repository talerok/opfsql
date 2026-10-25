import type { ColumnBinding, LogicalProjection } from "../../binder/types.js";
import {
  type CompiledExpression,
  compileExpression,
  type ExecutionContext,
} from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";

export class PhysicalProjection implements SyncPhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly columns: readonly CompiledExpression[];

  constructor(
    private readonly child: SyncPhysicalOperator,
    operator: LogicalProjection,
    context: ExecutionContext,
  ) {
    const resolver = buildResolver(child.getLayout());
    this.layout = operator.columnBindings;
    this.columns = operator.expressions.map((expression) =>
      compileExpression(expression, resolver, context),
    );
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    const batch = this.child.next();
    if (batch === null) return null;
    return batch.map((row) => this.columns.map((column) => column(row)));
  }

  reset(): void {
    this.child.reset();
  }
}

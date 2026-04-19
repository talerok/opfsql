import type {
  BoundExpression,
  ColumnBinding,
  IndexUnionHint,
  LogicalGet,
} from "../../binder/types.js";
import type {
  IndexKeyValue,
  SearchPredicate,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { evaluateExpression } from "../evaluate/index.js";
import { isTruthy } from "../evaluate/utils/compare.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { resolveFilterValue, rowToTuple, SCAN_BATCH } from "./utils.js";

export class PhysicalIndexUnionScan implements SyncPhysicalOperator {
  private rowIds: number[] | null = null;
  private cursor = 0;
  private done = false;
  private readonly layout: ColumnBinding[];
  private readonly resolver;
  private readonly residualExpr: BoundExpression;

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: SyncIRowManager,
    private readonly indexManager: SyncIIndexManager,
    private readonly hint: IndexUnionHint,
    private readonly ctx: SyncEvalContext,
  ) {
    this.layout = op.getColumnBindings();
    this.resolver = buildResolver(this.layout);
    this.residualExpr = hint.originalFilter;
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.done) return null;

    if (this.rowIds === null) {
      this.rowIds = this.fetchUnionRowIds();
    }

    while (this.cursor < this.rowIds.length) {
      const batch: Tuple[] = [];

      while (this.cursor < this.rowIds.length && batch.length < SCAN_BATCH) {
        const rowId = this.rowIds[this.cursor++];
        const row = this.rowManager.readRow(this.op.tableName, rowId);
        if (row === null) continue;

        const tuple = rowToTuple(row, this.op.columnIds, this.op.schema);

        // Apply the original OR expression as residual filter
        const val = evaluateExpression(
          this.residualExpr, tuple, this.resolver, this.ctx,
        );
        if (isTruthy(val)) {
          batch.push(tuple);
        }
      }

      if (batch.length > 0) return batch;
    }

    this.done = true;
    return null;
  }

  reset(): void {
    this.rowIds = null;
    this.cursor = 0;
    this.done = false;
  }

  private fetchUnionRowIds(): number[] {
    const seen = new Set<number>();

    for (const branch of this.hint.branches) {
      const predicates: SearchPredicate[] = branch.predicates.map((p) => ({
        columnPosition: p.columnPosition,
        comparisonType: p.comparisonType,
        value: resolveFilterValue(p.value, this.ctx.params) as IndexKeyValue,
      }));
      const ids = this.indexManager.search(branch.indexDef.name, predicates);
      for (const id of ids) {
        seen.add(id);
      }
    }

    // Sort for deterministic output order
    return [...seen].sort((a, b) => a - b);
  }
}

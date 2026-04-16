import type {
  ColumnBinding,
  IndexSearchPredicate,
  LogicalGet,
  TableFilter,
} from "../../binder/types.js";
import type {
  IndexDef,
  IndexKeyValue,
  SearchPredicate,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import {
  passesFilters,
  resolveFilterValue,
  rowToTuple,
  SCAN_BATCH,
} from "./utils.js";

export class PhysicalIndexScan implements SyncPhysicalOperator {
  private rowIds: number[] | null = null;
  private cursor = 0;
  private done = false;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: SyncIRowManager,
    private readonly indexManager: SyncIIndexManager,
    private readonly indexDef: IndexDef,
    private readonly indexPredicates: IndexSearchPredicate[],
    private readonly residualFilters: TableFilter[],
    private readonly ctx: SyncEvalContext,
  ) {
    this.layout = op.getColumnBindings();
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.done) return null;

    if (this.rowIds === null) {
      this.rowIds = this.fetchRowIds();
    }

    while (this.cursor < this.rowIds.length) {
      const batch: Tuple[] = [];

      while (this.cursor < this.rowIds.length && batch.length < SCAN_BATCH) {
        const rowId = this.rowIds[this.cursor++];
        const row = this.rowManager.readRow(this.op.tableName, rowId);
        if (row === null) continue;

        const tuple = rowToTuple(row, this.op.columnIds, this.op.schema);
        if (
          passesFilters(
            tuple,
            this.residualFilters,
            this.op.columnIds,
            this.ctx.params,
          )
        ) {
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

  private fetchRowIds(): number[] {
    const predicates: SearchPredicate[] = this.indexPredicates.map((p) => ({
      columnPosition: p.columnPosition,
      comparisonType: p.comparisonType,
      value: resolveFilterValue(p.value, this.ctx.params) as IndexKeyValue,
    }));
    return this.indexManager.search(this.indexDef.name, predicates);
  }
}

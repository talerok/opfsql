import type { ColumnBinding, LogicalGet } from "../../binder/types.js";
import type { SyncIRowManager } from "../../store/types.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import type { SyncPhysicalOperator, Tuple, Value } from "../types.js";
import { passesFilters, rowToTuple, SCAN_BATCH } from "./utils.js";

export class PhysicalScan implements SyncPhysicalOperator {
  private iterator: Iterator<{ row: Record<string, Value> }> | null = null;
  private done = false;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: SyncIRowManager,
    private readonly ctx: SyncEvalContext,
    private readonly childOp?: SyncPhysicalOperator,
  ) {
    this.layout = op.getColumnBindings();
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.childOp) return this.nextFromChild();

    if (this.op.tableName === "__empty") {
      if (this.done) return null;
      this.done = true;
      return [[]];
    }

    return this.nextFromStorage();
  }

  reset(): void {
    this.iterator = null;
    this.done = false;
    if (this.childOp) this.childOp.reset();
  }

  private nextFromChild(): Tuple[] | null {
    while (!this.done) {
      const batch = this.childOp!.next();
      if (!batch) {
        this.done = true;
        return null;
      }

      const result: Tuple[] = [];
      for (const childTuple of batch) {
        const tuple: Tuple = this.op.columnIds.map(
          (colId) => childTuple[colId] ?? null,
        );
        if (
          passesFilters(
            tuple,
            this.op.tableFilters,
            this.op.columnIds,
            this.ctx.params,
          )
        ) {
          result.push(tuple);
        }
      }
      if (result.length > 0) return result;
    }
    return null;
  }

  private nextFromStorage(): Tuple[] | null {
    if (!this.iterator) {
      this.iterator = (
        this.rowManager.scanTable(this.op.tableName) as Iterable<{
          row: Record<string, Value>;
        }>
      )[Symbol.iterator]();
    }

    while (!this.done) {
      const batch: Tuple[] = [];
      for (let i = 0; i < SCAN_BATCH; i++) {
        const { value, done } = this.iterator.next();
        if (done) {
          this.done = true;
          break;
        }

        const tuple = rowToTuple(value.row, this.op.columnIds, this.op.schema);
        if (
          passesFilters(
            tuple,
            this.op.tableFilters,
            this.op.columnIds,
            this.ctx.params,
          )
        ) {
          batch.push(tuple);
        }
      }

      if (batch.length > 0) return batch;
    }

    return null;
  }
}

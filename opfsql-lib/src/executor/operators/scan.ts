import type { LogicalGet, ColumnBinding } from '../../binder/types.js';
import type { IRowManager } from '../../store/types.js';

const SCAN_BATCH = 1024;
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { rowToTuple, passesFilters } from './utils.js';

export class PhysicalScan implements PhysicalOperator {
  private generator: AsyncGenerator<{ row: Record<string, Value> }> | null =
    null;
  private done = false;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: IRowManager,
    private readonly _ctx: EvalContext,
    private readonly childOp?: PhysicalOperator, // for subquery GET
  ) {
    this.layout = op.getColumnBindings();
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    if (this.childOp) return this.nextFromChild();

    // Empty GET (e.g. SELECT 1+1)
    if (this.op.tableName === '__empty') {
      if (this.done) return null;
      this.done = true;
      return [[]];
    }

    return this.nextFromStorage();
  }

  async reset(): Promise<void> {
    this.generator = null;
    this.done = false;
    if (this.childOp) await this.childOp.reset();
  }

  private async nextFromChild(): Promise<Tuple[] | null> {
    while (!this.done) {
      const batch = await this.childOp!.next();
      if (!batch) {
        this.done = true;
        return null;
      }

      const result: Tuple[] = [];
      for (const childTuple of batch) {
        const tuple: Tuple = this.op.columnIds.map((colId) => childTuple[colId] ?? null);
        if (passesFilters(tuple, this.op.tableFilters, this.op.columnIds)) {
          result.push(tuple);
        }
      }
      if (result.length > 0) return result;
    }
    return null;
  }

  private async nextFromStorage(): Promise<Tuple[] | null> {
    if (!this.generator) {
      this.generator = this.rowManager.scanTable(this.op.tableName);
    }

    while (!this.done) {
      const batch: Tuple[] = [];
      for (let i = 0; i < SCAN_BATCH; i++) {
        const { value, done } = await this.generator.next();
        if (done) {
          this.done = true;
          break;
        }

        const tuple = rowToTuple(value.row, this.op.columnIds, this.op.schema);
        if (passesFilters(tuple, this.op.tableFilters, this.op.columnIds)) {
          batch.push(tuple);
        }
      }

      if (batch.length > 0) return batch;
    }

    return null;
  }
}

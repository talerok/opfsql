import type { LogicalGet, ColumnBinding } from '../../binder/types.js';
import type { IRowManager } from '../../store/types.js';
import { PAGE_SIZE } from '../../store/types.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { applyComparison } from '../evaluate/helpers.js';

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
    // Subquery GET — drain child operator
    if (this.childOp) {
      return this.nextFromChild();
    }

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

      // Remap child tuples through columnIds
      const result: Tuple[] = [];
      for (const childTuple of batch) {
        const tuple: Tuple = this.op.columnIds.map((colId) => {
          return childTuple[colId] ?? null;
        });
        if (this.passesTableFilters(tuple)) {
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
      for (let i = 0; i < PAGE_SIZE; i++) {
        const { value, done } = await this.generator.next();
        if (done) {
          this.done = true;
          break;
        }

        const row = value.row;
        const tuple = this.rowToTuple(row);
        if (this.passesTableFilters(tuple)) {
          batch.push(tuple);
        }
      }

      if (batch.length > 0) return batch;
    }

    return null;
  }

  /** Convert storage Row → Tuple using columnIds and schema */
  private rowToTuple(row: Record<string, Value>): Tuple {
    return this.op.columnIds.map(
      (colId) => row[this.op.schema.columns[colId].name] ?? null,
    );
  }

  /** Apply pushed-down table filters */
  private passesTableFilters(tuple: Tuple): boolean {
    for (const filter of this.op.tableFilters) {
      // Find the position of filter.columnIndex in columnIds
      const pos = this.op.columnIds.indexOf(filter.columnIndex);
      if (pos === -1) continue;
      const val = tuple[pos];
      const result = applyComparison(val, filter.constant.value, filter.comparisonType);
      if (result !== true) return false;
    }
    return true;
  }
}

import type {
  LogicalGet,
  ColumnBinding,
  TableFilter,
  IndexSearchPredicate,
} from '../../binder/types.js';
import type { IRowManager, IndexDef, RowId } from '../../store/types.js';
import type { IIndexManager } from '../../store/index-manager.js';
import type { SearchPredicate } from '../../store/btree/btree.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import { applyComparison } from '../evaluate/helpers.js';

const BATCH_SIZE = 500;

export class PhysicalIndexScan implements PhysicalOperator {
  private rowIds: RowId[] | null = null;
  private cursor = 0;
  private done = false;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: IRowManager,
    private readonly indexManager: IIndexManager,
    private readonly indexDef: IndexDef,
    private readonly indexPredicates: IndexSearchPredicate[],
    private readonly residualFilters: TableFilter[],
  ) {
    this.layout = op.getColumnBindings();
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    if (this.done) return null;

    // Lazily fetch RowIds from index on first call
    if (this.rowIds === null) {
      const predicates: SearchPredicate[] = this.indexPredicates.map((p) => ({
        columnPosition: p.columnPosition,
        comparisonType: p.comparisonType,
        value: p.value,
      }));
      this.rowIds = await this.indexManager.search(
        this.indexDef.name,
        predicates,
        this.indexDef.columns.length,
      );
    }

    while (this.cursor < this.rowIds.length) {
      const batch: Tuple[] = [];

      while (this.cursor < this.rowIds.length && batch.length < BATCH_SIZE) {
        const rowId = this.rowIds[this.cursor++];
        const row = await this.rowManager.readRow(this.op.tableName, rowId);
        if (row === null) continue; // row was deleted

        const tuple = this.rowToTuple(row);
        if (this.passesResidualFilters(tuple)) {
          batch.push(tuple);
        }
      }

      if (batch.length > 0) return batch;
    }

    this.done = true;
    return null;
  }

  async reset(): Promise<void> {
    this.rowIds = null;
    this.cursor = 0;
    this.done = false;
  }

  private rowToTuple(row: Record<string, Value>): Tuple {
    return this.op.columnIds.map(
      (colId) => row[this.op.schema.columns[colId].name] ?? null,
    );
  }

  private passesResidualFilters(tuple: Tuple): boolean {
    for (const filter of this.residualFilters) {
      const pos = this.op.columnIds.indexOf(filter.columnIndex);
      if (pos === -1) continue;
      const val = tuple[pos];
      const result = applyComparison(
        val,
        filter.constant.value,
        filter.comparisonType,
      );
      if (result !== true) return false;
    }
    return true;
  }
}

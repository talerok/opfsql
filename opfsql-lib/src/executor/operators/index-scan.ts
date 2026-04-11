import type {
  LogicalGet,
  ColumnBinding,
  TableFilter,
  IndexSearchPredicate,
} from '../../binder/types.js';
import type { IRowManager, IndexDef, RowId } from '../../store/types.js';
import type { IIndexManager } from '../../store/index-manager.js';
import type { SearchPredicate } from '../../store/btree/btree.js';
import type { PhysicalOperator, Tuple } from '../types.js';
import { rowToTuple, passesFilters, SCAN_BATCH } from './utils.js';

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

    if (this.rowIds === null) {
      this.rowIds = await this.fetchRowIds();
    }

    while (this.cursor < this.rowIds.length) {
      const batch: Tuple[] = [];

      while (this.cursor < this.rowIds.length && batch.length < SCAN_BATCH) {
        const rowId = this.rowIds[this.cursor++];
        const row = await this.rowManager.readRow(this.op.tableName, rowId);
        if (row === null) continue; // row was deleted

        const tuple = rowToTuple(row, this.op.columnIds, this.op.schema);
        if (passesFilters(tuple, this.residualFilters, this.op.columnIds)) {
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

  private async fetchRowIds(): Promise<RowId[]> {
    const predicates: SearchPredicate[] = this.indexPredicates.map((p) => ({
      columnPosition: p.columnPosition,
      comparisonType: p.comparisonType,
      value: p.value,
    }));
    return this.indexManager.search(
      this.indexDef.name,
      predicates,
      this.indexDef.columns.length,
    );
  }
}

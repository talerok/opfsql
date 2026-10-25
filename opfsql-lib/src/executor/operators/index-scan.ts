import type {
  ColumnBinding,
  IndexSearchPredicate,
  LogicalGet,
  TableFilter,
} from "../../sql/binder/types.js";
import type {
  IndexDef,
  IndexKeyValue,
  SearchPredicate,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import type { ExecutionContext } from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import {
  compileTableFilters,
  passesFilters,
  indexSearchValue,
  type RowFilter,
  rowToTuple,
  SCAN_BATCH,
} from "./shared.js";

export class PhysicalIndexScan implements SyncPhysicalOperator {
  private rowIds: number[] | null = null;
  private cursor = 0;
  private done = false;
  private readonly layout: ColumnBinding[];
  private readonly residuals: readonly RowFilter[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: SyncIRowManager,
    private readonly indexManager: SyncIIndexManager,
    private readonly indexDef: IndexDef,
    private readonly indexPredicates: IndexSearchPredicate[],
    residualFilters: TableFilter[],
    private readonly context: ExecutionContext,
  ) {
    this.layout = op.columnBindings;
    // Every filter on the table is checked on the rows the index finds,
    // those it covers too: the index finds rows by its own key order (and
    // one chosen for ORDER BY covers none), while each row returned must
    // satisfy the filters as compareValues defines them.
    this.residuals = compileTableFilters(
      [...new Set([...residualFilters, ...op.tableFilters])],
      buildResolver(this.layout),
      context,
    );
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
        if (passesFilters(tuple, this.residuals)) {
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
      value: indexSearchValue(p, this.indexDef, this.context) as IndexKeyValue,
    }));
    return this.indexManager.search(this.indexDef.name, predicates);
  }
}

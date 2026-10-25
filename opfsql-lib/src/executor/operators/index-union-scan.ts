import type {
  ColumnBinding,
  IndexUnionHint,
  LogicalGet,
} from "../../sql/binder/types.js";
import type {
  IndexKeyValue,
  SearchPredicate,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../store/types.js";
import { compilePredicate, type ExecutionContext } from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import {
  compileTableFilters,
  indexSearchValue,
  passesFilters,
  type RowFilter,
  rowToTuple,
  SCAN_BATCH,
} from "./shared.js";

export class PhysicalIndexUnionScan implements SyncPhysicalOperator {
  private rowIds: number[] | null = null;
  private cursor = 0;
  private done = false;
  private readonly layout: ColumnBinding[];
  private readonly residual: RowFilter;
  private readonly filters: readonly RowFilter[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: SyncIRowManager,
    private readonly indexManager: SyncIIndexManager,
    private readonly hint: IndexUnionHint,
    private readonly context: ExecutionContext,
  ) {
    this.layout = op.columnBindings;
    const resolver = buildResolver(this.layout);
    this.residual = compilePredicate(hint.originalFilter, resolver, context);
    // The table's other filters hold for the rows too.
    this.filters = compileTableFilters(op.tableFilters, resolver, context);
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
        if (this.residual(tuple) && passesFilters(tuple, this.filters)) {
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
        value: indexSearchValue(
          p,
          branch.indexDef,
          this.context,
        ) as IndexKeyValue,
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

import type { ColumnBinding, LogicalGet } from "../../sql/binder/types.js";
import type { Row, SyncIRowManager } from "../../store/types.js";
import type { ExecutionContext } from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import {
  compileTableFilters,
  nextNonEmpty,
  passesFilters,
  type RowFilter,
  rowToTuple,
  SCAN_BATCH,
} from "./shared.js";

/** The table filters of a scan, compiled for its row layout. */
function scanFilters(
  operator: LogicalGet,
  context: ExecutionContext,
): readonly RowFilter[] {
  return compileTableFilters(
    operator.tableFilters,
    buildResolver(operator.columnBindings),
    context,
  );
}

// ---------------------------------------------------------------------------
// PhysicalScan — reads rows from storage, applies table filters.
// ---------------------------------------------------------------------------

export class PhysicalScan implements SyncPhysicalOperator {
  private iterator: Iterator<{ rowId: number; row: Row }> | null = null;

  private done = false;

  private readonly filters: readonly RowFilter[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: SyncIRowManager,
    context: ExecutionContext,
  ) {
    this.filters = scanFilters(op, context);
  }

  getLayout(): ColumnBinding[] {
    return this.op.columnBindings;
  }

  next(): Tuple[] | null {
    if (this.op.tableName === "__empty") {
      if (this.done) return null;
      this.done = true;
      return [[]];
    }

    this.iterator ??= this.rowManager
      .scanTable(this.op.tableName)
      [Symbol.iterator]();

    while (!this.done) {
      const batch = this.readBatch(this.iterator);
      if (batch.length > 0) return batch;
    }
    return null;
  }

  reset(): void {
    this.iterator = null;
    this.done = false;
  }

  private readBatch(iterator: Iterator<{ rowId: number; row: Row }>): Tuple[] {
    const batch: Tuple[] = [];
    for (let count = 0; count < SCAN_BATCH; count++) {
      const next = iterator.next();
      if (next.done === true) {
        this.done = true;
        break;
      }
      const tuple = rowToTuple(
        next.value.row,
        this.op.columnIds,
        this.op.schema,
      );
      if (passesFilters(tuple, this.filters)) batch.push(tuple);
    }
    return batch;
  }
}

// ---------------------------------------------------------------------------
// PhysicalChildScan — reads tuples from a child operator, remaps columns
// via columnIds, applies table filters. Used when LogicalGet has children
// (e.g. CTE-backed scans).
// ---------------------------------------------------------------------------

export class PhysicalChildScan implements SyncPhysicalOperator {
  private done = false;
  private readonly filters: readonly RowFilter[];

  constructor(
    private readonly op: LogicalGet,
    private readonly childOp: SyncPhysicalOperator,
    context: ExecutionContext,
  ) {
    this.filters = scanFilters(op, context);
  }

  getLayout(): ColumnBinding[] {
    return this.op.columnBindings;
  }

  next(): Tuple[] | null {
    if (this.done) return null;
    const batch = nextNonEmpty(this.childOp, (childBatch) =>
      childBatch
        .map((childTuple) =>
          this.op.columnIds.map((columnId) => childTuple[columnId] ?? null),
        )
        .filter((tuple) => passesFilters(tuple, this.filters)),
    );
    this.done = batch === null;
    return batch;
  }

  reset(): void {
    this.done = false;
    this.childOp.reset();
  }
}

import type { ColumnBinding, LogicalGet } from "../../binder/types.js";
import type { Row, SyncIRowManager } from "../../store/types.js";
import type { CompiledFilter } from "../evaluate/compile.js";
import { compileFilter } from "../evaluate/compile.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { passesCompiledFilters, rowToTuple, SCAN_BATCH } from "./utils.js";

// ---------------------------------------------------------------------------
// PhysicalScan — reads rows from storage, applies table filters.
// ---------------------------------------------------------------------------

export class PhysicalScan implements SyncPhysicalOperator {
  private iterator: Iterator<{ rowId: number; row: Row }> | null = null;
  private done = false;
  private readonly layout: ColumnBinding[];
  private readonly compiledFilters: CompiledFilter[];

  constructor(
    private readonly op: LogicalGet,
    private readonly rowManager: SyncIRowManager,
    private readonly ctx: SyncEvalContext,
  ) {
    this.layout = op.getColumnBindings();
    const resolver = buildResolver(this.layout);
    this.compiledFilters = op.tableFilters.map((f) =>
      compileFilter(f, resolver, ctx),
    );
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.op.tableName === "__empty") {
      if (this.done) return null;
      this.done = true;
      return [[]];
    }

    if (!this.iterator) {
      this.iterator = this.rowManager
        .scanTable(this.op.tableName)
        [Symbol.iterator]();
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
        if (passesCompiledFilters(tuple, this.compiledFilters, this.ctx.params)) {
          batch.push(tuple);
        }
      }

      if (batch.length > 0) return batch;
    }

    return null;
  }

  reset(): void {
    this.iterator = null;
    this.done = false;
  }
}

// ---------------------------------------------------------------------------
// PhysicalChildScan — reads tuples from a child operator, remaps columns
// via columnIds, applies table filters. Used when LogicalGet has children
// (e.g. CTE-backed scans).
// ---------------------------------------------------------------------------

export class PhysicalChildScan implements SyncPhysicalOperator {
  private done = false;
  private readonly layout: ColumnBinding[];
  private readonly compiledFilters: CompiledFilter[];

  constructor(
    private readonly op: LogicalGet,
    private readonly childOp: SyncPhysicalOperator,
    private readonly ctx: SyncEvalContext,
  ) {
    this.layout = op.getColumnBindings();
    const resolver = buildResolver(this.layout);
    this.compiledFilters = op.tableFilters.map((f) =>
      compileFilter(f, resolver, ctx),
    );
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    while (!this.done) {
      const batch = this.childOp.next();
      if (!batch) {
        this.done = true;
        return null;
      }

      const result: Tuple[] = [];
      for (const childTuple of batch) {
        const tuple: Tuple = this.op.columnIds.map(
          (colId) => childTuple[colId] ?? null,
        );
        if (passesCompiledFilters(tuple, this.compiledFilters, this.ctx.params)) {
          result.push(tuple);
        }
      }
      if (result.length > 0) return result;
    }
    return null;
  }

  reset(): void {
    this.done = false;
    this.childOp.reset();
  }
}

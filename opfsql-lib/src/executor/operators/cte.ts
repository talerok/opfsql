import type { LogicalCTERef, ColumnBinding } from '../../binder/types.js';
import type { SyncPhysicalOperator, Tuple, CTECacheEntry } from '../types.js';
import { ExecutorError } from '../errors.js';
import { drainOperator, serializeKey, SCAN_BATCH } from './utils.js';

// ---------------------------------------------------------------------------
// Materialize
// ---------------------------------------------------------------------------

export class PhysicalMaterialize implements SyncPhysicalOperator {
  private materialized = false;

  constructor(
    private readonly cteDefinition: SyncPhysicalOperator,
    private readonly mainPlan: SyncPhysicalOperator,
    private readonly cteIndex: number,
    private readonly cteCache: Map<number, CTECacheEntry>,
  ) {}

  getLayout(): ColumnBinding[] {
    return this.mainPlan.getLayout();
  }

  next(): Tuple[] | null {
    if (!this.materialized) {
      const tuples = drainOperator(this.cteDefinition);
      this.cteCache.set(this.cteIndex, { tuples, layout: this.cteDefinition.getLayout() });
      this.materialized = true;
    }
    return this.mainPlan.next();
  }

  reset(): void {
    this.materialized = false;
    this.cteCache.delete(this.cteIndex);
    this.cteDefinition.reset();
    this.mainPlan.reset();
  }
}

// ---------------------------------------------------------------------------
// CTE Scan
// ---------------------------------------------------------------------------

export class PhysicalCTEScan implements SyncPhysicalOperator {
  private offset = 0;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly op: LogicalCTERef,
    private readonly cteCache: Map<number, CTECacheEntry>,
  ) {
    this.layout = op.columnBindings;
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    const entry = this.cteCache.get(this.op.cteIndex);
    if (!entry) throw new ExecutorError(`CTE '${this.op.cteName}' not materialized yet`);

    if (this.offset >= entry.tuples.length) return null;
    const batch = entry.tuples.slice(this.offset, this.offset + SCAN_BATCH);
    this.offset += batch.length;
    return batch;
  }

  reset(): void {
    this.offset = 0;
  }
}

// ---------------------------------------------------------------------------
// Recursive CTE
// ---------------------------------------------------------------------------

const MAX_RECURSIVE_ITERATIONS = 1000;

export class PhysicalRecursiveCTE implements SyncPhysicalOperator {
  private result: Tuple[] | null = null;
  private offset = 0;

  constructor(
    private readonly anchor: SyncPhysicalOperator,
    private readonly recursive: SyncPhysicalOperator,
    private readonly cteIndex: number,
    private readonly cteCache: Map<number, CTECacheEntry>,
    private readonly isUnionAll: boolean,
  ) {}

  getLayout(): ColumnBinding[] {
    return this.anchor.getLayout();
  }

  next(): Tuple[] | null {
    if (!this.result) this.result = this.execute();
    if (this.offset >= this.result.length) return null;
    const batch = this.result.slice(this.offset, this.offset + SCAN_BATCH);
    this.offset += batch.length;
    return batch;
  }

  reset(): void {
    this.result = null;
    this.offset = 0;
    this.cteCache.delete(this.cteIndex);
    this.anchor.reset();
    this.recursive.reset();
  }

  private execute(): Tuple[] {
    const allRows: Tuple[] = [];
    const seen = this.isUnionAll ? null : new Set<string>();
    const layout = this.anchor.getLayout();

    const anchorRows = drainOperator(this.anchor);
    let workingTable: Tuple[] = [];

    for (const row of anchorRows) {
      if (seen) {
        const key = serializeKey(row);
        if (seen.has(key)) continue;
        seen.add(key);
      }
      allRows.push(row);
      workingTable.push(row);
    }

    for (let i = 0; i < MAX_RECURSIVE_ITERATIONS; i++) {
      if (workingTable.length === 0) break;

      this.cteCache.set(this.cteIndex, { tuples: workingTable, layout });
      this.recursive.reset();
      const newRows = drainOperator(this.recursive);

      if (newRows.length === 0) { workingTable = []; break; }

      workingTable = [];
      for (const row of newRows) {
        if (seen) {
          const key = serializeKey(row);
          if (seen.has(key)) continue;
          seen.add(key);
        }
        allRows.push(row);
        workingTable.push(row);
      }
    }

    if (workingTable.length > 0) {
      throw new ExecutorError(
        `Recursive CTE exceeded maximum iteration limit (${MAX_RECURSIVE_ITERATIONS})`,
      );
    }

    this.cteCache.set(this.cteIndex, { tuples: allRows, layout });
    return allRows;
  }
}

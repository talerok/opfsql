import type { LogicalCTERef, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple, CTECacheEntry } from '../types.js';
import { ExecutorError } from '../errors.js';
import { drainOperator, serializeKey, SCAN_BATCH } from './utils.js';

// ---------------------------------------------------------------------------
// Materialize — executes CTE definition once, caches, then proxies main plan
// ---------------------------------------------------------------------------

export class PhysicalMaterialize implements PhysicalOperator {
  private materialized = false;

  constructor(
    private readonly cteDefinition: PhysicalOperator,
    private readonly mainPlan: PhysicalOperator,
    private readonly cteIndex: number,
    private readonly cteCache: Map<number, CTECacheEntry>,
  ) {}

  getLayout(): ColumnBinding[] {
    return this.mainPlan.getLayout();
  }

  async next(): Promise<Tuple[] | null> {
    if (!this.materialized) {
      const tuples = await drainOperator(this.cteDefinition);
      this.cteCache.set(this.cteIndex, {
        tuples,
        layout: this.cteDefinition.getLayout(),
      });
      this.materialized = true;
    }

    return this.mainPlan.next();
  }

  async reset(): Promise<void> {
    this.materialized = false;
    this.cteCache.delete(this.cteIndex);
    await this.cteDefinition.reset();
    await this.mainPlan.reset();
  }
}

// ---------------------------------------------------------------------------
// CTE Scan — reads from pre-materialized CTE cache
// ---------------------------------------------------------------------------

export class PhysicalCTEScan implements PhysicalOperator {
  private offset = 0;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly op: LogicalCTERef,
    private readonly cteCache: Map<number, CTECacheEntry>,
  ) {
    this.layout = op.getColumnBindings();
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    const entry = this.cteCache.get(this.op.cteIndex);
    if (!entry) {
      throw new ExecutorError(
        `CTE '${this.op.cteName}' not materialized yet`,
      );
    }

    if (this.offset >= entry.tuples.length) return null;

    const batch = entry.tuples.slice(this.offset, this.offset + SCAN_BATCH);
    this.offset += batch.length;
    return batch;
  }

  async reset(): Promise<void> {
    this.offset = 0;
  }
}

// ---------------------------------------------------------------------------
// Recursive CTE — iterative fixed-point execution
// ---------------------------------------------------------------------------

const MAX_RECURSIVE_ITERATIONS = 1000;

export class PhysicalRecursiveCTE implements PhysicalOperator {
  private result: Tuple[] | null = null;
  private offset = 0;

  constructor(
    private readonly anchor: PhysicalOperator,
    private readonly recursive: PhysicalOperator,
    private readonly cteIndex: number,
    private readonly cteCache: Map<number, CTECacheEntry>,
    private readonly isUnionAll: boolean,
  ) {}

  getLayout(): ColumnBinding[] {
    return this.anchor.getLayout();
  }

  async next(): Promise<Tuple[] | null> {
    if (!this.result) {
      this.result = await this.execute();
    }
    if (this.offset >= this.result.length) return null;
    const batch = this.result.slice(this.offset, this.offset + SCAN_BATCH);
    this.offset += batch.length;
    return batch;
  }

  async reset(): Promise<void> {
    this.result = null;
    this.offset = 0;
    this.cteCache.delete(this.cteIndex);
    await this.anchor.reset();
    await this.recursive.reset();
  }

  private async execute(): Promise<Tuple[]> {
    const allRows: Tuple[] = [];
    const seen = this.isUnionAll ? null : new Set<string>();
    const layout = this.anchor.getLayout();

    // 1. Execute anchor
    const anchorRows = await drainOperator(this.anchor);
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

    // 2. Iterative fixed-point
    for (let i = 0; i < MAX_RECURSIVE_ITERATIONS; i++) {
      if (workingTable.length === 0) break;

      // Put working table in cache for recursive term to read via PhysicalCTEScan
      this.cteCache.set(this.cteIndex, { tuples: workingTable, layout });

      await this.recursive.reset();
      const newRows = await drainOperator(this.recursive);

      if (newRows.length === 0) {
        workingTable = [];
        break;
      }

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

    // Set final cache so CTE refs in the main query can read the full result.
    // PhysicalMaterialize will overwrite this — that's fine, keeps us self-contained.
    this.cteCache.set(this.cteIndex, { tuples: allRows, layout });
    return allRows;
  }
}

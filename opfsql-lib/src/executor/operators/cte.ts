import type { LogicalCTERef, ColumnBinding } from '../../binder/types.js';
import type { PhysicalOperator, Tuple, CTECacheEntry } from '../types.js';
import { ExecutorError } from '../errors.js';
import { drainOperator } from './utils.js';

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

    const batch = entry.tuples.slice(this.offset, this.offset + 500);
    this.offset += batch.length;
    return batch;
  }

  async reset(): Promise<void> {
    this.offset = 0;
  }
}

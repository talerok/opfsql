import type {
  LogicalComparisonJoin,
  ColumnBinding,
} from '../../binder/types.js';
import type { PhysicalOperator, Tuple } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver, type Resolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { serializeValue } from '../evaluate/helpers.js';
import { drainOperator, JOIN_BATCH } from './utils.js';

// ---------------------------------------------------------------------------
// Hash Join (INNER / LEFT / SEMI / ANTI)
// ---------------------------------------------------------------------------

export class PhysicalHashJoin implements PhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly probeResolver: Resolver;
  private readonly buildResolver: Resolver;
  private readonly buildNullTuple: Tuple;

  private hashTable: Map<string, Tuple[]> | null = null;
  private emitter: AsyncGenerator<Tuple> | null = null;

  constructor(
    private readonly probe: PhysicalOperator,
    private readonly build: PhysicalOperator,
    private readonly op: LogicalComparisonJoin,
    private readonly ctx: EvalContext,
  ) {
    this.layout = op.joinType === 'SEMI' || op.joinType === 'ANTI'
      ? [...probe.getLayout()]
      : [...probe.getLayout(), ...build.getLayout()];
    this.probeResolver = buildResolver(probe.getLayout());
    this.buildResolver = buildResolver(build.getLayout());
    this.buildNullTuple = new Array(build.getLayout().length).fill(null);
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    if (!this.hashTable) await this.buildHashTable();
    if (!this.emitter) {
      this.emitter = this.op.joinType === 'SEMI' || this.op.joinType === 'ANTI'
        ? this.emitSemiAnti()
        : this.emitMatches();
    }

    const batch: Tuple[] = [];
    for (let i = 0; i < JOIN_BATCH; i++) {
      const { value, done } = await this.emitter.next();
      if (done) break;
      batch.push(value);
    }
    return batch.length > 0 ? batch : null;
  }

  async reset(): Promise<void> {
    this.hashTable = null;
    this.emitter = null;
    await this.probe.reset();
    await this.build.reset();
  }

  // --- Hash table construction ---

  private async buildHashTable(): Promise<void> {
    this.hashTable = new Map();
    const tuples = await drainOperator(this.build);

    for (const tuple of tuples) {
      const key = await this.evalKey(tuple, this.buildResolver, 'right');
      if (key === null) continue; // NULL keys never match
      const bucket = this.hashTable.get(key);
      if (bucket) bucket.push(tuple);
      else this.hashTable.set(key, [tuple]);
    }
  }

  // --- Tuple generators (flatten the state machine into linear async flow) ---

  private async *emitMatches(): AsyncGenerator<Tuple> {
    for await (const probeTuple of this.probeTuples()) {
      const key = await this.evalKey(probeTuple, this.probeResolver, 'left');

      if (key === null) {
        if (this.op.joinType === 'LEFT') {
          yield [...probeTuple, ...this.buildNullTuple];
        }
        continue;
      }

      const matches = this.hashTable!.get(key);
      if (matches && matches.length > 0) {
        for (const buildTuple of matches) {
          yield [...probeTuple, ...buildTuple];
        }
      } else if (this.op.joinType === 'LEFT') {
        yield [...probeTuple, ...this.buildNullTuple];
      }
    }
  }

  private async *emitSemiAnti(): AsyncGenerator<Tuple> {
    const isSemi = this.op.joinType === 'SEMI';

    for await (const probeTuple of this.probeTuples()) {
      const key = await this.evalKey(probeTuple, this.probeResolver, 'left');
      const hasMatch = key !== null
        && (this.hashTable!.get(key)?.length ?? 0) > 0;

      if (isSemi ? hasMatch : !hasMatch) {
        yield probeTuple;
      }
    }
  }

  /** Yields individual probe tuples from batched child output. */
  private async *probeTuples(): AsyncGenerator<Tuple> {
    while (true) {
      const batch = await this.probe.next();
      if (!batch) return;
      yield* batch;
    }
  }

  // --- Key evaluation ---

  private async evalKey(
    tuple: Tuple,
    resolver: Resolver,
    side: 'left' | 'right',
  ): Promise<string | null> {
    const parts: string[] = [];
    for (const cond of this.op.conditions) {
      const expr = side === 'left' ? cond.left : cond.right;
      const val = await evaluateExpression(expr, tuple, resolver, this.ctx);
      if (val === null) return null;
      parts.push(serializeValue(val));
    }
    return parts.join('\x00');
  }
}

// ---------------------------------------------------------------------------
// Nested Loop Join (CROSS JOIN)
// ---------------------------------------------------------------------------

export class PhysicalNestedLoopJoin implements PhysicalOperator {
  private readonly layout: ColumnBinding[];
  private rightTuples: Tuple[] | null = null;
  private emitter: AsyncGenerator<Tuple> | null = null;

  constructor(
    private readonly left: PhysicalOperator,
    private readonly right: PhysicalOperator,
  ) {
    this.layout = [...left.getLayout(), ...right.getLayout()];
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    if (!this.rightTuples) {
      this.rightTuples = await drainOperator(this.right);
      if (this.rightTuples.length === 0) return null;
    }
    if (!this.emitter) {
      this.emitter = this.emitCrossProduct();
    }

    const batch: Tuple[] = [];
    for (let i = 0; i < JOIN_BATCH; i++) {
      const { value, done } = await this.emitter.next();
      if (done) break;
      batch.push(value);
    }
    return batch.length > 0 ? batch : null;
  }

  async reset(): Promise<void> {
    this.rightTuples = null;
    this.emitter = null;
    await this.left.reset();
    await this.right.reset();
  }

  private async *emitCrossProduct(): AsyncGenerator<Tuple> {
    while (true) {
      const leftBatch = await this.left.next();
      if (!leftBatch) return;

      for (const leftTuple of leftBatch) {
        for (const rightTuple of this.rightTuples!) {
          yield [...leftTuple, ...rightTuple];
        }
      }
    }
  }
}

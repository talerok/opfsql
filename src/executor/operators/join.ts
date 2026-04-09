import type {
  LogicalComparisonJoin,
  ColumnBinding,
} from '../../binder/types.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver, type Resolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { serializeValue } from '../evaluate/helpers.js';
import { drainOperator } from './utils.js';

// ---------------------------------------------------------------------------
// Hash Join (INNER / LEFT)
// ---------------------------------------------------------------------------

export class PhysicalHashJoin implements PhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly probeResolver: Resolver;
  private readonly buildResolver: Resolver;
  private hashTable: Map<string, Tuple[]> | null = null;
  private probeBatch: Tuple[] | null = null;
  private probeIndex = 0;
  private matchIndex = 0;
  private currentMatches: Tuple[] | null = null;
  private currentProbe: Tuple | null = null;
  private probeExhausted = false;
  private readonly buildNullTuple: Tuple;

  constructor(
    private readonly probe: PhysicalOperator,
    private readonly build: PhysicalOperator,
    private readonly op: LogicalComparisonJoin,
    private readonly ctx: EvalContext,
  ) {
    this.layout = [
      ...probe.getLayout(),
      ...build.getLayout(),
    ];
    this.probeResolver = buildResolver(probe.getLayout());
    this.buildResolver = buildResolver(build.getLayout());
    this.buildNullTuple = new Array(build.getLayout().length).fill(null);
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    if (!this.hashTable) {
      await this.buildHashTable();
    }

    const result: Tuple[] = [];

    while (result.length < 100) {
      // Try to emit from current matches
      if (this.currentMatches && this.matchIndex < this.currentMatches.length) {
        result.push([
          ...this.currentProbe!,
          ...this.currentMatches[this.matchIndex++],
        ]);
        continue;
      }

      // Get next probe tuple
      const probe = await this.nextProbe();
      if (!probe) break;

      this.currentProbe = probe;
      const key = await this.probeKey(probe);

      if (key === null) {
        // NULL key — no match
        if (this.op.joinType === 'LEFT') {
          result.push([...probe, ...this.buildNullTuple]);
        }
        continue;
      }

      const matches = this.hashTable!.get(key);
      if (matches && matches.length > 0) {
        this.currentMatches = matches;
        this.matchIndex = 0;
        // Continue loop to emit first match
      } else if (this.op.joinType === 'LEFT') {
        result.push([...probe, ...this.buildNullTuple]);
      }
    }

    return result.length > 0 ? result : null;
  }

  async reset(): Promise<void> {
    this.hashTable = null;
    this.probeBatch = null;
    this.probeIndex = 0;
    this.probeExhausted = false;
    this.currentMatches = null;
    this.matchIndex = 0;
    await this.probe.reset();
    await this.build.reset();
  }

  private async buildHashTable(): Promise<void> {
    this.hashTable = new Map();
    const tuples = await drainOperator(this.build);

    for (const tuple of tuples) {
      const key = await this.buildKey(tuple);
      if (key === null) continue; // NULL key — never matches
      const bucket = this.hashTable.get(key);
      if (bucket) {
        bucket.push(tuple);
      } else {
        this.hashTable.set(key, [tuple]);
      }
    }
  }

  private async nextProbe(): Promise<Tuple | null> {
    while (true) {
      if (this.probeBatch && this.probeIndex < this.probeBatch.length) {
        return this.probeBatch[this.probeIndex++];
      }
      if (this.probeExhausted) return null;
      this.probeBatch = await this.probe.next();
      this.probeIndex = 0;
      if (!this.probeBatch) {
        this.probeExhausted = true;
        return null;
      }
    }
  }

  private async probeKey(tuple: Tuple): Promise<string | null> {
    return this.joinKey(tuple, this.probeResolver, 'left');
  }

  private async buildKey(tuple: Tuple): Promise<string | null> {
    return this.joinKey(tuple, this.buildResolver, 'right');
  }

  private async joinKey(
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
  private leftBatch: Tuple[] | null = null;
  private leftIdx = 0;
  private rightIdx = 0;
  private leftExhausted = false;

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

    const result: Tuple[] = [];

    while (result.length < 100) {
      // Get current left tuple
      if (!this.leftBatch || this.leftIdx >= this.leftBatch.length) {
        if (this.leftExhausted) break;
        this.leftBatch = await this.left.next();
        this.leftIdx = 0;
        if (!this.leftBatch) {
          this.leftExhausted = true;
          break;
        }
      }

      const leftTuple = this.leftBatch[this.leftIdx];
      result.push([...leftTuple, ...this.rightTuples[this.rightIdx]]);
      this.rightIdx++;

      if (this.rightIdx >= this.rightTuples.length) {
        this.rightIdx = 0;
        this.leftIdx++;
      }
    }

    return result.length > 0 ? result : null;
  }

  async reset(): Promise<void> {
    this.rightTuples = null;
    this.leftBatch = null;
    this.leftIdx = 0;
    this.rightIdx = 0;
    this.leftExhausted = false;
    await this.left.reset();
    await this.right.reset();
  }
}

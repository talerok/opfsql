import type {
  ColumnBinding,
  LogicalComparisonJoin,
} from "../../binder/types.js";
import { serializeValue } from "../evaluate/utils/serialize.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { evaluateExpression } from "../evaluate/index.js";
import { buildResolver, type Resolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { drainOperator, JOIN_BATCH } from "./utils.js";

// ---------------------------------------------------------------------------
// Hash Join (INNER / LEFT / SEMI / ANTI)
// ---------------------------------------------------------------------------

export class PhysicalHashJoin implements SyncPhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly probeResolver: Resolver;
  private readonly buildResolver: Resolver;
  private readonly buildNullTuple: Tuple;

  private hashTable: Map<string, Tuple[]> | null = null;
  private emitter: Iterator<Tuple> | null = null;

  constructor(
    private readonly probe: SyncPhysicalOperator,
    private readonly build: SyncPhysicalOperator,
    private readonly op: LogicalComparisonJoin,
    private readonly ctx: SyncEvalContext,
  ) {
    this.layout =
      op.joinType === "SEMI" || op.joinType === "ANTI"
        ? [...probe.getLayout()]
        : [...probe.getLayout(), ...build.getLayout()];
    this.probeResolver = buildResolver(probe.getLayout());
    this.buildResolver = buildResolver(build.getLayout());
    this.buildNullTuple = new Array(build.getLayout().length).fill(null);
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (!this.hashTable) this.buildHashTable();
    if (!this.emitter) {
      this.emitter =
        this.op.joinType === "SEMI" || this.op.joinType === "ANTI"
          ? this.emitSemiAnti()
          : this.emitMatches();
    }

    const batch: Tuple[] = [];
    for (let i = 0; i < JOIN_BATCH; i++) {
      const { value, done } = this.emitter.next();
      if (done) break;
      batch.push(value);
    }
    return batch.length > 0 ? batch : null;
  }

  reset(): void {
    this.hashTable = null;
    this.emitter = null;
    this.probe.reset();
    this.build.reset();
  }

  private buildHashTable(): void {
    this.hashTable = new Map();
    const tuples = drainOperator(this.build);

    for (const tuple of tuples) {
      const key = this.evalKey(tuple, this.buildResolver, "right");
      if (key === null) continue;
      const bucket = this.hashTable.get(key);
      if (bucket) bucket.push(tuple);
      else this.hashTable.set(key, [tuple]);
    }
  }

  private *emitMatches(): Iterator<Tuple> {
    for (const probeTuple of this.probeTuples()) {
      const key = this.evalKey(probeTuple, this.probeResolver, "left");

      if (key === null) {
        if (this.op.joinType === "LEFT")
          yield [...probeTuple, ...this.buildNullTuple];
        continue;
      }

      const matches = this.hashTable!.get(key);
      if (matches && matches.length > 0) {
        for (const buildTuple of matches) yield [...probeTuple, ...buildTuple];
      } else if (this.op.joinType === "LEFT") {
        yield [...probeTuple, ...this.buildNullTuple];
      }
    }
  }

  private *emitSemiAnti(): Iterator<Tuple> {
    const isSemi = this.op.joinType === "SEMI";
    for (const probeTuple of this.probeTuples()) {
      const key = this.evalKey(probeTuple, this.probeResolver, "left");
      const hasMatch =
        key !== null && (this.hashTable!.get(key)?.length ?? 0) > 0;
      if (isSemi ? hasMatch : !hasMatch) yield probeTuple;
    }
  }

  private *probeTuples(): Generator<Tuple> {
    while (true) {
      const batch = this.probe.next();
      if (!batch) return;
      yield* batch;
    }
  }

  private evalKey(
    tuple: Tuple,
    resolver: Resolver,
    side: "left" | "right",
  ): string | null {
    const parts: string[] = [];
    for (const cond of this.op.conditions) {
      const expr = side === "left" ? cond.left : cond.right;
      const val = evaluateExpression(expr, tuple, resolver, this.ctx);
      if (val === null) return null;
      parts.push(serializeValue(val));
    }
    return parts.join("\x00");
  }
}

// ---------------------------------------------------------------------------
// Nested Loop Join (CROSS JOIN)
// ---------------------------------------------------------------------------

export class PhysicalNestedLoopJoin implements SyncPhysicalOperator {
  private readonly layout: ColumnBinding[];
  private rightTuples: Tuple[] | null = null;
  private emitter: Iterator<Tuple> | null = null;

  constructor(
    private readonly left: SyncPhysicalOperator,
    private readonly right: SyncPhysicalOperator,
  ) {
    this.layout = [...left.getLayout(), ...right.getLayout()];
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (!this.rightTuples) {
      this.rightTuples = drainOperator(this.right);
      if (this.rightTuples.length === 0) return null;
    }
    if (!this.emitter) this.emitter = this.emitCrossProduct();

    const batch: Tuple[] = [];
    for (let i = 0; i < JOIN_BATCH; i++) {
      const { value, done } = this.emitter.next();
      if (done) break;
      batch.push(value);
    }
    return batch.length > 0 ? batch : null;
  }

  reset(): void {
    this.rightTuples = null;
    this.emitter = null;
    this.left.reset();
    this.right.reset();
  }

  private *emitCrossProduct(): Generator<Tuple> {
    while (true) {
      const leftBatch = this.left.next();
      if (!leftBatch) return;
      for (const leftTuple of leftBatch) {
        for (const rightTuple of this.rightTuples!) {
          yield [...leftTuple, ...rightTuple];
        }
      }
    }
  }
}

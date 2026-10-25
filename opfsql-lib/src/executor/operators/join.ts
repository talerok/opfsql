import type {
  BoundExpression,
  ColumnBinding,
  LogicalComparisonJoin,
} from "../../sql/binder/types.js";
import { rowKey } from "../../values/index.js";
import {
  comparisonConversion,
  type CompiledExpression,
  compileExpression,
  type ExecutionContext,
} from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { drainOperator, JOIN_BATCH } from "./shared.js";

// ---------------------------------------------------------------------------
// Hash Join (INNER / LEFT / SEMI / ANTI). Rows match when their join keys
// have the same rowKey — equal under compareValues — and a NULL key matches
// nothing, as `=` never does.
// ---------------------------------------------------------------------------

export class PhysicalHashJoin implements SyncPhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly probeKeys: readonly CompiledExpression[];
  private readonly buildKeys: readonly CompiledExpression[];
  private readonly buildNullTuple: Tuple;
  private readonly isSemiOrAnti: boolean;

  private hashTable: Map<string, Tuple[]> | null = null;
  private emitter: Iterator<Tuple> | null = null;

  constructor(
    private readonly probe: SyncPhysicalOperator,
    private readonly build: SyncPhysicalOperator,
    private readonly operator: LogicalComparisonJoin,
    context: ExecutionContext,
  ) {
    this.isSemiOrAnti =
      operator.joinType === "SEMI" || operator.joinType === "ANTI";
    this.layout = this.isSemiOrAnti
      ? [...probe.getLayout()]
      : [...probe.getLayout(), ...build.getLayout()];
    const probeResolver = buildResolver(probe.getLayout());
    const buildSideResolver = buildResolver(build.getLayout());
    // A key of unknown type takes the other side's affinity, as `=` does.
    this.probeKeys = operator.conditions.map((condition) =>
      joinKeyPart(
        condition.left,
        condition.right,
        compileExpression(condition.left, probeResolver, context),
      ),
    );
    this.buildKeys = operator.conditions.map((condition) =>
      joinKeyPart(
        condition.right,
        condition.left,
        compileExpression(condition.right, buildSideResolver, context),
      ),
    );
    this.buildNullTuple = Array.from(build.getLayout(), () => null);
  }

  private buildHashTable(): Map<string, Tuple[]> {
    const hashTable = new Map<string, Tuple[]>();
    for (const row of drainOperator(this.build)) {
      const key = joinKey(row, this.buildKeys);
      if (key === null) continue;
      const bucket = hashTable.get(key);
      if (bucket === undefined) hashTable.set(key, [row]);
      else bucket.push(row);
    }
    return hashTable;
  }

  private *emitMatches(hashTable: Map<string, Tuple[]>): Iterator<Tuple> {
    const isLeft = this.operator.joinType === "LEFT";
    for (const probeRow of this.probeRows()) {
      const key = joinKey(probeRow, this.probeKeys);
      const matches = key === null ? undefined : hashTable.get(key);
      if (matches !== undefined && matches.length > 0) {
        for (const buildRow of matches) yield [...probeRow, ...buildRow];
      } else if (isLeft) {
        yield [...probeRow, ...this.buildNullTuple];
      }
    }
  }

  private *emitSemiAnti(hashTable: Map<string, Tuple[]>): Iterator<Tuple> {
    const isSemi = this.operator.joinType === "SEMI";
    for (const probeRow of this.probeRows()) {
      const key = joinKey(probeRow, this.probeKeys);
      const hasMatch = key !== null && (hashTable.get(key)?.length ?? 0) > 0;
      if (isSemi === hasMatch) yield probeRow;
    }
  }

  private *probeRows(): Generator<Tuple> {
    for (
      let batch = this.probe.next();
      batch !== null;
      batch = this.probe.next()
    ) {
      yield* batch;
    }
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    const hashTable = (this.hashTable ??= this.buildHashTable());
    this.emitter ??= this.isSemiOrAnti
      ? this.emitSemiAnti(hashTable)
      : this.emitMatches(hashTable);

    const batch: Tuple[] = [];
    for (let count = 0; count < JOIN_BATCH; count++) {
      const next = this.emitter.next();
      if (next.done === true) break;
      batch.push(next.value);
    }
    return batch.length > 0 ? batch : null;
  }

  reset(): void {
    this.hashTable = null;
    this.emitter = null;
    this.probe.reset();
    this.build.reset();
  }
}

function joinKeyPart(
  side: BoundExpression,
  other: BoundExpression,
  value: CompiledExpression,
): CompiledExpression {
  const convert = comparisonConversion(side.returnType, other.returnType);
  return (row) => convert(value(row));
}

/** The hash key of a row's join values; null if one is NULL. */
function joinKey(
  row: Tuple,
  keys: readonly CompiledExpression[],
): string | null {
  const values = keys.map((key) => key(row));
  return values.includes(null) ? null : rowKey(values);
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

  private *emitCrossProduct(rightTuples: readonly Tuple[]): Generator<Tuple> {
    for (
      let batch = this.left.next();
      batch !== null;
      batch = this.left.next()
    ) {
      for (const leftTuple of batch) {
        for (const rightTuple of rightTuples) {
          yield [...leftTuple, ...rightTuple];
        }
      }
    }
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.rightTuples === null) {
      this.rightTuples = drainOperator(this.right);
      if (this.rightTuples.length === 0) return null;
    }
    this.emitter ??= this.emitCrossProduct(this.rightTuples);

    const batch: Tuple[] = [];
    for (let count = 0; count < JOIN_BATCH; count++) {
      const next = this.emitter.next();
      if (next.done === true) break;
      batch.push(next.value);
    }
    return batch.length > 0 ? batch : null;
  }

  reset(): void {
    this.rightTuples = null;
    this.emitter = null;
    this.left.reset();
    this.right.reset();
  }
}

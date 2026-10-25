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
import { drainOperator, JOIN_BATCH, rowsOf, takeBatch } from "./shared.js";

// ---------------------------------------------------------------------------
// Hash Join (INNER / LEFT / SEMI / ANTI). Rows match when their join keys
// have the same rowKey — equal under compareValues — and a NULL key matches
// nothing, as `=` never does.
// ---------------------------------------------------------------------------

/** Shared by every probe row without a match: never modified. */
const NO_MATCHES: readonly Tuple[] = [];

export class PhysicalHashJoin implements SyncPhysicalOperator {
  private readonly layout: ColumnBinding[];

  private readonly probeKeys: readonly CompiledExpression[];

  private readonly buildKeys: readonly CompiledExpression[];

  private readonly buildNullTuple: Tuple;

  private hashTable: Map<string, Tuple[]> | null = null;

  private emitter: Iterator<Tuple> | null = null;

  constructor(
    private readonly probe: SyncPhysicalOperator,
    private readonly build: SyncPhysicalOperator,
    private readonly operator: LogicalComparisonJoin,
    context: ExecutionContext,
  ) {
    const buildLayout = this.isSemiOrAnti() ? [] : build.getLayout();
    this.layout = [...probe.getLayout(), ...buildLayout];
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

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    // The build side is read first, even when the probe side is empty.
    this.hashTable ??= this.buildHashTable();
    this.emitter ??= this.emit();
    return takeBatch(this.emitter, JOIN_BATCH);
  }

  reset(): void {
    this.hashTable = null;
    this.emitter = null;
    this.probe.reset();
    this.build.reset();
  }

  private emit(): Iterator<Tuple> {
    return this.isSemiOrAnti() ? this.emitSemiAnti() : this.emitMatches();
  }

  private isSemiOrAnti(): boolean {
    const { joinType } = this.operator;
    return joinType === "SEMI" || joinType === "ANTI";
  }

  private buildHashTable(): Map<string, Tuple[]> {
    const hashTable = new Map<string, Tuple[]>();
    for (const row of rowsOf(this.build)) {
      const key = joinKey(row, this.buildKeys);
      if (key === null) continue;
      const bucket = hashTable.get(key);
      if (bucket === undefined) hashTable.set(key, [row]);
      else bucket.push(row);
    }
    return hashTable;
  }

  /** The build rows a probe row joins with; none for a NULL key. */
  private matchesFor(probeRow: Tuple): readonly Tuple[] {
    const key = joinKey(probeRow, this.probeKeys);
    if (key === null) return NO_MATCHES;
    return this.hashTable?.get(key) ?? NO_MATCHES;
  }

  private *emitMatches(): Iterator<Tuple> {
    const isLeft = this.operator.joinType === "LEFT";
    for (const probeRow of rowsOf(this.probe)) {
      const matches = this.matchesFor(probeRow);
      for (const buildRow of matches) yield [...probeRow, ...buildRow];
      if (isLeft && matches.length === 0) {
        yield [...probeRow, ...this.buildNullTuple];
      }
    }
  }

  private *emitSemiAnti(): Iterator<Tuple> {
    const isSemi = this.operator.joinType === "SEMI";
    for (const probeRow of rowsOf(this.probe)) {
      const hasMatch = this.matchesFor(probeRow).length > 0;
      if (isSemi === hasMatch) yield probeRow;
    }
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
    this.emitter ??= this.emitCrossProduct();
    return takeBatch(this.emitter, JOIN_BATCH);
  }

  reset(): void {
    this.emitter = null;
    this.left.reset();
    this.right.reset();
  }

  private *emitCrossProduct(): Generator<Tuple> {
    const rightRows = drainOperator(this.right);
    if (rightRows.length === 0) return;
    for (const leftRow of rowsOf(this.left)) {
      for (const rightRow of rightRows) yield [...leftRow, ...rightRow];
    }
  }
}

import type {
  BoundAggregateExpression,
  ColumnBinding,
  LogicalAggregate,
} from "../../binder/types.js";
import { compareValues, isTruthy } from "../evaluate/utils/compare.js";
import { serializeValue } from "../evaluate/utils/serialize.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { evaluateExpression } from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple, Value } from "../types.js";
import { drainOperator, serializeKey } from "./utils.js";

interface AggState {
  count: number;
  sum: number;
  min: Value;
  max: Value;
  distinctSet: Set<string> | null;
}

interface GroupEntry {
  groupValues: Value[];
  aggs: AggState[];
}

export class PhysicalHashAggregate implements SyncPhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly childResolver;
  private emitted = false;

  constructor(
    private readonly child: SyncPhysicalOperator,
    private readonly op: LogicalAggregate,
    private readonly ctx: SyncEvalContext,
  ) {
    this.childResolver = buildResolver(child.getLayout());
    this.layout = op.columnBindings;
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.emitted) return null;
    this.emitted = true;

    const tuples = drainOperator(this.child);
    const groups = this.buildGroups(tuples);
    const result = this.finalize(groups);

    if (!this.op.havingExpression) return result.length > 0 ? result : null;
    return this.applyHaving(result);
  }

  reset(): void {
    this.emitted = false;
    this.child.reset();
  }

  private buildGroups(tuples: Tuple[]): Map<string, GroupEntry> {
    const groups = new Map<string, GroupEntry>();

    if (tuples.length === 0 && this.op.groups.length === 0) {
      groups.set("", { groupValues: [], aggs: this.newAggStates() });
      return groups;
    }

    for (const tuple of tuples) {
      const { key, values } = this.computeGroupKey(tuple);
      let group = groups.get(key);
      if (!group) {
        group = { groupValues: values, aggs: this.newAggStates() };
        groups.set(key, group);
      }
      for (let i = 0; i < this.op.expressions.length; i++) {
        this.updateAgg(group.aggs[i], this.op.expressions[i], tuple);
      }
    }

    return groups;
  }

  private computeGroupKey(tuple: Tuple): { key: string; values: Value[] } {
    const values: Value[] = [];
    for (const groupExpr of this.op.groups) {
      values.push(
        evaluateExpression(groupExpr, tuple, this.childResolver, this.ctx),
      );
    }
    return { key: serializeKey(values), values };
  }

  private newAggStates(): AggState[] {
    return this.op.expressions.map(() => ({
      count: 0,
      sum: 0,
      min: null,
      max: null,
      distinctSet: null,
    }));
  }

  private updateAgg(
    state: AggState,
    aggExpr: BoundAggregateExpression,
    tuple: Tuple,
  ): void {
    if (aggExpr.isStar) {
      state.count++;
      return;
    }

    const val = evaluateExpression(
      aggExpr.children[0],
      tuple,
      this.childResolver,
      this.ctx,
    );
    if (val === null) return;

    if (aggExpr.distinct) {
      if (!state.distinctSet) state.distinctSet = new Set();
      const key = serializeValue(val);
      if (state.distinctSet.has(key)) return;
      state.distinctSet.add(key);
    }

    state.count++;
    if (typeof val === "number") state.sum += val;
    if (state.min === null || compareValues(val, state.min) < 0)
      state.min = val;
    if (state.max === null || compareValues(val, state.max) > 0)
      state.max = val;
  }

  private finalize(groups: Map<string, GroupEntry>): Tuple[] {
    const result: Tuple[] = [];
    for (const { groupValues, aggs } of groups.values()) {
      result.push([
        ...groupValues,
        ...aggs.map((agg, i) => this.finalizeAgg(agg, this.op.expressions[i])),
      ]);
    }
    return result;
  }

  private finalizeAgg(
    state: AggState,
    aggExpr: BoundAggregateExpression,
  ): Value {
    switch (aggExpr.functionName) {
      case "COUNT":
        return state.count;
      case "SUM":
        return state.count === 0 ? null : state.sum;
      case "AVG":
        return state.count === 0 ? null : state.sum / state.count;
      case "MIN":
        return state.min;
      case "MAX":
        return state.max;
      default:
        return null;
    }
  }

  private applyHaving(result: Tuple[]): Tuple[] | null {
    const aggResolver = buildResolver(this.layout);
    const filtered: Tuple[] = [];
    for (const tuple of result) {
      const val = evaluateExpression(
        this.op.havingExpression!,
        tuple,
        aggResolver,
        this.ctx,
      );
      if (isTruthy(val)) filtered.push(tuple);
    }
    return filtered.length > 0 ? filtered : null;
  }
}

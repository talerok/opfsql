import type {
  LogicalAggregate,
  BoundAggregateExpression,
  ColumnBinding,
} from '../../binder/types.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';
import { buildResolver } from '../resolve.js';
import { evaluateExpression } from '../evaluate/index.js';
import { isTruthy, serializeValue, compareValues } from '../evaluate/helpers.js';
import { drainOperator } from './utils.js';

interface AggState {
  count: number;
  sum: number;
  min: Value;
  max: Value;
  distinctSet: Set<string> | null;
}

export class PhysicalHashAggregate implements PhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly childResolver;
  private emitted = false;

  constructor(
    private readonly child: PhysicalOperator,
    private readonly op: LogicalAggregate,
    private readonly ctx: EvalContext,
  ) {
    this.childResolver = buildResolver(child.getLayout());
    this.layout = op.getColumnBindings();
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    if (this.emitted) return null;
    this.emitted = true;

    const tuples = await drainOperator(this.child);
    const groups = await this.buildGroups(tuples);
    const result = this.finalize(groups);

    // Apply HAVING
    if (this.op.havingExpression) {
      const aggResolver = buildResolver(this.layout);
      const filtered: Tuple[] = [];
      for (const tuple of result) {
        const val = await evaluateExpression(
          this.op.havingExpression,
          tuple,
          aggResolver,
          this.ctx,
        );
        if (isTruthy(val)) filtered.push(tuple);
      }
      return filtered.length > 0 ? filtered : null;
    }

    return result.length > 0 ? result : null;
  }

  async reset(): Promise<void> {
    this.emitted = false;
    await this.child.reset();
  }

  private async buildGroups(
    tuples: Tuple[],
  ): Promise<Map<string, { groupValues: Value[]; aggs: AggState[] }>> {
    const groups = new Map<
      string,
      { groupValues: Value[]; aggs: AggState[] }
    >();

    const hasGroups = this.op.groups.length > 0;

    // If no input and no GROUP BY, we still produce one row
    if (tuples.length === 0 && !hasGroups) {
      const aggs = this.op.expressions.map(() => this.newAggState());
      groups.set('', { groupValues: [], aggs });
      return groups;
    }

    for (const tuple of tuples) {
      // Evaluate group-by expressions
      const groupValues: Value[] = [];
      const keyParts: string[] = [];
      for (const groupExpr of this.op.groups) {
        const val = await evaluateExpression(
          groupExpr,
          tuple,
          this.childResolver,
          this.ctx,
        );
        groupValues.push(val);
        keyParts.push(serializeValue(val));
      }
      const key = keyParts.join('\x00');

      let entry = groups.get(key);
      if (!entry) {
        entry = {
          groupValues,
          aggs: this.op.expressions.map(() => this.newAggState()),
        };
        groups.set(key, entry);
      }

      // Update each aggregate
      for (let i = 0; i < this.op.expressions.length; i++) {
        await this.updateAgg(
          entry.aggs[i],
          this.op.expressions[i],
          tuple,
        );
      }
    }

    return groups;
  }

  private finalize(
    groups: Map<string, { groupValues: Value[]; aggs: AggState[] }>,
  ): Tuple[] {
    const result: Tuple[] = [];
    for (const { groupValues, aggs } of groups.values()) {
      const tuple: Tuple = [
        ...groupValues,
        ...aggs.map((agg, i) =>
          this.finalizeAgg(agg, this.op.expressions[i]),
        ),
      ];
      result.push(tuple);
    }
    return result;
  }

  private newAggState(): AggState {
    return { count: 0, sum: 0, min: null, max: null, distinctSet: null };
  }

  private async updateAgg(
    state: AggState,
    aggExpr: BoundAggregateExpression,
    tuple: Tuple,
  ): Promise<void> {
    if (aggExpr.isStar) {
      // COUNT(*)
      state.count++;
      return;
    }

    const val = await evaluateExpression(
      aggExpr.children[0],
      tuple,
      this.childResolver,
      this.ctx,
    );

    if (val === null) return; // NULL ignored by all aggregates

    // Handle DISTINCT
    if (aggExpr.distinct) {
      if (!state.distinctSet) state.distinctSet = new Set();
      const key = serializeValue(val);
      if (state.distinctSet.has(key)) return;
      state.distinctSet.add(key);
    }

    state.count++;
    if (typeof val === 'number') {
      state.sum += val;
    }
    if (state.min === null || compareValues(val, state.min) < 0) {
      state.min = val;
    }
    if (state.max === null || compareValues(val, state.max) > 0) {
      state.max = val;
    }
  }

  private finalizeAgg(
    state: AggState,
    aggExpr: BoundAggregateExpression,
  ): Value {
    switch (aggExpr.functionName) {
      case 'COUNT':
        return state.count;
      case 'SUM':
        return state.count === 0 ? null : state.sum;
      case 'AVG':
        return state.count === 0 ? null : state.sum / state.count;
      case 'MIN':
        return state.min;
      case 'MAX':
        return state.max;
    }
  }
}

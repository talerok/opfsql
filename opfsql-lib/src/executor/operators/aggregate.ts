import {
  type Accumulator,
  startAggregate,
  type StaticType,
  staticType,
} from "../../functions/index.js";
import type {
  BoundAggregateExpression,
  ColumnBinding,
  LogicalAggregate,
} from "../../sql/binder/types.js";
import { rowKey, type Value, valueKey } from "../../values/index.js";
import {
  type CompiledExpression,
  compileExpression,
  compilePredicate,
  type ExecutionContext,
} from "../evaluate/index.js";
import { buildResolver } from "../resolve.js";
import type { SyncPhysicalOperator, Tuple } from "../types.js";
import { drainOperator, type RowFilter } from "./shared.js";

// ---------------------------------------------------------------------------
// GROUP BY and aggregates. Groups are keyed by rowKey, so two group values
// fall in one group exactly when compareValues calls them equal — the same
// equality DISTINCT, UNION and joins use. The aggregates come from the
// function registry, resolved by the binder.
// ---------------------------------------------------------------------------

interface CompiledAggregate {
  readonly expression: BoundAggregateExpression;
  readonly arguments: readonly CompiledExpression[];
  readonly argumentTypes: readonly StaticType[];
  /** One row's argument values, refilled for each row. */
  readonly values: Value[];
}

interface Group {
  readonly values: readonly Value[];
  readonly accumulators: readonly Accumulator[];
}

export class PhysicalHashAggregate implements SyncPhysicalOperator {
  private readonly layout: ColumnBinding[];
  private readonly groupKeys: readonly CompiledExpression[];
  private readonly aggregates: readonly CompiledAggregate[];
  private readonly having: RowFilter | null;
  private emitted = false;

  constructor(
    private readonly child: SyncPhysicalOperator,
    private readonly operator: LogicalAggregate,
    context: ExecutionContext,
  ) {
    const resolver = buildResolver(child.getLayout());
    this.layout = operator.columnBindings;
    this.groupKeys = operator.groups.map((group) =>
      compileExpression(group, resolver, context),
    );
    this.aggregates = operator.expressions.map((expression) => ({
      expression,
      arguments: expression.children.map((argument) =>
        compileExpression(argument, resolver, context),
      ),
      argumentTypes: expression.children.map((argument) =>
        staticType(argument.returnType),
      ),
      values: [],
    }));
    this.having =
      operator.havingExpression === null
        ? null
        : compilePredicate(
            operator.havingExpression,
            buildResolver(this.layout),
            context,
          );
  }

  private buildGroups(rows: readonly Tuple[]): Map<string, Group> {
    const groups = new Map<string, Group>();
    // Without GROUP BY there is one group, even over no rows.
    if (rows.length === 0 && this.groupKeys.length === 0) {
      groups.set("", this.newGroup([]));
    }
    const isSingleKey = this.groupKeys.length === 1;
    for (const row of rows) {
      const values = this.groupKeys.map((key) => key(row));
      // One key needs no list encoding: equal values, equal keys.
      const key = isSingleKey ? valueKey(values[0]) : rowKey(values);
      let group = groups.get(key);
      if (group === undefined) {
        group = this.newGroup(values);
        groups.set(key, group);
      }
      this.accumulate(group, row);
    }
    return groups;
  }

  private accumulate(group: Group, row: Tuple): void {
    for (let index = 0; index < this.aggregates.length; index++) {
      const aggregate = this.aggregates[index];
      const { values } = aggregate;
      for (
        let position = 0;
        position < aggregate.arguments.length;
        position++
      ) {
        values[position] = aggregate.arguments[position](row);
      }
      group.accumulators[index].add(values);
    }
  }

  private newGroup(values: readonly Value[]): Group {
    return {
      values,
      accumulators: this.aggregates.map(({ expression, argumentTypes }) =>
        startAggregate(expression.function, argumentTypes, expression.distinct),
      ),
    };
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.emitted) return null;
    this.emitted = true;

    const groups = this.buildGroups(drainOperator(this.child));
    const rows = Array.from(groups.values(), (group) => [
      ...group.values,
      ...group.accumulators.map((accumulator) => accumulator.result()),
    ]);
    const { having } = this;
    const result = having === null ? rows : rows.filter((row) => having(row));
    return result.length > 0 ? result : null;
  }

  reset(): void {
    this.emitted = false;
    this.child.reset();
  }
}

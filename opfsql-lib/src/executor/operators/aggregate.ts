import {
  type Accumulator,
  startAggregate,
  staticType,
} from "../../functions/index.js";
import type {
  BoundAggregateExpression,
  ColumnBinding,
  LogicalAggregate,
} from "../../sql/binder/types.js";
import { rowKey, type Value } from "../../values/index.js";
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
    for (const row of rows) {
      const values = this.groupKeys.map((key) => key(row));
      const key = rowKey(values);
      let group = groups.get(key);
      if (group === undefined) {
        group = this.newGroup(values);
        groups.set(key, group);
      }
      for (const [index, aggregate] of this.aggregates.entries()) {
        group.accumulators[index]?.add(
          aggregate.arguments.map((argument) => argument(row)),
        );
      }
    }
    return groups;
  }

  private newGroup(values: readonly Value[]): Group {
    return {
      values,
      accumulators: this.aggregates.map(({ expression }) =>
        startAggregate(
          expression.function,
          expression.children.map((child) => staticType(child.returnType)),
          expression.distinct,
        ),
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

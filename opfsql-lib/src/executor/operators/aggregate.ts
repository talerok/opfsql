import {
  type Accumulator,
  argumentTypes,
  startAggregate,
  type StaticType,
} from "../../functions/index.js";
import type {
  BoundAggregateExpression,
  BoundExpression,
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
import { buildResolver, type Resolver } from "../resolve.js";
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
  readonly types: readonly StaticType[];
  /** A row's argument values, in one array refilled for each row. */
  readonly argumentValues: (row: Tuple) => readonly Value[];
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
      types: argumentTypes(expression.children),
      argumentValues: argumentReader(
        expression.children.map((argument) =>
          compileExpression(argument, resolver, context),
        ),
      ),
      values: [],
    }));
    this.having = compileHaving(
      operator.havingExpression,
      buildResolver(this.layout),
      context,
    );
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

  private buildGroups(rows: readonly Tuple[]): Map<string, Group> {
    const groups = new Map<string, Group>();
    // Without GROUP BY there is one group, even over no rows.
    if (rows.length === 0 && this.groupKeys.length === 0) {
      groups.set("", this.newGroup([]));
    }
    for (const row of rows) {
      const group = this.groupFor(
        groups,
        this.groupKeys.map((key) => key(row)),
      );
      this.accumulate(group, row);
    }
    return groups;
  }

  /** Adds a row to each aggregate of its group. */
  private accumulate(group: Group, row: Tuple): void {
    for (let index = 0; index < this.aggregates.length; index++) {
      const values = this.aggregates[index].argumentValues(row);
      group.accumulators[index].add(values);
    }
  }

  /** The group of rows whose GROUP BY values are `values`, made if new. */
  private groupFor(groups: Map<string, Group>, values: Value[]): Group {
    // One key needs no list encoding: equal values, equal keys.
    const key = values.length === 1 ? valueKey(values[0]) : rowKey(values);
    const existing = groups.get(key);
    if (existing !== undefined) return existing;
    const group = this.newGroup(values);
    groups.set(key, group);
    return group;
  }

  private newGroup(values: readonly Value[]): Group {
    return {
      values,
      accumulators: this.aggregates.map(({ expression, types }) =>
        startAggregate(expression.function, types, expression.distinct),
      ),
    };
  }
}

/**
 * Reads a row's argument values into one array, reused for every row:
 * accumulators keep values, never the array.
 */
function argumentReader(
  compiled: readonly CompiledExpression[],
): (row: Tuple) => readonly Value[] {
  const values: Value[] = [];
  return (row) => {
    // A counted loop: this runs for every row and aggregate.
    for (let position = 0; position < compiled.length; position++) {
      values[position] = compiled[position](row);
    }
    return values;
  };
}

/** HAVING over the aggregate's output rows, if there is one. */
function compileHaving(
  having: BoundExpression | null,
  resolver: Resolver,
  context: ExecutionContext,
): RowFilter | null {
  if (having === null) return null;
  return compilePredicate(having, resolver, context);
}

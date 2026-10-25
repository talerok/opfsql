import {
  type BoundExpression,
  type LogicalGet,
  type LogicalOperator,
  LogicalOperatorType,
} from "../../../binder/types.js";
import type {
  ICatalog,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../../../store/types.js";
import {
  compilePredicate,
  type ExecutionContext,
} from "../../evaluate/index.js";
import { ExecutorError } from "../../errors.js";
import { compileTableFilters, type RowFilter } from "../../operators/shared.js";
import { buildResolver, type Resolver } from "../../resolve.js";

/** Where an UPDATE or DELETE runs: the storage and the statement. */
export interface DmlTarget {
  readonly rowManager: SyncIRowManager;
  readonly context: ExecutionContext;
  readonly catalog?: ICatalog;
  readonly indexManager?: SyncIIndexManager;
}

/** The rows an UPDATE or DELETE touches: a table and conditions on it. */
export interface DmlScan {
  readonly table: LogicalGet;
  /** Every column of the table, in order: rows are read whole. */
  readonly columnIds: readonly number[];
  /** Where the columns are in a row read with `columnIds`. */
  readonly resolver: Resolver;
  /** Every condition — pushed-down table filters and WHERE — compiled. */
  readonly filters: readonly RowFilter[];
}

export function extractDmlScan(
  child: LogicalOperator,
  context: ExecutionContext,
): DmlScan {
  const { table, conditions } = collectConditions(child);
  const columnIds = table.schema.columns.map((_column, index) => index);
  const resolver = buildResolver(
    columnIds.map((columnIndex) => ({
      tableIndex: table.tableIndex,
      columnIndex,
    })),
  );
  const filters = [
    ...compileTableFilters(table.tableFilters, resolver, context),
    ...conditions.map((condition) =>
      compilePredicate(condition, resolver, context),
    ),
  ];
  return { table, columnIds, resolver, filters };
}

function collectConditions(child: LogicalOperator): {
  table: LogicalGet;
  conditions: BoundExpression[];
} {
  if (child.type === LogicalOperatorType.LOGICAL_FILTER) {
    const inner = collectConditions(child.children[0]);
    return {
      table: inner.table,
      conditions: [...child.expressions, ...inner.conditions],
    };
  }
  if (child.type === LogicalOperatorType.LOGICAL_GET) {
    return { table: child, conditions: [] };
  }
  throw new ExecutorError(`Unexpected node ${child.type} in DML scan tree`);
}

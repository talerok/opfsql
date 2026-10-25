import {
  type BoundExpression,
  type LogicalGet,
  type LogicalOperator,
  LogicalOperatorType,
} from "../../../sql/binder/types.js";
import type {
  ICatalog,
  Row,
  SyncIIndexManager,
  SyncIRowManager,
} from "../../../store/types.js";
import {
  compilePredicate,
  type ExecutionContext,
} from "../../evaluate/index.js";
import { ExecutorError } from "../../errors.js";
import {
  compileTableFilters,
  passesFilters,
  type RowFilter,
} from "../../operators/shared.js";
import { buildResolver, type Resolver } from "../../resolve.js";
import type { Tuple } from "../../types.js";

/** Where an UPDATE or DELETE runs: the storage and the statement. */
export interface DmlTarget {
  readonly rowManager: SyncIRowManager;
  readonly context: ExecutionContext;
  readonly catalog?: ICatalog;
  readonly indexManager?: SyncIIndexManager;
}

/** The rows an UPDATE or DELETE touches: a table and conditions on it. */
export interface DmlScan {
  readonly get: LogicalGet;
  /** Where the table's columns are in a row from `rowToTuple`. */
  readonly resolver: Resolver;
  /** Every condition — pushed-down table filters and WHERE — compiled. */
  readonly filters: readonly RowFilter[];
}

export function extractDmlScan(
  child: LogicalOperator,
  context: ExecutionContext,
): DmlScan {
  const { get, conditions } = collectConditions(child);
  const resolver = buildResolver(
    get.schema.columns.map((_column, columnIndex) => ({
      tableIndex: get.tableIndex,
      columnIndex,
    })),
  );
  const filters = [
    ...compileTableFilters(get.tableFilters, resolver, context),
    ...conditions.map((condition) =>
      compilePredicate(condition, resolver, context),
    ),
  ];
  return { get, resolver, filters };
}

function collectConditions(child: LogicalOperator): {
  get: LogicalGet;
  conditions: BoundExpression[];
} {
  if (child.type === LogicalOperatorType.LOGICAL_FILTER) {
    const inner = collectConditions(child.children[0]);
    return {
      get: inner.get,
      conditions: [...child.expressions, ...inner.conditions],
    };
  }
  if (child.type === LogicalOperatorType.LOGICAL_GET) {
    return { get: child, conditions: [] };
  }
  throw new ExecutorError(`Unexpected node ${child.type} in DML scan tree`);
}

export function rowToTuple(row: Readonly<Row>, get: LogicalGet): Tuple {
  return get.schema.columns.map((column) =>
    Object.hasOwn(row, column.name) ? row[column.name] : column.defaultValue,
  );
}

export function passesFilter(row: Tuple, scan: DmlScan): boolean {
  return passesFilters(row, scan.filters);
}

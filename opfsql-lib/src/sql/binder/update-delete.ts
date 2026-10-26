import { SemanticError } from "../../errors.js";
import type { Table } from "../../schema/index.js";
import type {
  Assignment as AssignmentNode,
  DeleteStatement,
  ParsedExpression,
  UpdateStatement,
} from "../parser/index.js";
import {
  ROWID_COLUMN,
  type Assignment,
  type Delete,
  type logical,
  type Update,
} from "../plan/index.js";
import { bindCondition, conjunctsOf, refuse, type Clause } from "./clause.js";
import { columnNamed } from "./columns.js";
import { clauseOf } from "./expression.js";
import { relation, Scope, type BindContext } from "./scope.js";
import { writableTable, writtenTableReads } from "./tables.js";

// ---------------------------------------------------------------------------
// UPDATE and DELETE are planned like queries: their source reads the target
// table — every column in column order, then the rowid — under a filter
// with the WHERE clause, so the optimizer finds their rows as it finds a
// SELECT's. UPDATE says whether what it computes reads a table it writes
// (statement.ts, readsTarget).
// ---------------------------------------------------------------------------

type Plan = logical.Plan;

export function bindUpdate(
  statement: UpdateStatement,
  context: BindContext,
): Update<Plan> {
  const target = targetOf(statement.table, statement.where, context);
  const clause = clauseOf(target.scope, refuse("UPDATE"));
  // Only the assignments' reads count: the WHERE is evaluated before any
  // row is written.
  const readsBefore = writtenTableReads(target.table, context);
  const assignments = bindAssignments(
    statement.assignments,
    target.table,
    clause,
  );
  const isWrittenTableRead =
    writtenTableReads(target.table, context) > readsBefore;
  return {
    kind: "update",
    table: target.table,
    tableIndex: target.tableIndex,
    source: target.source,
    assignments,
    readsTarget: isWrittenTableRead,
    parameterCount: context.parameterCount,
  };
}

export function bindDelete(
  statement: DeleteStatement,
  context: BindContext,
): Delete<Plan> {
  const target = targetOf(statement.table, statement.where, context);
  return {
    kind: "delete",
    table: target.table,
    tableIndex: target.tableIndex,
    source: target.source,
    parameterCount: context.parameterCount,
  };
}

/**
 * `SET column = value, …`: each column of `table`, at most once, its value
 * bound in `clause`.
 */
export function bindAssignments(
  assignments: readonly AssignmentNode[],
  table: Table,
  clause: Clause,
): Assignment<Plan>[] {
  const assigned = new Set<number>();
  return assignments.map((assignment) => {
    const column = columnNamed(table, assignment.column);
    if (assigned.has(column)) {
      throw new SemanticError(
        `column ${assignment.column} is assigned more than once`,
      );
    }
    assigned.add(column);
    return { column, value: clause.bind(assignment.value) };
  });
}

// --- The target table ----------------------------------------------------------

interface Target {
  readonly table: Table;
  readonly tableIndex: number;
  readonly scope: Scope;
  readonly source: Plan;
}

/** The rows of `name` that `where` selects: every column, then the rowid. */
function targetOf(
  name: string,
  where: ParsedExpression | null,
  context: BindContext,
): Target {
  const table = writableTable(name, context);
  const tableIndex = context.newIndex();
  const scope = Scope.root(context);
  scope.add(relation(table.name, tableIndex, table.columns));
  const columns = [
    ...table.columns.map((_, position) => position),
    ROWID_COLUMN,
  ];
  const get: Plan = { kind: "get", tableIndex, table, columns };
  if (where === null) return { table, tableIndex, scope, source: get };
  const clause = clauseOf(scope, refuse("WHERE"));
  const condition = bindCondition(where, clause, "WHERE");
  const source: Plan = {
    kind: "filter",
    child: get,
    conditions: conjunctsOf(condition),
  };
  return { table, tableIndex, scope, source };
}

import { SemanticError } from "../../errors.js";
import { commonType } from "../../functions/index.js";
import { isReservedName, type Table } from "../../schema/index.js";
import type {
  Assignment as AssignmentNode,
  DeleteStatement,
  InsertSource,
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
import { columnIn } from "./columns.js";
import { clauseOf } from "./expression.js";
import { relation, Scope, type BindContext, type BoundQuery } from "./scope.js";

// ---------------------------------------------------------------------------
// UPDATE and DELETE are planned like queries: their source reads the target
// table — every column in column order, then the rowid — under a filter
// with the WHERE clause, so the optimizer finds their rows as it finds a
// SELECT's. INSERT's source is VALUES or a query.
// ---------------------------------------------------------------------------

type Plan = logical.Plan;

export function bindUpdate(
  statement: UpdateStatement,
  context: BindContext,
): Update<Plan> {
  const target = targetOf(statement.table, statement.where, context);
  const clause = clauseOf(target.scope, refuse("UPDATE"));
  const assignments = bindAssignments(
    statement.assignments,
    target.table,
    clause,
  );
  return {
    kind: "update",
    table: target.table,
    tableIndex: target.tableIndex,
    source: target.source,
    assignments,
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

/** The table a statement names, which must exist. */
export function tableNamed(name: string, context: BindContext): Table {
  const table = context.schema.table(name);
  if (table === null) throw new SemanticError(`no such table: ${name}`);
  return table;
}

/**
 * The table INSERT, UPDATE or DELETE writes: not one of opfsql's own, which
 * a statement may read but never write (docs/semantics.md, Names).
 */
export function writableTable(name: string, context: BindContext): Table {
  const table = tableNamed(name, context);
  if (isReservedName(table.name)) {
    throw new SemanticError(`table ${table.name} may not be modified`);
  }
  return table;
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

/** The position of `table`'s column called `name`. */
export function columnNamed(table: Table, name: string): number {
  return columnIn(table.columns, name, table.name);
}

/** INSERT's rows: VALUES, each row as long as the column list, or a query. */
export function insertSource(
  source: InsertSource,
  width: number,
  context: BindContext,
): BoundQuery {
  if (source.type === "VALUES") {
    for (const row of source.rows) checkWidth(row.length, width);
    return bindValues(source.rows, width, context);
  }
  const query = context.bindQuery(source.query, Scope.root(context));
  checkWidth(query.columns.length, width);
  return query;
}

function checkWidth(count: number, width: number): void {
  if (count !== width) {
    throw new SemanticError(
      `${String(count)} values for ${String(width)} columns`,
    );
  }
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

// --- VALUES --------------------------------------------------------------------------

/** VALUES rows: constants and expressions without columns, typed per column. */
function bindValues(
  rows: readonly (readonly ParsedExpression[])[],
  width: number,
  context: BindContext,
): BoundQuery {
  const clause = clauseOf(Scope.root(context), refuse("VALUES"));
  const bound = rows.map((row) => row.map((value) => clause.bind(value)));
  const types = Array.from({ length: width }, (_, position) =>
    commonType(bound.map((row) => row[position]?.type ?? "NULL")),
  );
  const tableIndex = context.newIndex();
  const plan: Plan = { kind: "values", tableIndex, rows: bound, types };
  const columns = types.map((type, position) => ({
    name: `column${String(position + 1)}`,
    type,
  }));
  return { plan, columns };
}

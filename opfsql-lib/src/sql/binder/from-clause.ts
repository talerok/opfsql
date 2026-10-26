import { SemanticError } from "../../errors.js";
import {
  JoinType,
  TableReferenceType,
  type BaseTableReference,
  type JoinReference,
  type SubqueryReference,
  type TableReference,
} from "../parser/index.js";
import {
  logical,
  type ColumnReference,
  type OutputColumn,
} from "../plan/index.js";
import { bindCondition, conjunctsOf, refuse } from "./clause.js";
import { clauseOf } from "./expression.js";
import { columnOf, unqualifiedColumn } from "./names.js";
import { relation, type Relation, type Scope } from "./scope.js";
import { checkComparable } from "./type-rules.js";

// ---------------------------------------------------------------------------
// FROM: each item a relation of the query's scope, under its own table
// index. A table is a `get` of every column; a CTE a `cte-scan`; a subquery
// is its plan under a `project` that gives it an index of its own. Joins
// keep their ON conjuncts; a RIGHT join is a LEFT join with its sides
// swapped. A query without FROM reads one row of no columns.
// ---------------------------------------------------------------------------

type Plan = logical.Plan;

export function bindFrom(from: TableReference | null, scope: Scope): Plan {
  if (from !== null) return bindTableReference(from, scope);
  const tableIndex = scope.context.newIndex();
  return { kind: "values", tableIndex, rows: [[]], types: [] };
}

function bindTableReference(reference: TableReference, scope: Scope): Plan {
  switch (reference.type) {
    case TableReferenceType.BASE_TABLE: {
      return bindNamedTable(reference, scope);
    }
    case TableReferenceType.SUBQUERY: {
      return bindDerivedTable(reference, scope);
    }
    case TableReferenceType.JOIN: {
      return bindJoin(reference, scope);
    }
  }
}

/** A CTE the query sees, else a table of the schema. */
function bindNamedTable(reference: BaseTableReference, scope: Scope): Plan {
  const name = reference.alias ?? reference.table;
  const tableIndex = scope.context.newIndex();
  const cte = scope.cte(reference.table);
  if (cte !== null) {
    scope.add(relation(name, tableIndex, cte.columns));
    const types = cte.columns.map((column) => column.type);
    return { kind: "cte-scan", cteIndex: cte.cteIndex, tableIndex, types };
  }
  const table = scope.context.schema.table(reference.table);
  if (table === null) {
    throw new SemanticError(`no such table: ${reference.table}`);
  }
  scope.add(relation(name, tableIndex, table.columns));
  const columns = table.columns.map((_, position) => position);
  return { kind: "get", tableIndex, table, columns };
}

/** `(SELECT …) AS s`: bound beside this query, which it cannot see. */
function bindDerivedTable(reference: SubqueryReference, scope: Scope): Plan {
  const query = scope.context.bindQuery(reference.subquery, scope.sibling());
  const tableIndex = scope.context.newIndex();
  scope.add(relation(reference.alias, tableIndex, query.columns));
  const expressions = readColumns(query.plan, query.columns);
  return { kind: "project", tableIndex, child: query.plan, expressions };
}

/** References to every column of `plan`, in its layout order. */
export function readColumns(
  plan: Plan,
  columns: readonly OutputColumn[],
): ColumnReference[] {
  return logical.layoutOf(plan).map((binding, position) => ({
    kind: "column",
    binding,
    depth: 0,
    name: columns[position]?.name ?? String(position),
    type: columns[position]?.type ?? "ANY",
  }));
}

// --- Joins -------------------------------------------------------------------------

function bindJoin(reference: JoinReference, scope: Scope): Plan {
  const left = bindTableReference(reference.left, scope);
  const leftItems = scope.relations.length;
  const right = bindTableReference(reference.right, scope);
  const conditions = joinConditions(reference, scope, leftItems);
  switch (reference.joinType) {
    case JoinType.INNER:
    case JoinType.CROSS: {
      return { kind: "join", joinType: "inner", left, right, conditions };
    }
    case JoinType.LEFT: {
      return { kind: "join", joinType: "left", left, right, conditions };
    }
    case JoinType.RIGHT: {
      return {
        kind: "join",
        joinType: "left",
        left: right,
        right: left,
        conditions,
      };
    }
  }
}

/** The ON conjuncts, or the equalities USING stands for; none for a cross join. */
function joinConditions(
  reference: JoinReference,
  scope: Scope,
  leftItems: number,
): readonly logical.Expression[] {
  const { constraint } = reference;
  if (constraint === null) return [];
  if (constraint.type === "ON") {
    const clause = clauseOf(scope, refuse("JOIN ON"));
    return conjunctsOf(bindCondition(constraint.condition, clause, "ON"));
  }
  const isRight = reference.joinType === JoinType.RIGHT;
  return constraint.columns.map((name) =>
    usingEquality(name, scope, leftItems, isRight),
  );
}

/**
 * `USING (c)`: left.c = right.c. An unqualified c then means the column of
 * the side every row has — the left, or the right of a RIGHT join — and
 * `*` lists it once.
 */
function usingEquality(
  name: string,
  scope: Scope,
  leftItems: number,
  isRight: boolean,
): logical.Expression {
  const items = scope.relations;
  const left = usingColumn(name, items.slice(0, leftItems));
  const right = usingColumn(name, items.slice(leftItems));
  const hidden = isRight ? left : right;
  scope.hide(hidden.binding.tableIndex, hidden.binding.columnIndex);
  checkComparable(left.type, right.type);
  return {
    kind: "comparison",
    operator: "EQUAL",
    left,
    right,
    type: "BOOLEAN",
  };
}

function usingColumn(
  name: string,
  items: readonly Relation[],
): ColumnReference {
  const found = items.flatMap((item) => {
    const column = unqualifiedColumn(item, name);
    return column === null ? [] : [columnOf(item, column, 0)];
  });
  const [column, other] = found;
  if (column === undefined || other !== undefined) {
    throw new SemanticError(
      `cannot join using column ${name}: it must be in exactly one table on each side`,
    );
  }
  return column;
}

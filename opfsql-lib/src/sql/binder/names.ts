import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import type {
  ColumnReferenceExpression,
  JsonPathSegment,
  StarExpression,
} from "../parser/index.js";
import type {
  ColumnReference,
  JsonAccess,
  JsonPathStep,
  OutputColumn,
} from "../plan/index.js";
import type { Relation, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// Column names (docs/semantics.md, Names): compared by nameKey. `a.b` is
// column b of FROM item a when this query has an item a with that column,
// else column a and the JSON path .b. An unqualified name must be found in
// exactly one FROM item of the innermost query that has it; otherwise it
// is ambiguous. A name no query level has is an error.
// ---------------------------------------------------------------------------

/** A column found: where, and how many of the written names it took. */
interface Found {
  readonly relation: Relation;
  readonly column: number;
  /** 2 for `t.c`, 1 for `c`: the names after it are a JSON path. */
  readonly namesUsed: number;
}

/** A column reference as written, bound to the relation that has it. */
export function bindColumn(
  reference: ColumnReferenceExpression,
  scope: Scope,
): ColumnReference | JsonAccess {
  for (let level: Scope | null = scope; level !== null; level = level.outer) {
    const found = findIn(level, reference.columnNames);
    if (found === null) continue;
    const depth = scope.level - level.level;
    if (depth > 0) scope.noteOuterReference(level);
    return referenceTo(found, depth, reference);
  }
  throw new SemanticError(`no such column: ${reference.columnNames.join(".")}`);
}

/** The relations `*` or `t.*` stands for, and the columns of each it reads. */
export function starColumns(
  star: StarExpression,
  scope: Scope,
): ColumnReference[] {
  if (star.table !== null) {
    const item = relationNamed(scope, star.table);
    if (item === null) throw new SemanticError(`no such table: ${star.table}`);
    return item.columns.map((_, column) => columnOf(item, column, 0));
  }
  const items = scope.relations.filter((item) => !item.isQualifiedOnly);
  if (items.length === 0) throw new SemanticError("no tables specified");
  return items.flatMap((item) =>
    item.columns
      .map((_, column) => column)
      .filter((column) => !item.hidden.has(column))
      .map((column) => columnOf(item, column, 0)),
  );
}

/** True if an unqualified `name` finds a column in this query's own FROM. */
export function hasColumn(scope: Scope, name: string): boolean {
  return scope.relations.some(
    (item) => visibleColumns(item, name, false).length > 0,
  );
}

/** The FROM item of this query called `name`, if any. */
export function relationNamed(scope: Scope, name: string): Relation | null {
  const key = nameKey(name);
  const isNamed = (item: Relation): boolean =>
    item.name !== null && nameKey(item.name) === key;
  return scope.relations.find((item) => isNamed(item)) ?? null;
}

/** The column `name` of `item`, which an unqualified name can find; null if none. */
export function unqualifiedColumn(item: Relation, name: string): number | null {
  const [column = null, other] = visibleColumns(item, name, false);
  if (other !== undefined) throw ambiguous(name);
  return column;
}

/** A reference to column `column` of `item`, `depth` levels out. */
export function columnOf(
  item: Relation,
  column: number,
  depth: number,
): ColumnReference {
  const output = item.columns[column];
  return {
    kind: "column",
    binding: { tableIndex: item.tableIndex, columnIndex: column },
    depth,
    name: output?.name ?? String(column),
    type: output?.type ?? "ANY",
  };
}

// --- One query level -----------------------------------------------------------

function findIn(scope: Scope, names: readonly string[]): Found | null {
  const [first, second] = names;
  if (first === undefined) return null;
  const qualifier = second === undefined ? null : relationNamed(scope, first);
  if (qualifier !== null && second !== undefined) {
    const column = qualifiedColumn(qualifier, second);
    if (column !== null) {
      return { relation: qualifier, column, namesUsed: 2 };
    }
  }
  const found = unqualified(scope, first);
  if (found !== null) return found;
  if (qualifier !== null) {
    throw new SemanticError(`no such column: ${names.join(".")}`);
  }
  return null;
}

function unqualified(scope: Scope, name: string): Found | null {
  const matches = scope.relations.flatMap((item) =>
    visibleColumns(item, name, false).map((column) => ({
      relation: item,
      column,
      namesUsed: 1,
    })),
  );
  const [found = null, other] = matches;
  if (other !== undefined) throw ambiguous(name);
  return found;
}

function qualifiedColumn(item: Relation, name: string): number | null {
  const [column = null, other] = visibleColumns(item, name, true);
  if (other !== undefined) throw ambiguous(`${item.name ?? "?"}.${name}`);
  return column;
}

/** The columns of `item` named `name` that a name, qualified or not, can see. */
function visibleColumns(
  item: Relation,
  name: string,
  isQualified: boolean,
): number[] {
  if (!isQualified && item.isQualifiedOnly) return [];
  const key = nameKey(name);
  const isFound = (column: OutputColumn, position: number): boolean => {
    const isVisible = isQualified || !item.hidden.has(position);
    return isVisible && nameKey(column.name) === key;
  };
  return item.columns
    .map((column, position) => (isFound(column, position) ? position : -1))
    .filter((position) => position !== -1);
}

function ambiguous(name: string): SemanticError {
  return new SemanticError(`ambiguous column name: ${name}`);
}

// --- JSON paths ------------------------------------------------------------------

function referenceTo(
  found: Found,
  depth: number,
  reference: ColumnReferenceExpression,
): ColumnReference | JsonAccess {
  const column = columnOf(found.relation, found.column, depth);
  const keys = reference.columnNames.slice(found.namesUsed);
  const path: JsonPathStep[] = [
    ...keys.map((key) => ({ kind: "key" as const, key })),
    ...reference.path.map((segment) => pathStep(segment)),
  ];
  if (path.length === 0) return column;
  const isJson = column.type === "JSON" || column.type === "ANY";
  if (!isJson) {
    throw new SemanticError(
      `column ${column.name} is ${column.type}, not JSON: it has no path ${formatPath(path)}`,
    );
  }
  return { kind: "json-access", column, path, type: "JSON" };
}

function pathStep(segment: JsonPathSegment): JsonPathStep {
  return segment.type === "field"
    ? { kind: "key", key: segment.name }
    : { kind: "index", index: segment.value };
}

function formatPath(path: readonly JsonPathStep[]): string {
  return path
    .map((step) =>
      step.kind === "key" ? `.${step.key}` : `[${String(step.index)}]`,
    )
    .join("");
}

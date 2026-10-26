import type { SqlType, Value } from "../values/index.js";
import { areEqual, assignValue } from "../values/index.js";
import type { Column, Index, Table } from "./model.js";
import { nameKey } from "./names.js";

/** The tables and indexes of one snapshot, found by name. */
export class Schema {
  static readonly empty = new Schema(0, [], []);

  private readonly tablesByName: ReadonlyMap<string, Table>;
  private readonly indexesByName: ReadonlyMap<string, Index>;

  /** `version` is the schema version of the snapshot it was read from. */
  constructor(
    readonly version: number,
    readonly tables: readonly Table[],
    readonly indexes: readonly Index[],
  ) {
    this.tablesByName = byName(tables);
    this.indexesByName = byName(indexes);
  }

  table(name: string): Table | null {
    return this.tablesByName.get(nameKey(name)) ?? null;
  }

  index(name: string): Index | null {
    return this.indexesByName.get(nameKey(name)) ?? null;
  }

  /** The indexes of table `name`, in the order they were created. */
  indexesOf(name: string): Index[] {
    const key = nameKey(name);
    return this.indexes.filter((index) => nameKey(index.table) === key);
  }

  /**
   * What is inconsistent, for the integrity check: names used twice, indexes
   * of tables that do not exist, keys and rowid columns past the columns.
   */
  problems(): string[] {
    return [
      ...duplicateNames([...this.tables, ...this.indexes]),
      ...this.tables.flatMap((table) => tableProblems(table)),
      ...this.indexes.flatMap((index) => this.indexProblems(index)),
    ];
  }

  private indexProblems(index: Index): string[] {
    const table = this.table(index.table);
    if (table === null) {
      return [`index ${index.name} is on a missing table ${index.table}`];
    }
    return index.keys
      .filter((key) => key.kind === "column")
      .filter((key) => key.column >= table.columns.length)
      .map(
        (key) =>
          `index ${index.name} keys missing column ${String(key.column)}`,
      );
  }
}

function byName<T extends { readonly name: string }>(
  items: readonly T[],
): ReadonlyMap<string, T> {
  return new Map(items.map((item) => [nameKey(item.name), item]));
}

/** Tables and indexes share one namespace, as in SQLite. */
function duplicateNames(items: readonly { readonly name: string }[]): string[] {
  const seen = new Set<string>();
  const problems: string[] = [];
  for (const { name } of items) {
    const key = nameKey(name);
    if (seen.has(key)) problems.push(`the name ${name} is used twice`);
    seen.add(key);
  }
  return problems;
}

function tableProblems(table: Table): string[] {
  const named = (problem: string): string => `table ${table.name}: ${problem}`;
  return [
    ...duplicateNames(table.columns),
    ...rowidProblems(table),
    ...table.columns.flatMap((column) => defaultProblems(column)),
  ].map((problem) => named(problem));
}

function rowidProblems(table: Table): string[] {
  const { rowidColumn, isAutoincrement } = table;
  if (rowidColumn === null) {
    return isAutoincrement ? ["AUTOINCREMENT without a rowid column"] : [];
  }
  const isInteger = table.columns[rowidColumn]?.type === "INTEGER";
  return isInteger ? [] : [`no INTEGER rowid column ${String(rowidColumn)}`];
}

/** A default is stored as its column's type: rows written before the column read it as it is. */
function defaultProblems(column: Column): string[] {
  const { defaultValue, type, name } = column;
  if (isOfType(defaultValue, type)) return [];
  return [`the default of ${name} is not a ${type}`];
}

function isOfType(value: Value, type: SqlType): boolean {
  try {
    return areEqual(assignValue(value, type), value);
  } catch {
    return false;
  }
}

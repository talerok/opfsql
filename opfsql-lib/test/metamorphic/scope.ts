import { type ExpressionType, type Leaf, leaf } from "./expression.js";
import {
  type ColumnType,
  type DatabaseSpec,
  type IndexKey,
  JSON_KEYS,
  keySql,
  type TableSpec,
} from "./schema.js";

/** The static type a column's values have in expressions. */
export function expressionTypeOf(type: ColumnType): ExpressionType {
  switch (type) {
    case "INTEGER": {
      return "integer";
    }
    case "REAL": {
      return "real";
    }
    case "TEXT": {
      return "text";
    }
    case "BOOLEAN": {
      return "boolean";
    }
    case "ANY":
    case "JSON": {
      return "untyped";
    }
  }
}

/** The values a query reads from `table` as `alias`: its columns, and paths into its JSON. */
export function tableLeaves(table: TableSpec, alias: string): Leaf[] {
  return table.columns.flatMap((column) => {
    const sql = `${alias}.${column.name}`;
    const own = leaf(expressionTypeOf(column.type), sql);
    if (column.type !== "JSON") return [own];
    const paths = JSON_KEYS.map((key) => leaf("untyped", `${sql}.${key}`));
    return [own, ...paths];
  });
}

/**
 * What an access path of `table` can seek on: each index's keys — the first
 * one alone, the second after it — and the rowid and UNIQUE columns.
 */
export function seekKeys(
  database: DatabaseSpec,
  table: TableSpec,
  isFirstOnly: boolean,
): IndexKey[] {
  const indexKeys = database.indexes
    .filter((index) => index.table === table.name)
    .flatMap((index) => (isFirstOnly ? index.keys.slice(0, 1) : index.keys));
  const constrained = table.columns
    .filter((column) => column.constraint !== null)
    .map((column) => ({ column, form: "column" as const }));
  return [...indexKeys, ...constrained];
}

/** A key as the expression a query writes for it, its column read through `alias`. */
export function keyLeaf(key: IndexKey, alias: string): Leaf {
  return leaf(keyType(key), keySql(key, alias));
}

function keyType(key: IndexKey): ExpressionType {
  switch (key.form) {
    case "column": {
      return expressionTypeOf(key.column.type);
    }
    case "lower": {
      return "text";
    }
    case "abs": {
      return "integer";
    }
    case "path": {
      return "untyped";
    }
  }
}

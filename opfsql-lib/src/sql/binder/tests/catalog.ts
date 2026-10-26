// A schema changed by bound DDL, as the engine's catalog would change it,
// for tests that bind statement after statement: page numbers aside.

import {
  nameKey,
  Schema,
  type Index,
  type Table,
} from "../../../schema/index.js";
import type { logical, Statement } from "../../plan/index.js";

const NO_PAGE = 0;

/** The schema after `statement`; statements other than DDL leave it as it is. */
export function applyStatement(
  schema: Schema,
  statement: Statement<logical.Plan>,
): Schema {
  switch (statement.kind) {
    case "create-table": {
      if (schema.table(statement.table.name) !== null) return schema;
      const table = { ...statement.table, rootPage: NO_PAGE };
      const constraints = statement.constraints.map((index) => ({
        ...index,
        rootPage: NO_PAGE,
      }));
      return changed(
        schema,
        [...schema.tables, table],
        [...schema.indexes, ...constraints],
      );
    }
    case "create-index": {
      if (schema.index(statement.index.name) !== null) return schema;
      const index = { ...statement.index, rootPage: NO_PAGE };
      return changed(schema, schema.tables, [...schema.indexes, index]);
    }
    case "drop": {
      return drop(schema, statement.object, statement.name);
    }
    case "alter-table": {
      return alter(schema, statement);
    }
    case "query":
    case "insert":
    case "update":
    case "delete": {
      return schema;
    }
  }
}

function drop(schema: Schema, object: "table" | "index", name: string): Schema {
  const key = nameKey(name);
  if (object === "index") {
    const indexes = schema.indexes.filter(
      (index) => nameKey(index.name) !== key,
    );
    return changed(schema, schema.tables, indexes);
  }
  const tables = schema.tables.filter((table) => nameKey(table.name) !== key);
  const indexes = schema.indexes.filter(
    (index) => nameKey(index.table) !== key,
  );
  return changed(schema, tables, indexes);
}

function alter(
  schema: Schema,
  statement: Extract<Statement<logical.Plan>, { kind: "alter-table" }>,
): Schema {
  const { table, action } = statement;
  if (action.kind === "add-column") {
    const added = { ...table, columns: [...table.columns, action.column] };
    return replaced(schema, added, schema.indexes);
  }
  const after = (position: number): number =>
    position < action.column ? position : position - 1;
  const { rowidColumn } = table;
  const narrowed: Table = {
    ...table,
    columns: table.columns.toSpliced(action.column, 1),
    rowidColumn: rowidColumn === null ? null : after(rowidColumn),
  };
  const indexes = schema.indexes.map((index): Index => {
    if (nameKey(index.table) !== nameKey(table.name)) return index;
    const keys = index.keys.map((key) =>
      key.kind === "column" ? { ...key, column: after(key.column) } : key,
    );
    return { ...index, keys };
  });
  return replaced(schema, narrowed, indexes);
}

function replaced(
  schema: Schema,
  table: Table,
  indexes: readonly Index[],
): Schema {
  const key = nameKey(table.name);
  const tables = schema.tables.map((other) =>
    nameKey(other.name) === key ? table : other,
  );
  return changed(schema, tables, indexes);
}

function changed(
  schema: Schema,
  tables: readonly Table[],
  indexes: readonly Index[],
): Schema {
  return new Schema(schema.version + 1, tables, indexes);
}

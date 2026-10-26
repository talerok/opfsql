import { InternalError } from "../errors.js";
import type { Schema, Table } from "../schema/index.js";
import type {
  AlterTable,
  CreateIndex,
  CreateTable,
  Drop,
} from "../sql/plan/index.js";
import type { Catalog } from "../table/index.js";

// DDL through the table layer's catalog. The binder has checked what the
// statement may do against `schema`, the schema it was bound against; what
// is left here is IF [NOT] EXISTS finding nothing to do.

export type Ddl = CreateTable | CreateIndex | Drop | AlterTable;

export function runDdl(statement: Ddl, catalog: Catalog, schema: Schema): void {
  switch (statement.kind) {
    case "create-table": {
      const isThere = schema.table(statement.table.name) !== null;
      if (isThere && statement.isIfNotExists) return;
      catalog.createTable(statement.table, statement.constraints);
      return;
    }
    case "create-index": {
      const isThere = schema.index(statement.index.name) !== null;
      if (isThere && statement.isIfNotExists) return;
      catalog.createIndex(
        statement.index,
        tableNamed(schema, statement.index.table),
      );
      return;
    }
    case "drop": {
      drop(statement, catalog, schema);
      return;
    }
    case "alter-table": {
      alter(statement, catalog);
      return;
    }
  }
}

/** Drops the table or index; nothing if DROP … IF EXISTS found none. */
function drop(statement: Drop, catalog: Catalog, schema: Schema): void {
  if (statement.object === "table") {
    const table = schema.table(statement.name);
    if (table !== null) catalog.dropTable(table);
    return;
  }
  const index = schema.index(statement.name);
  if (index !== null) catalog.dropIndex(index);
}

function alter(statement: AlterTable, catalog: Catalog): void {
  const { table, action } = statement;
  switch (action.kind) {
    case "add-column": {
      catalog.addColumn(table, action.column);
      return;
    }
    case "drop-column": {
      catalog.dropColumn(table, action.column);
      return;
    }
  }
}

function tableNamed(schema: Schema, name: string): Table {
  const table = schema.table(name);
  if (table === null) throw new InternalError(`no table ${name} to index`);
  return table;
}

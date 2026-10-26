import { SemanticError } from "../../errors.js";
import { isReservedName, type Schema } from "../../schema/index.js";
import { DropType, type DropStatement } from "../parser/index.js";
import type { Drop } from "../plan/index.js";

// ---------------------------------------------------------------------------
// DROP TABLE and DROP INDEX. What is dropped must exist, unless IF EXISTS.
// opfsql's own tables are never dropped, and a constraint's index goes only
// with its table.
// ---------------------------------------------------------------------------

export function bindDrop(statement: DropStatement, schema: Schema): Drop {
  const { name, ifExists } = statement;
  const object = statement.dropType === DropType.TABLE ? "table" : "index";
  const found = droppedName(object, name, schema);
  if (found === null && !ifExists) {
    throw new SemanticError(`no such ${object}: ${name}`);
  }
  return { kind: "drop", object, name: found ?? name, isIfExists: ifExists };
}

/** The name of the table or index to drop, as the schema has it; null if there is none. */
function droppedName(
  object: "table" | "index",
  name: string,
  schema: Schema,
): string | null {
  if (object === "table") return droppedTable(name, schema);
  return droppedIndex(name, schema);
}

/** The name of the table to drop, as the schema has it; null if there is none. */
function droppedTable(name: string, schema: Schema): string | null {
  const table = schema.table(name);
  if (table === null) return null;
  if (isReservedName(table.name)) {
    throw new SemanticError(`table ${table.name} may not be dropped`);
  }
  return table.name;
}

function droppedIndex(name: string, schema: Schema): string | null {
  const index = schema.index(name);
  if (index === null) return null;
  if (index.origin !== "statement") {
    throw new SemanticError(
      `index ${index.name} belongs to a PRIMARY KEY or UNIQUE constraint of ${index.tableName} and cannot be dropped`,
    );
  }
  return index.name;
}

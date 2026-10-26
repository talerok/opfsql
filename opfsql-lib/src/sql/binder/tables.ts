import { SemanticError } from "../../errors.js";
import {
  isReservedName,
  SEQUENCE_TABLE_NAME,
  type Table,
} from "../../schema/index.js";
import type { BindContext } from "./scope.js";

// ---------------------------------------------------------------------------
// The tables a statement names: found in the schema, which must have them;
// written only if they are not opfsql's own; and the reads of a table a
// write writes, which decide whether its source may stream (statement.ts,
// readsTarget).
// ---------------------------------------------------------------------------

/**
 * The reads, in what was bound so far, of a table that writing `table`
 * writes: `table` itself, and opfsql_sequence when it has AUTOINCREMENT,
 * whose counter a row written may raise.
 */
export function writtenTableReads(table: Table, context: BindContext): number {
  const own = context.readsOf(table.name);
  if (!table.isAutoincrement) return own;
  return own + context.readsOf(SEQUENCE_TABLE_NAME);
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

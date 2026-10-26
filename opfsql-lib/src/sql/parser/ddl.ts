// CREATE TABLE, CREATE INDEX, ALTER TABLE and DROP.

import type { Cursor } from "./cursor.js";
import { above, power } from "../precedence.js";
import { parseTypeName, startsTypeName } from "./type-name.js";
import {
  AlterType,
  DropType,
  StatementType,
  type AlterTableStatement,
  type ColumnConstraint,
  type ColumnDefinition,
  type CreateIndexStatement,
  type CreateTableStatement,
  type DropStatement,
  type ParsedExpression,
} from "./types.js";

export function parseCreate(
  cursor: Cursor,
): CreateTableStatement | CreateIndexStatement {
  cursor.next();
  const isIndex = cursor.isKeyword("UNIQUE") || cursor.isKeyword("INDEX");
  if (isIndex) return parseCreateIndex(cursor);
  if (cursor.isKeyword("TABLE")) return parseCreateTable(cursor);
  rejectOtherObjects(cursor, "CREATE");
  return cursor.fail("TABLE or INDEX");
}

// --- CREATE TABLE -------------------------------------------------------------------

/** An item between CREATE TABLE's parentheses. */
type TableElement =
  | { readonly type: "COLUMN"; readonly column: ColumnDefinition }
  | { readonly type: "PRIMARY KEY"; readonly columns: readonly string[] };

function parseCreateTable(cursor: Cursor): CreateTableStatement {
  cursor.expectKeyword("TABLE");
  const hasIfNotExists = parseIfNotExists(cursor);
  const table = cursor.objectName("a table name");
  cursor.rejectKeyword("AS", "CREATE TABLE … AS");
  cursor.expectSymbol("(");
  const elements = cursor.list(() => parseTableElement(cursor));
  cursor.expectSymbol(")");
  cursor.rejectWord("STRICT", "STRICT");
  cursor.rejectWord("WITHOUT", "WITHOUT ROWID");
  return {
    type: StatementType.CREATE_TABLE_STATEMENT,
    table,
    ifNotExists: hasIfNotExists,
    columns: elements.flatMap((element) =>
      element.type === "COLUMN" ? [element.column] : [],
    ),
    primaryKeys: elements.flatMap((element) =>
      element.type === "PRIMARY KEY" ? [element.columns] : [],
    ),
  };
}

/** A column, or a table-level `PRIMARY KEY (…)`. */
function parseTableElement(cursor: Cursor): TableElement {
  if (cursor.matchKeyword("PRIMARY")) {
    cursor.expectKeyword("KEY");
    const columns = cursor.nameList("a column name");
    rejectConflictClause(cursor);
    return { type: "PRIMARY KEY", columns };
  }
  cursor.rejectKeyword("CONSTRAINT", "CONSTRAINT");
  cursor.rejectKeyword("UNIQUE", "a table UNIQUE constraint");
  cursor.rejectKeyword("CHECK", "CHECK");
  cursor.rejectKeyword("FOREIGN", "FOREIGN KEY");
  return { type: "COLUMN", column: parseColumnDefinition(cursor) };
}

/** `name [type] [constraint …]` */
function parseColumnDefinition(cursor: Cursor): ColumnDefinition {
  const name = cursor.name("a column name");
  const type = startsTypeName(cursor) ? parseTypeName(cursor) : null;
  const constraints: ColumnConstraint[] = [];
  let constraint = parseColumnConstraint(cursor);
  while (constraint !== undefined) {
    constraints.push(constraint);
    constraint = parseColumnConstraint(cursor);
  }
  return { name, type, constraints };
}

/** One column constraint, or undefined when there is none here. */
function parseColumnConstraint(cursor: Cursor): ColumnConstraint | undefined {
  if (cursor.matchKeyword("PRIMARY")) return parsePrimaryKeyConstraint(cursor);
  if (cursor.matchKeyword("NOT")) {
    cursor.expectKeyword("NULL");
    rejectConflictClause(cursor);
    return { type: "NOT NULL" };
  }
  if (cursor.matchKeyword("NULL")) return { type: "NULL" };
  if (cursor.matchKeyword("UNIQUE")) {
    rejectConflictClause(cursor);
    return { type: "UNIQUE" };
  }
  if (cursor.matchKeyword("AUTOINCREMENT")) return { type: "AUTOINCREMENT" };
  if (cursor.matchKeyword("DEFAULT")) {
    return { type: "DEFAULT", value: parseDefault(cursor) };
  }
  rejectUnsupportedConstraint(cursor);
  return undefined;
}

/** After PRIMARY: `KEY`, without a sort order or a conflict clause. */
function parsePrimaryKeyConstraint(cursor: Cursor): ColumnConstraint {
  cursor.expectKeyword("KEY");
  cursor.rejectKeyword("ASC", "ASC or DESC on a PRIMARY KEY");
  cursor.rejectKeyword("DESC", "ASC or DESC on a PRIMARY KEY");
  rejectConflictClause(cursor);
  return { type: "PRIMARY KEY" };
}

function rejectConflictClause(cursor: Cursor): void {
  cursor.rejectKeyword("ON", "ON CONFLICT in a constraint");
}

function rejectUnsupportedConstraint(cursor: Cursor): void {
  cursor.rejectKeyword("CONSTRAINT", "CONSTRAINT");
  cursor.rejectKeyword("CHECK", "CHECK");
  cursor.rejectKeyword("COLLATE", "COLLATE");
  cursor.rejectKeyword("REFERENCES", "REFERENCES");
  cursor.rejectKeyword("GENERATED", "a generated column");
  cursor.rejectKeyword("AS", "a generated column");
}

/**
 * The value after DEFAULT: an expression without comparisons or logical
 * operators at its top, as in PostgreSQL, so that `DEFAULT 0 NOT NULL` keeps
 * its NOT NULL constraint. Anything else goes in parentheses.
 */
function parseDefault(cursor: Cursor): ParsedExpression {
  // A prefix NOT would take a following NOT NULL as its operand.
  if (cursor.isKeyword("NOT")) cursor.fail("a default value");
  return cursor.expression(above(power.relational));
}

// --- CREATE INDEX -------------------------------------------------------------------

function parseCreateIndex(cursor: Cursor): CreateIndexStatement {
  const isUnique = cursor.matchKeyword("UNIQUE");
  cursor.expectKeyword("INDEX");
  const hasIfNotExists = parseIfNotExists(cursor);
  const name = cursor.objectName("an index name");
  cursor.expectKeyword("ON");
  const table = cursor.objectName("a table name");
  cursor.expectSymbol("(");
  const expressions = cursor.list(() => parseIndexedExpression(cursor));
  cursor.expectSymbol(")");
  cursor.rejectKeyword("WHERE", "a partial index");
  return {
    type: StatementType.CREATE_INDEX_STATEMENT,
    name,
    table,
    expressions,
    isUnique,
    ifNotExists: hasIfNotExists,
  };
}

function parseIndexedExpression(cursor: Cursor): ParsedExpression {
  const expression = cursor.expression();
  cursor.rejectKeyword("ASC", "ASC or DESC in an index");
  cursor.rejectKeyword("DESC", "ASC or DESC in an index");
  return expression;
}

// --- ALTER TABLE and DROP ---------------------------------------------------------------

export function parseAlterTable(cursor: Cursor): AlterTableStatement {
  cursor.next();
  cursor.expectKeyword("TABLE");
  const table = cursor.objectName("a table name");
  const type = StatementType.ALTER_TABLE_STATEMENT;
  if (cursor.matchKeyword("ADD")) {
    skipColumnKeyword(cursor);
    const column = parseColumnDefinition(cursor);
    return { type, table, alterType: AlterType.ADD_COLUMN, column };
  }
  if (cursor.matchKeyword("DROP")) {
    skipColumnKeyword(cursor);
    const columnName = cursor.name("a column name");
    return { type, table, alterType: AlterType.DROP_COLUMN, columnName };
  }
  cursor.rejectWord("RENAME", "ALTER TABLE … RENAME");
  return cursor.fail("ADD or DROP");
}

/** COLUMN is optional; as in SQLite, `ADD column INT` adds a column INT. */
function skipColumnKeyword(cursor: Cursor): void {
  cursor.matchKeyword("COLUMN");
}

export function parseDrop(cursor: Cursor): DropStatement {
  cursor.next();
  const dropType = parseDropType(cursor);
  const hasIfExists = parseIfExists(cursor);
  const what = dropType === DropType.TABLE ? "a table name" : "an index name";
  const name = cursor.objectName(what);
  return {
    type: StatementType.DROP_STATEMENT,
    dropType,
    name,
    ifExists: hasIfExists,
  };
}

function parseDropType(cursor: Cursor): DropType {
  if (cursor.matchKeyword("TABLE")) return DropType.TABLE;
  if (cursor.matchKeyword("INDEX")) return DropType.INDEX;
  rejectOtherObjects(cursor, "DROP");
  return cursor.fail("TABLE or INDEX");
}

/** `IF NOT EXISTS`; IF alone is a name. */
function parseIfNotExists(cursor: Cursor): boolean {
  const isClause = cursor.isKeyword("IF") && cursor.isKeyword("NOT", 1);
  if (!isClause) return false;
  cursor.next();
  cursor.next();
  cursor.expectKeyword("EXISTS");
  return true;
}

/** `IF EXISTS`; IF alone is a name. */
function parseIfExists(cursor: Cursor): boolean {
  const isClause = cursor.isKeyword("IF") && cursor.isKeyword("EXISTS", 1);
  if (!isClause) return false;
  cursor.next();
  cursor.next();
  return true;
}

const OTHER_OBJECTS: ReadonlySet<string> = new Set([
  "TEMP",
  "TEMPORARY",
  "TRIGGER",
  "VIEW",
  "VIRTUAL",
]);

function rejectOtherObjects(cursor: Cursor, verb: "CREATE" | "DROP"): void {
  cursor.rejectWordIn(OTHER_OBJECTS, (word) => `${verb} ${word}`);
}

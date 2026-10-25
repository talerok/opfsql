// CREATE TABLE, CREATE INDEX, ALTER TABLE and DROP.

import { NotSupportedError, SemanticError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import { parseTableName } from "./from-clause.js";
import type { Keyword } from "./keywords.js";
import { power } from "./precedence.js";
import { parseTypeName, startsTypeName } from "./type-name.js";
import {
  AlterType,
  DropType,
  StatementType,
  type AlterTableStatement,
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
  const isUnique = cursor.matchKeyword("UNIQUE");
  if (isUnique || cursor.isKeyword("INDEX")) {
    return parseCreateIndex(cursor, isUnique);
  }
  if (cursor.isKeyword("TABLE")) return parseCreateTable(cursor);
  rejectOtherObjects(cursor, "CREATE");
  return cursor.fail("TABLE or INDEX");
}

// --- CREATE TABLE -------------------------------------------------------------------

function parseCreateTable(cursor: Cursor): CreateTableStatement {
  cursor.next();
  const isConditional = parseIfNotExists(cursor);
  const table = parseTableName(cursor);
  if (cursor.isKeyword("AS")) {
    throw new NotSupportedError("CREATE TABLE … AS");
  }
  cursor.expectSymbol("(");
  const columns: ColumnDefinition[] = [];
  let primaryKey: readonly string[] | null = null;
  do {
    if (!cursor.matchKeyword("PRIMARY")) {
      rejectTableConstraint(cursor);
      columns.push(parseColumnDefinition(cursor));
      continue;
    }
    if (primaryKey !== null) {
      throw new SemanticError(`table ${table} has more than one PRIMARY KEY`);
    }
    cursor.expectKeyword("KEY");
    primaryKey = cursor.nameList("a column name");
  } while (cursor.matchSymbol(","));
  cursor.expectSymbol(")");
  if (cursor.isWord("STRICT")) throw new NotSupportedError("STRICT");
  if (cursor.isWord("WITHOUT")) throw new NotSupportedError("WITHOUT ROWID");
  return {
    type: StatementType.CREATE_TABLE_STATEMENT,
    table,
    ifNotExists: isConditional,
    columns,
    primaryKey: primaryKey ?? [],
  };
}

const TABLE_CONSTRAINTS: ReadonlyMap<Keyword, string> = new Map<
  Keyword,
  string
>([
  ["CONSTRAINT", "CONSTRAINT"],
  ["UNIQUE", "a table UNIQUE constraint"],
  ["CHECK", "CHECK"],
  ["FOREIGN", "FOREIGN KEY"],
]);

const COLUMN_CONSTRAINTS: ReadonlyMap<Keyword, string> = new Map<
  Keyword,
  string
>([
  ["CONSTRAINT", "CONSTRAINT"],
  ["CHECK", "CHECK"],
  ["COLLATE", "COLLATE"],
  ["REFERENCES", "REFERENCES"],
  ["GENERATED", "a generated column"],
  ["AS", "a generated column"],
]);

function rejectTableConstraint(cursor: Cursor): void {
  rejectKeyword(cursor, TABLE_CONSTRAINTS);
}

function rejectKeyword(
  cursor: Cursor,
  unsupported: ReadonlyMap<Keyword, string>,
): void {
  const token = cursor.peek();
  if (token.kind !== "keyword") return;
  const feature = unsupported.get(token.value);
  if (feature !== undefined) throw new NotSupportedError(feature);
}

/** `name [type] [constraint …]` */
function parseColumnDefinition(cursor: Cursor): ColumnDefinition {
  const name = cursor.name("a column name");
  const type = startsTypeName(cursor) ? parseTypeName(cursor) : null;
  let definition: ColumnDefinition = {
    name,
    type,
    isPrimaryKey: false,
    isNotNull: false,
    isUnique: false,
    isAutoincrement: false,
    defaultValue: null,
  };
  for (
    let constraint = parseColumnConstraint(cursor);
    constraint !== undefined;
    constraint = parseColumnConstraint(cursor)
  ) {
    definition = { ...definition, ...constraint };
  }
  return definition;
}

type ColumnConstraint = Partial<
  Pick<
    ColumnDefinition,
    | "isPrimaryKey"
    | "isNotNull"
    | "isUnique"
    | "isAutoincrement"
    | "defaultValue"
  >
>;

/** One column constraint, or undefined when there is none here. */
function parseColumnConstraint(cursor: Cursor): ColumnConstraint | undefined {
  if (cursor.matchKeyword("PRIMARY")) {
    cursor.expectKeyword("KEY");
    return { isPrimaryKey: true };
  }
  if (cursor.matchKeyword("NOT")) {
    cursor.expectKeyword("NULL");
    return { isNotNull: true };
  }
  if (cursor.matchKeyword("NULL")) return {};
  if (cursor.matchKeyword("UNIQUE")) return { isUnique: true };
  if (cursor.matchKeyword("AUTOINCREMENT")) return { isAutoincrement: true };
  if (cursor.matchKeyword("DEFAULT"))
    return { defaultValue: parseDefault(cursor) };
  rejectKeyword(cursor, COLUMN_CONSTRAINTS);
  return undefined;
}

/**
 * The value after DEFAULT: an expression without comparisons or logical
 * operators at its top, as in PostgreSQL, so that `DEFAULT 0 NOT NULL` keeps
 * its NOT NULL constraint. Anything else goes in parentheses.
 */
function parseDefault(cursor: Cursor): ParsedExpression {
  return cursor.expression(power.bitwise);
}

// --- CREATE INDEX -------------------------------------------------------------------

function parseCreateIndex(
  cursor: Cursor,
  isUnique: boolean,
): CreateIndexStatement {
  cursor.expectKeyword("INDEX");
  const isConditional = parseIfNotExists(cursor);
  const indexName = parseTableName(cursor, "an index name");
  cursor.expectKeyword("ON");
  const tableName = parseTableName(cursor);
  cursor.expectSymbol("(");
  const expressions = cursor.list(() => parseIndexedExpression(cursor));
  cursor.expectSymbol(")");
  if (cursor.isKeyword("WHERE")) throw new NotSupportedError("a partial index");
  return {
    type: StatementType.CREATE_INDEX_STATEMENT,
    indexName,
    tableName,
    expressions,
    isUnique,
    ifNotExists: isConditional,
  };
}

function parseIndexedExpression(cursor: Cursor): ParsedExpression {
  const expression = cursor.expression();
  if (cursor.isKeyword("ASC") || cursor.isKeyword("DESC")) {
    throw new NotSupportedError("ASC or DESC in an index");
  }
  return expression;
}

// --- ALTER TABLE and DROP ---------------------------------------------------------------

export function parseAlterTable(cursor: Cursor): AlterTableStatement {
  cursor.next();
  cursor.expectKeyword("TABLE");
  const table = parseTableName(cursor);
  const statement = {
    type: StatementType.ALTER_TABLE_STATEMENT,
    table,
  } as const;
  if (cursor.matchKeyword("ADD")) {
    skipColumnKeyword(cursor);
    const columnDefinition = parseColumnDefinition(cursor);
    return {
      ...statement,
      alterType: AlterType.ADD_COLUMN,
      columnDefinition,
      columnName: null,
    };
  }
  if (cursor.matchKeyword("DROP")) {
    skipColumnKeyword(cursor);
    const columnName = cursor.name("a column name");
    return {
      ...statement,
      alterType: AlterType.DROP_COLUMN,
      columnDefinition: null,
      columnName,
    };
  }
  if (cursor.isWord("RENAME")) {
    throw new NotSupportedError("ALTER TABLE … RENAME");
  }
  return cursor.fail("ADD or DROP");
}

/** COLUMN is optional, and is a column's name when no name follows it. */
function skipColumnKeyword(cursor: Cursor): void {
  if (cursor.isKeyword("COLUMN") && cursor.isName(1)) cursor.next();
}

export function parseDrop(cursor: Cursor): DropStatement {
  cursor.next();
  let dropType: DropType;
  if (cursor.matchKeyword("TABLE")) {
    dropType = DropType.TABLE;
  } else if (cursor.matchKeyword("INDEX")) {
    dropType = DropType.INDEX;
  } else {
    rejectOtherObjects(cursor, "DROP");
    return cursor.fail("TABLE or INDEX");
  }
  const isConditional = cursor.isKeyword("IF") && cursor.isKeyword("EXISTS", 1);
  if (isConditional) {
    cursor.next();
    cursor.next();
  }
  const name = parseTableName(
    cursor,
    dropType === DropType.TABLE ? "a table name" : "an index name",
  );
  return {
    type: StatementType.DROP_STATEMENT,
    dropType,
    name,
    ifExists: isConditional,
  };
}

/** `IF NOT EXISTS`; IF alone is a name. */
function parseIfNotExists(cursor: Cursor): boolean {
  if (!cursor.isKeyword("IF") || !cursor.isKeyword("NOT", 1)) return false;
  cursor.next();
  cursor.next();
  cursor.expectKeyword("EXISTS");
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
  const token = cursor.peek();
  const word = token.value.toUpperCase();
  if (token.kind === "identifier" && OTHER_OBJECTS.has(word)) {
    throw new NotSupportedError(`${verb} ${word}`);
  }
}

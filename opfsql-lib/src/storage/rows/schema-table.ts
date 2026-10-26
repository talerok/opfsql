import { CorruptError, InternalError } from "../../errors.js";
import type { Value } from "../../values/index.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import { SCHEMA_ROOT_PAGE } from "../pager/database-file.js";
import type { StoredRow } from "./table-store.js";
import { TableReader, TableWriter } from "./table-store.js";

// ---------------------------------------------------------------------------
// The schema table, opfsql_schema (docs/file-format.md, System tables): one
// row per table and index, rooted at page 2. Storage keeps the rows; making a
// schema of them takes the parser, and happens above.
// ---------------------------------------------------------------------------

export const SCHEMA_TABLE_NAME = "opfsql_schema";

export type SchemaEntryType = "table" | "index";

export interface SchemaEntry {
  readonly type: SchemaEntryType;
  readonly name: string;
  /** The table an index belongs to; a table's own name for a table. */
  readonly tableName: string;
  readonly rootPage: number;
  /** The CREATE statement; null for an index made by a constraint. */
  readonly sql: string | null;
}

export interface StoredSchemaEntry extends SchemaEntry {
  readonly rowid: number;
}

/** Pages that can also record a schema change in the database header. */
export interface SchemaPages extends PageWriter {
  bumpSchemaVersion(): void;
}

// The columns, in order.
const TYPE = 0;
const NAME = 1;
const TABLE_NAME = 2;
const ROOT_PAGE = 3;
const SQL = 4;

export function readSchemaTable(pages: PageReader): StoredSchemaEntry[] {
  const rows = new TableReader(pages, SCHEMA_ROOT_PAGE).scan();
  return Array.from(rows, (row) => entryOf(row));
}

/** Changes the schema table; every change also bumps the schema version. */
export class SchemaTableWriter {
  private readonly table: TableWriter;

  constructor(private readonly pages: SchemaPages) {
    this.table = new TableWriter(pages, SCHEMA_ROOT_PAGE, SCHEMA_TABLE_NAME);
  }

  /** Adds an entry and returns its rowid. */
  add(entry: SchemaEntry): number {
    const rowid = (this.table.lastRowid() ?? 0) + 1;
    this.table.insert(rowid, valuesOf(entry));
    this.pages.bumpSchemaVersion();
    return rowid;
  }

  replace(rowid: number, entry: SchemaEntry): void {
    if (!this.table.replace(rowid, valuesOf(entry))) throw missing(rowid);
    this.pages.bumpSchemaVersion();
  }

  remove(rowid: number): void {
    if (!this.table.delete(rowid)) throw missing(rowid);
    this.pages.bumpSchemaVersion();
  }
}

function valuesOf(entry: SchemaEntry): Value[] {
  const { type, name, tableName, rootPage, sql } = entry;
  return [type, name, tableName, rootPage, sql];
}

function entryOf(row: StoredRow): StoredSchemaEntry {
  return {
    rowid: row.rowid,
    type: entryTypeOf(textAt(row, TYPE), row),
    name: textAt(row, NAME),
    tableName: textAt(row, TABLE_NAME),
    rootPage: pageAt(row, ROOT_PAGE),
    sql: row.field(SQL) === null ? null : textAt(row, SQL),
  };
}

function entryTypeOf(text: string, row: StoredRow): SchemaEntryType {
  if (text === "table" || text === "index") return text;
  throw malformed(row, TYPE);
}

function textAt(row: StoredRow, column: number): string {
  const value = row.field(column);
  if (typeof value !== "string") throw malformed(row, column);
  return value;
}

function pageAt(row: StoredRow, column: number): number {
  const value = row.field(column);
  const isPageNumber = typeof value === "number" && Number.isSafeInteger(value);
  if (!isPageNumber || value <= SCHEMA_ROOT_PAGE) throw malformed(row, column);
  return value;
}

function malformed(row: StoredRow, column: number): CorruptError {
  return new CorruptError(
    `${SCHEMA_TABLE_NAME} row ${String(row.rowid)} has an invalid value in column ${String(column)}`,
  );
}

function missing(rowid: number): InternalError {
  return new InternalError(`${SCHEMA_TABLE_NAME} has no row ${String(rowid)}`);
}

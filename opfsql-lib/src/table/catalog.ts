import { InternalError } from "../errors.js";
import type {
  Column,
  Index,
  IndexShape,
  Schema,
  Table,
  TableShape,
} from "../schema/index.js";
import { createIndexSql, createTableSql, nameKey } from "../schema/index.js";
import type {
  IndexReader,
  SchemaEntry,
  SchemaPages,
} from "../storage/index.js";
import {
  encodeKey,
  IndexWriter,
  readSchemaTable,
  SchemaTableWriter,
  TableWriter,
} from "../storage/index.js";
import type { Value } from "../values/index.js";
import type { CompileExpression } from "./keys.js";
import { entryOf, keyedIndex, uniqueViolation } from "./keys.js";
import { TableRows } from "./rows.js";
import { SEQUENCE_TABLE_NAME, Sequences } from "./sequences.js";

// ---------------------------------------------------------------------------
// Schema changes on storage: the trees of tables and indexes, their rows in
// opfsql_schema — statements generated from the model, never edited — and
// the AUTOINCREMENT counters. Whether a change makes sense (names free, a
// dropped column unindexed) is the binder's to check; the catalog does it.
// The schema changes once a statement's changes are read back.
// ---------------------------------------------------------------------------

export class Catalog {
  private readonly entries: SchemaTableWriter;

  /** `schema` is the schema as of the transaction's last statement. */
  constructor(
    private readonly pages: SchemaPages,
    private readonly schema: Schema,
    private readonly compile: CompileExpression,
  ) {
    this.entries = new SchemaTableWriter(pages);
  }

  /** Creates a table and the indexes of its constraints. */
  createTable(
    shape: TableShape,
    constraints: readonly IndexShape[],
  ): { table: Table; indexes: Index[] } {
    const table: Table = { ...shape, rootPage: TableWriter.create(this.pages) };
    this.entries.add(tableEntry(table, constraints));
    // A constraint's index has no statement of its own: the table's has it.
    const indexes = constraints.map((index) => this.addIndex(index, null));
    if (table.isAutoincrement) this.ensureSequences();
    return { table, indexes };
  }

  /** Creates an index and fills it from the table's rows. */
  createIndex(shape: IndexShape, table: Table): Index {
    const index = this.addIndex(shape, createIndexSql(shape, table));
    this.fill(index, table);
    return index;
  }

  /** Drops a table, its indexes and its AUTOINCREMENT counter. */
  dropTable(table: Table): void {
    for (const index of this.schema.indexesOf(table.name)) {
      this.dropIndex(index);
    }
    new TableWriter(this.pages, table.rootPage, table.name).drop();
    this.entries.remove(this.entryRowid(table.name));
    if (table.isAutoincrement) this.sequences()?.remove(table.name);
  }

  dropIndex(index: Index): void {
    new IndexWriter(this.pages, index.rootPage, index.name).drop();
    this.entries.remove(this.entryRowid(index.name));
  }

  /** Adds a column at the end; rows written before it read its default. */
  addColumn(table: Table, column: Column): Table {
    const changed = { ...table, columns: [...table.columns, column] };
    this.rewriteEntry(changed, this.constraintsOf(table));
    return changed;
  }

  /** Drops a column no index or constraint uses, from every row too. */
  dropColumn(table: Table, column: number): Table {
    const changed = withoutColumn(table, column);
    const constraints = this.constraintsOf(table).map((index) =>
      withKeysAfter(index, column),
    );
    this.rewriteEntry(changed, constraints);
    this.rewriteRows(table, column);
    return changed;
  }

  // --- Indexes -------------------------------------------------------------------

  private addIndex(shape: IndexShape, sql: string | null): Index {
    const index: Index = {
      ...shape,
      rootPage: IndexWriter.create(this.pages),
    };
    this.entries.add({
      type: "index",
      name: index.name,
      tableName: index.table,
      rootPage: index.rootPage,
      sql,
    });
    return index;
  }

  /** Adds every row's entry; a unique index refuses a key two rows share. */
  private fill(index: Index, table: Table): void {
    const keyed = keyedIndex(index, table, this.compile);
    const entries = new IndexWriter(this.pages, index.rootPage, index.name);
    const rows = new TableRows(this.pages, table).scan();
    for (const row of rows) {
      const values = row.values();
      if (index.isUnique && hasKey(entries, keyed.keyOf(values))) {
        throw uniqueViolation(table, index);
      }
      entries.insert(entryOf(keyed, values, row.rowid));
    }
  }

  /** The indexes of the table's constraints, as its statement writes them. */
  private constraintsOf(table: Table): IndexShape[] {
    return this.schema
      .indexesOf(table.name)
      .filter((index) => index.origin !== "statement");
  }

  // --- Rows ----------------------------------------------------------------------

  /**
   * Rewrites every row without field `column`. The rowids are read first,
   * so no row moves under the scan that finds it.
   */
  private rewriteRows(table: Table, column: number): void {
    const rows = new TableWriter(this.pages, table.rootPage, table.name);
    const rowids = [...rows.scan()].map((row) => row.rowid);
    for (const rowid of rowids) {
      const stored = rows.get(rowid);
      if (stored === null) continue;
      const fields = Array.from({ length: stored.length }, (_, field) =>
        stored.value(field),
      );
      rows.replace(rowid, fields.toSpliced(column, 1));
    }
  }

  // --- opfsql_schema and opfsql_sequence ------------------------------------------

  private rewriteEntry(table: Table, constraints: readonly IndexShape[]): void {
    const rowid = this.entryRowid(table.name);
    this.entries.replace(rowid, tableEntry(table, constraints));
  }

  /** The rowid of the schema table's row for `name`. */
  private entryRowid(name: string): number {
    const key = nameKey(name);
    const entry = readSchemaTable(this.pages).find(
      (candidate) => nameKey(candidate.name) === key,
    );
    if (entry === undefined) {
      throw new InternalError(`opfsql_schema has no row for ${name}`);
    }
    return entry.rowid;
  }

  private sequences(): Sequences | null {
    const table = this.schema.table(SEQUENCE_TABLE_NAME);
    return table === null ? null : new Sequences(this.pages, table.rootPage);
  }

  private ensureSequences(): void {
    if (this.schema.table(SEQUENCE_TABLE_NAME) === null) {
      Sequences.create(this.pages);
    }
  }
}

function tableEntry(
  table: Table,
  constraints: readonly IndexShape[],
): SchemaEntry {
  return {
    type: "table",
    name: table.name,
    tableName: table.name,
    rootPage: table.rootPage,
    sql: createTableSql(table, constraints),
  };
}

/** True if the index has an entry with `key`; a key with a NULL never matches. */
function hasKey(entries: IndexReader, key: readonly Value[]): boolean {
  if (key.includes(null)) return false;
  const matches = entries.keysWithPrefix(encodeKey(key));
  return matches.next().done !== true;
}

function withoutColumn(table: Table, dropped: number): Table {
  const columns = table.columns.toSpliced(dropped, 1);
  const { rowidColumn } = table;
  if (rowidColumn === null) return { ...table, columns };
  return {
    ...table,
    columns,
    rowidColumn: positionAfter(rowidColumn, dropped),
  };
}

/** The index with its column keys renumbered for column `dropped` gone. */
function withKeysAfter(index: IndexShape, dropped: number): IndexShape {
  const keys = index.keys.map((key) => {
    if (key.kind === "expression") return key;
    return { ...key, column: positionAfter(key.column, dropped) };
  });
  return { ...index, keys };
}

/** Where a column at `position` is once column `dropped` is gone. */
function positionAfter(position: number, dropped: number): number {
  return position < dropped ? position : position - 1;
}

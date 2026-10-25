import {
  ConstraintError,
  DatatypeMismatchError,
  IntegerOverflowError,
  InternalError,
} from "../errors.js";
import type { Column, Table } from "../schema/index.js";
import type { PageWriter } from "../storage/index.js";
import {
  encodeKey,
  IndexWriter,
  keyRowid,
  TableWriter,
} from "../storage/index.js";
import type { Value } from "../values/index.js";
import { assignValue, compareBytes } from "../values/index.js";
import type { KeyedIndex } from "./keys.js";
import { entryOf } from "./keys.js";
import { TableRows } from "./rows.js";
import type { Sequences } from "./sequences.js";

// ---------------------------------------------------------------------------
// The one write path for a table's rows: values converted to the column
// types, NOT NULL, rowids, unique keys, index entries and AUTOINCREMENT, all
// kept together. A change is decided before anything is written, so a row
// that breaks a constraint leaves the table and its indexes as they were.
// ---------------------------------------------------------------------------

/** A row that is in the way of a change, and the constraint it would break. */
export interface Conflict {
  readonly rowid: number;
  readonly error: ConstraintError;
}

interface IndexEditor {
  readonly keyed: KeyedIndex;
  readonly entries: IndexWriter;
}

export class TableEditor extends TableRows {
  private readonly rows: TableWriter;
  private readonly indexes: readonly IndexEditor[];

  /**
   * `indexes` are every index of the table; `sequences` is needed when the
   * table has an AUTOINCREMENT column.
   */
  constructor(
    pages: PageWriter,
    table: Table,
    indexes: readonly KeyedIndex[],
    private readonly sequences: Sequences | null,
  ) {
    const rows = new TableWriter(pages, table.rootPage, table.name);
    super(pages, table, rows);
    this.rows = rows;
    this.indexes = indexes.map((keyed) => ({
      keyed,
      entries: new IndexWriter(pages, keyed.index.rootPage, keyed.index.name),
    }));
  }

  /**
   * Adds a row of a value for every column, and returns its rowid. A NULL
   * rowid column gets the next rowid.
   */
  insert(values: readonly Value[]): number {
    const prepared = this.prepare(values);
    const rowid = this.rowidOf(prepared) ?? this.nextRowid();
    const row = this.withRowid(prepared, rowid);
    const conflict = this.conflictOf(row, rowid, null);
    if (conflict !== null) throw conflict.error;
    this.rows.insert(rowid, this.storedValues(row));
    for (const { keyed, entries } of this.indexes) {
      entries.insert(entryOf(keyed, row, rowid));
    }
    this.recordIssued(rowid);
    return rowid;
  }

  /** Replaces row `rowid` with `values`, and returns its rowid, which may change. */
  update(rowid: number, values: readonly Value[]): number {
    const old = this.require(rowid);
    const row = this.prepare(values);
    const newRowid = this.rowidToUpdate(row, rowid);
    const conflict = this.conflictOf(row, newRowid, rowid);
    if (conflict !== null) throw conflict.error;
    this.moveRow(rowid, newRowid, this.storedValues(row));
    for (const index of this.indexes) {
      const from = entryOf(index.keyed, old, rowid);
      moveEntry(index, from, entryOf(index.keyed, row, newRowid));
    }
    this.recordIssued(newRowid);
    return newRowid;
  }

  /** Removes row `rowid`. False if there is none. */
  delete(rowid: number): boolean {
    const old = this.get(rowid)?.values();
    if (old === undefined) return false;
    this.rows.delete(rowid);
    for (const { keyed, entries } of this.indexes) {
      entries.delete(entryOf(keyed, old, rowid));
    }
    return true;
  }

  /**
   * The row an insert of `values` would conflict with, or null: for ON
   * CONFLICT, which decides what to do instead of failing. `values` are
   * prepared as insert() prepares them, so they must be valid otherwise.
   */
  findConflict(values: readonly Value[]): Conflict | null {
    const prepared = this.prepare(values);
    const rowid = this.rowidOf(prepared) ?? this.nextRowid();
    return this.conflictOf(this.withRowid(prepared, rowid), rowid, null);
  }

  // --- Rows ----------------------------------------------------------------------

  /** `values` converted to the column types, NOT NULL checked but for the rowid column. */
  private prepare(values: readonly Value[]): Value[] {
    const { columns } = this.table;
    if (values.length !== columns.length) {
      throw new InternalError(
        `a row of ${this.table.name} needs ${String(columns.length)} values, not ${String(values.length)}`,
      );
    }
    return columns.map((column, index) =>
      this.assign(column, index, values[index] ?? null),
    );
  }

  private assign(column: Column, index: number, value: Value): Value {
    const assigned = assignColumn(this.table.name, column, value);
    // A NULL rowid column asks for the next rowid.
    const isNullable = !column.isNotNull || index === this.table.rowidColumn;
    if (!isNullable && assigned === null) {
      throw new ConstraintError("not-null", this.table.name, [column.name]);
    }
    return assigned;
  }

  /** The row as stored: the rowid column's value is the key, not in the record. */
  private storedValues(row: readonly Value[]): Value[] {
    const { rowidColumn } = this.table;
    return rowidColumn === null ? [...row] : row.with(rowidColumn, null);
  }

  private require(rowid: number): Value[] {
    const row = this.get(rowid);
    if (row === null) {
      throw new InternalError(`${this.table.name} has no row ${String(rowid)}`);
    }
    return row.values();
  }

  private moveRow(from: number, to: number, values: readonly Value[]): void {
    if (from === to) {
      this.rows.replace(from, values);
      return;
    }
    this.rows.delete(from);
    this.rows.insert(to, values);
  }

  // --- Rowids --------------------------------------------------------------------

  private rowidOf(row: readonly Value[]): number | null {
    const { rowidColumn } = this.table;
    if (rowidColumn === null) return null;
    const value = row[rowidColumn];
    return typeof value === "number" ? value : null;
  }

  /** The row with `rowid` in its rowid column, if it has one. */
  private withRowid(row: readonly Value[], rowid: number): Value[] {
    const { rowidColumn } = this.table;
    return rowidColumn === null ? [...row] : row.with(rowidColumn, rowid);
  }

  /** An update keeps the rowid, unless it sets the rowid column, which cannot be NULL. */
  private rowidToUpdate(row: readonly Value[], rowid: number): number {
    const { rowidColumn } = this.table;
    if (rowidColumn === null) return rowid;
    const newRowid = this.rowidOf(row);
    if (newRowid !== null) return newRowid;
    const column = this.table.columns[rowidColumn]?.name ?? "rowid";
    throw new ConstraintError("not-null", this.table.name, [column]);
  }

  /**
   * One more than the largest rowid in the table, as in SQLite, or than the
   * largest ever issued for an AUTOINCREMENT table.
   */
  private nextRowid(): number {
    const largest = Math.max(this.rows.lastRowid() ?? 0, this.lastIssued());
    if (largest >= Number.MAX_SAFE_INTEGER) throw new IntegerOverflowError();
    return largest + 1;
  }

  private lastIssued(): number {
    if (!this.table.isAutoincrement) return 0;
    return this.requireSequences().last(this.table.name);
  }

  private recordIssued(rowid: number): void {
    if (!this.table.isAutoincrement) return;
    this.requireSequences().record(this.table.name, rowid);
  }

  private requireSequences(): Sequences {
    if (this.sequences === null) {
      throw new InternalError(
        `${this.table.name} has AUTOINCREMENT but no sequence table was given`,
      );
    }
    return this.sequences;
  }

  // --- Conflicts -----------------------------------------------------------------

  /**
   * The first constraint `row` under `rowid` would break, ignoring the row
   * it replaces (`replacing`, null for a new row).
   */
  private conflictOf(
    row: readonly Value[],
    rowid: number,
    replacing: number | null,
  ): Conflict | null {
    if (rowid !== replacing && this.rows.has(rowid)) {
      return { rowid, error: this.rowidConflict() };
    }
    for (const index of this.indexes) {
      const holder = this.holderOf(index, row, replacing);
      if (holder !== null)
        return { rowid: holder, error: this.uniqueConflict(index) };
    }
    return null;
  }

  /** The row other than `replacing` that has `row`'s key in a unique index. */
  private holderOf(
    { keyed, entries }: IndexEditor,
    row: readonly Value[],
    replacing: number | null,
  ): number | null {
    if (!keyed.index.isUnique) return null;
    const key = keyed.keyOf(row);
    // NULLs are distinct: a key with one never conflicts, as in SQLite.
    if (key.includes(null)) return null;
    const entriesWithKey = entries.keysWithPrefix(encodeKey(key));
    for (const entry of entriesWithKey) {
      const holder = keyRowid(entry);
      if (holder !== replacing) return holder;
    }
    return null;
  }

  private rowidConflict(): ConstraintError {
    const { rowidColumn, columns, name } = this.table;
    const column = rowidColumn === null ? undefined : columns[rowidColumn];
    return new ConstraintError(
      "primary-key",
      name,
      column === undefined ? [] : [column.name],
    );
  }

  private uniqueConflict({ keyed }: IndexEditor): ConstraintError {
    const { index } = keyed;
    const kind = index.origin === "primary-key" ? "primary-key" : "unique";
    const columns = keyColumnNames(this.table, keyed);
    return new ConstraintError(kind, this.table.name, columns, index.name);
  }
}

/** Assignment to a column, naming the column when the value does not fit. */
function assignColumn(table: string, column: Column, value: Value): Value {
  try {
    return assignValue(value, column.type);
  } catch (error) {
    if (!(error instanceof DatatypeMismatchError)) throw error;
    throw new DatatypeMismatchError(
      `${table}.${column.name}: ${error.message}`,
    );
  }
}

/** Replaces an index entry if the row's key or rowid changed. */
function moveEntry(index: IndexEditor, from: Uint8Array, to: Uint8Array): void {
  if (compareBytes(from, to) === 0) return;
  index.entries.delete(from);
  index.entries.insert(to);
}

/** The columns an index covers; none if any key part is an expression. */
function keyColumnNames(table: Table, keyed: KeyedIndex): string[] {
  const names: string[] = [];
  for (const key of keyed.index.keys) {
    const column =
      key.kind === "column" ? table.columns[key.column] : undefined;
    if (column === undefined) return [];
    names.push(column.name);
  }
  return names;
}

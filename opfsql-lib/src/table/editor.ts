import {
  ConstraintError,
  DatatypeMismatchError,
  IndexKeyTooLargeError,
  IntegerOverflowError,
  InternalError,
} from "../errors.js";
import type { Column, Index, Table } from "../schema/index.js";
import type { PageWriter } from "../storage/index.js";
import { encodeKey, IndexWriter, TableWriter } from "../storage/index.js";
import type { Value } from "../values/index.js";
import { assignValue, compareBytes } from "../values/index.js";
import type { KeyedIndex } from "./keys.js";
import { rowidsWithKey, uniqueViolation } from "./keys.js";
import { TableRows } from "./rows.js";
import type { Sequences } from "./sequences.js";

// ---------------------------------------------------------------------------
// The one write path for a table's rows: values converted to the column
// types, NOT NULL, rowids, unique keys, index entries and AUTOINCREMENT, all
// kept together.
//
// A change is decided in full before anything is written: the row, its
// rowid, every index key (expressions evaluated, sizes checked) and every
// constraint. Then the row is written — which refuses a row too large
// before writing a byte — and then its index entries, which can no longer
// fail. So a refused change leaves the table and its indexes as they were.
// ---------------------------------------------------------------------------

/** The constraint a conflict is with: a unique index, or the rowid. */
export type ConflictConstraint = Index | "rowid";

/** A row that is in the way of a change, and the constraint it would break. */
export interface Conflict {
  readonly rowid: number;
  readonly constraint: ConflictConstraint;
  readonly error: ConstraintError;
}

interface IndexEditor {
  readonly keyed: KeyedIndex;
  readonly entries: IndexWriter;
}

/** A row's key in one index, and the entry that holds it. */
interface IndexedKey {
  readonly index: IndexEditor;
  readonly key: readonly Value[];
  readonly entry: Uint8Array;
}

/** A row as it will be written, with its key in every index, in the editor's order. */
interface RowState {
  readonly rowid: number;
  readonly row: readonly Value[];
  readonly indexed: readonly IndexedKey[];
}

export class TableEditor extends TableRows {
  // The reader's rows, typed for writing.
  declare protected readonly stored: TableWriter;
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
    super(pages, table, new TableWriter(pages, table.rootPage, table.name));
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
    const state = this.newRow(values);
    this.refuseConflict(state, null);
    this.stored.insert(state.rowid, this.storedValues(state.row));
    this.insertEntries(state);
    this.recordIssued(state.rowid);
    return state.rowid;
  }

  /** Replaces row `rowid` with `values`, and returns its rowid, which may change. */
  update(rowid: number, values: readonly Value[]): number {
    const old = this.stateOf(rowid, this.require(rowid));
    const row = this.prepare(values);
    const state = this.stateOf(this.rowidToUpdate(row, rowid), row);
    this.refuseConflict(state, rowid);
    this.moveRow(rowid, state);
    this.moveEntries(old, state);
    if (state.rowid !== rowid) this.recordIssued(state.rowid);
    return state.rowid;
  }

  /** Removes row `rowid`. False if there is none. */
  delete(rowid: number): boolean {
    const values = this.get(rowid)?.values();
    if (values === undefined) return false;
    const old = this.stateOf(rowid, values);
    this.stored.delete(rowid);
    for (const { index, entry } of old.indexed) index.entries.delete(entry);
    return true;
  }

  /**
   * The row an insert of `values` would conflict with, or null: for ON
   * CONFLICT, which decides what to do instead of failing. `target`, the
   * constraint ON CONFLICT names, is checked first, as SQLite does. `values`
   * must be valid otherwise: they are prepared as insert() prepares them.
   */
  findConflict(
    values: readonly Value[],
    target: ConflictConstraint | null = null,
  ): Conflict | null {
    return this.conflictOf(this.newRow(values), null, target);
  }

  // --- Deciding a row ------------------------------------------------------------

  /** A new row: values prepared, the rowid chosen, every key computed. */
  private newRow(values: readonly Value[]): RowState {
    const row = this.prepare(values);
    return this.stateOf(this.rowidOf(row) ?? this.nextRowid(), row);
  }

  /** `row` under `rowid`, with its key and its checked entry in every index. */
  private stateOf(rowid: number, values: readonly Value[]): RowState {
    const row = this.withRowid(values, rowid);
    const indexed = this.indexes.map((index) => {
      const key = index.keyed.keyOf(row);
      return { index, key, entry: checkedEntry(index, key, rowid) };
    });
    return { rowid, row, indexed };
  }

  /** `values` converted to the column types, NOT NULL checked but for the rowid column. */
  private prepare(values: readonly Value[]): Value[] {
    const { columns } = this.table;
    if (values.length !== columns.length) {
      throw new InternalError(
        `a row of ${this.table.name} needs ${String(columns.length)} values, not ${String(values.length)}`,
      );
    }
    return columns.map((column, position) =>
      this.assignColumn(column, position, values[position] ?? null),
    );
  }

  private assignColumn(column: Column, position: number, value: Value): Value {
    const assigned = assignNamed(this.table.name, column, value);
    // A NULL rowid column asks for the next rowid.
    const isNullable = !column.isNotNull || position === this.table.rowidColumn;
    if (!isNullable && assigned === null) {
      throw new ConstraintError("not-null", this.table.name, [column.name]);
    }
    return assigned;
  }

  private require(rowid: number): Value[] {
    const row = this.get(rowid);
    if (row === null) {
      throw new InternalError(`${this.table.name} has no row ${String(rowid)}`);
    }
    return row.values();
  }

  // --- Writing a decided row -----------------------------------------------------

  /** The row as stored: the rowid column's value is the key, not in the record. */
  private storedValues(row: readonly Value[]): Value[] {
    const { rowidColumn } = this.table;
    return rowidColumn === null ? [...row] : row.with(rowidColumn, null);
  }

  /**
   * Writes the new row, moving it if its rowid changed: inserted under the
   * new rowid before the old is deleted, so a row too large is refused while
   * the old row still stands.
   */
  private moveRow(from: number, state: RowState): void {
    const values = this.storedValues(state.row);
    if (from === state.rowid) {
      this.stored.replace(from, values);
      return;
    }
    this.stored.insert(state.rowid, values);
    this.stored.delete(from);
  }

  private insertEntries(state: RowState): void {
    for (const { index, entry } of state.indexed) index.entries.insert(entry);
  }

  /** Replaces each index entry whose key or rowid changed. */
  private moveEntries(old: RowState, state: RowState): void {
    for (const [position, after] of state.indexed.entries()) {
      const from = indexedAt(old, position).entry;
      if (compareBytes(from, after.entry) === 0) continue;
      after.index.entries.delete(from);
      after.index.entries.insert(after.entry);
    }
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
    if (this.table.rowidColumn === null) return rowid;
    const newRowid = this.rowidOf(row);
    if (newRowid !== null) return newRowid;
    throw new ConstraintError("not-null", this.table.name, this.rowidColumns());
  }

  /**
   * One more than the largest rowid in the table, as in SQLite, or than the
   * largest ever issued for an AUTOINCREMENT table.
   */
  private nextRowid(): number {
    const largest = Math.max(this.stored.lastRowid() ?? 0, this.lastIssued());
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

  /** The rowid column's name, for messages; none when the rowid is hidden. */
  private rowidColumns(): string[] {
    const { rowidColumn, columns } = this.table;
    const column = rowidColumn === null ? undefined : columns[rowidColumn];
    return column === undefined ? [] : [column.name];
  }

  // --- Conflicts -----------------------------------------------------------------

  private refuseConflict(state: RowState, replacing: number | null): void {
    const conflict = this.conflictOf(state, replacing, null);
    if (conflict !== null) throw conflict.error;
  }

  /**
   * The first constraint the row would break, ignoring the row it replaces
   * (`replacing`, null for a new row): `first`, then the rowid, then each
   * unique index.
   */
  private conflictOf(
    state: RowState,
    replacing: number | null,
    first: ConflictConstraint | null,
  ): Conflict | null {
    for (const constraint of this.constraintsFrom(first)) {
      const holder = this.holderOf(constraint, state, replacing);
      if (holder !== null) return this.conflict(constraint, holder);
    }
    return null;
  }

  private constraintsFrom(
    first: ConflictConstraint | null,
  ): ConflictConstraint[] {
    const unique = this.indexes
      .map(({ keyed }) => keyed.index)
      .filter((index) => index.isUnique);
    const all: ConflictConstraint[] = ["rowid", ...unique];
    if (first === null) return all;
    return [first, ...all.filter((constraint) => constraint !== first)];
  }

  /** The row other than `replacing` that holds what the row needs under `constraint`. */
  private holderOf(
    constraint: ConflictConstraint,
    state: RowState,
    replacing: number | null,
  ): number | null {
    if (constraint === "rowid") return this.rowidHolder(state, replacing);
    const indexed = state.indexed.find(
      ({ index }) => index.keyed.index === constraint,
    );
    if (indexed === undefined) return null;
    for (const holder of rowidsWithKey(indexed.index.entries, indexed.key)) {
      if (holder !== replacing) return holder;
    }
    return null;
  }

  private rowidHolder(
    state: RowState,
    replacing: number | null,
  ): number | null {
    const isTaken = state.rowid !== replacing && this.stored.has(state.rowid);
    return isTaken ? state.rowid : null;
  }

  private conflict(constraint: ConflictConstraint, rowid: number): Conflict {
    return { rowid, constraint, error: this.violation(constraint) };
  }

  private violation(constraint: ConflictConstraint): ConstraintError {
    if (constraint !== "rowid") return uniqueViolation(this.table, constraint);
    return new ConstraintError(
      "primary-key",
      this.table.name,
      this.rowidColumns(),
    );
  }
}

/** Assignment to a column, naming the column when the value does not fit. */
function assignNamed(table: string, column: Column, value: Value): Value {
  try {
    return assignValue(value, column.type);
  } catch (error) {
    if (!(error instanceof DatatypeMismatchError)) throw error;
    throw new DatatypeMismatchError(
      `${table}.${column.name}: ${error.message}`,
    );
  }
}

/** A row's entry in an index, refused before anything is written if too long. */
function checkedEntry(
  { keyed, entries }: IndexEditor,
  key: readonly Value[],
  rowid: number,
): Uint8Array {
  const entry = encodeKey([...key, rowid]);
  if (entry.length > entries.maxKeySize) {
    const { name } = keyed.index;
    throw new IndexKeyTooLargeError(name, entry.length, entries.maxKeySize);
  }
  return entry;
}

/** Old and new states of a row list the same indexes in the same order. */
function indexedAt(state: RowState, position: number): IndexedKey {
  const indexed = state.indexed[position];
  if (indexed === undefined) {
    throw new InternalError(`no index key at ${String(position)}`);
  }
  return indexed;
}

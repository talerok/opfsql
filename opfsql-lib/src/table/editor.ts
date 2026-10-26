import {
  ConstraintError,
  CorruptError,
  DatatypeMismatchError,
  IndexKeyTooLargeError,
  IntegerOverflowError,
  InternalError,
} from "../errors.js";
import type { Column, Index, Table } from "../schema/index.js";
import { isSameName } from "../schema/index.js";
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
//
// Every write is one the editor decided the trees allow: a row or an entry
// it found absent is inserted, one it found present is replaced or deleted.
// A tree that refuses one disagrees with what was read from it, or an index
// with its table: CORRUPT, not a write skipped.
// ---------------------------------------------------------------------------

/** The constraint a conflict is with: a unique index, or the rowid. */
export type ConflictConstraint = Index | "rowid";

/**
 * True if two constraints are one: both the rowid, or indexes of one name,
 * whichever load of the schema each came from.
 */
export function isSameConstraint(
  constraint: ConflictConstraint,
  other: ConflictConstraint,
): boolean {
  if (constraint === "rowid" || other === "rowid") return constraint === other;
  return isSameName(constraint.name, other.name);
}

/** A row that is in the way of a change, and the constraint it would break. */
export interface Conflict {
  readonly rowid: number;
  readonly constraint: ConflictConstraint;
  readonly error: ConstraintError;
  /**
   * The row refused, as it would have been stored: converted to the column
   * types, its rowid filled in — ON CONFLICT DO UPDATE's `excluded` row.
   */
  readonly refused: readonly Value[];
}

/** An index of the table: its key, and the writer of its entries. */
interface IndexTarget {
  readonly keyed: KeyedIndex;
  readonly writer: IndexWriter;
}

/** A row's key in one index, and the entry that holds it. */
interface IndexedKey {
  readonly target: IndexTarget;
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
  private readonly indexes: readonly IndexTarget[];

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
      writer: new IndexWriter(pages, keyed.index.rootPage, keyed.index.name),
    }));
  }

  /**
   * Adds a row of a value for every column, and returns its rowid. A NULL
   * rowid column gets the next rowid.
   */
  insert(values: readonly Value[]): number {
    const state = this.newRow(values);
    this.refuseConflict(state, null);
    this.insertRow(state);
    this.insertEntries(state);
    this.recordIssued(state.rowid);
    return state.rowid;
  }

  /** Replaces row `rowid` with `values`, and returns its rowid, which may change. */
  update(rowid: number, values: readonly Value[]): number {
    return this.replace(rowid, this.rowValues(rowid), values);
  }

  /**
   * Replaces row `rowid` with what `change` makes of its values, every
   * column's, and returns its rowid, which may change. The row is read
   * once, for `change` and for its index entries.
   */
  updateWith(
    rowid: number,
    change: (old: readonly Value[]) => readonly Value[],
  ): number {
    const old = this.rowValues(rowid);
    return this.replace(rowid, old, change(old));
  }

  /** Removes row `rowid`, which the caller read. */
  delete(rowid: number): void {
    const old = this.stateOf(rowid, this.rowValues(rowid));
    this.deleteRow(rowid);
    for (const indexed of old.indexed) deleteEntry(indexed, rowid);
  }

  /**
   * The row an insert of `values` would conflict with, or null: for ON
   * CONFLICT, which decides what to do instead of failing. `target`, the
   * constraint ON CONFLICT names, is checked first, as SQLite does; it is
   * found by name, so it may come from any load of the schema. `values`
   * must be valid otherwise: they are prepared as insert() prepares them.
   */
  findConflict(
    values: readonly Value[],
    target: ConflictConstraint | null = null,
  ): Conflict | null {
    return this.conflictOf(this.newRow(values), null, target);
  }

  /** The values of row `rowid`, which the caller read: it must be there. */
  rowValues(rowid: number): Value[] {
    const row = this.get(rowid);
    if (row === null) {
      throw new InternalError(`${this.table.name} has no row ${String(rowid)}`);
    }
    return row.values();
  }

  // --- Replacing a row -----------------------------------------------------------

  /** Replaces row `rowid`, whose values were `oldValues`, with `values`. */
  private replace(
    rowid: number,
    oldValues: readonly Value[],
    values: readonly Value[],
  ): number {
    const old = this.stateOf(rowid, oldValues);
    const row = this.prepare(values);
    const state = this.stateOf(this.rowidToUpdate(row, rowid), row);
    this.refuseConflict(state, rowid);
    this.moveRow(rowid, state);
    this.moveEntries(old, state);
    if (state.rowid !== rowid) this.recordIssued(state.rowid);
    return state.rowid;
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
    const indexed = this.indexes.map((target) => {
      const key = target.keyed.keyOf(row);
      return { target, key, entry: checkedEntry(target, key, rowid) };
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
    if (from === state.rowid) {
      this.replaceRow(state);
      return;
    }
    this.insertRow(state);
    this.deleteRow(from);
  }

  /** Rewrites a row that was read, under its rowid. */
  private replaceRow(state: RowState): void {
    if (!this.stored.replace(state.rowid, this.storedValues(state.row))) {
      throw this.rowDisagreement(state.rowid, "is gone, though it was read");
    }
  }

  /** Stores a new row, under a rowid found free. */
  private insertRow(state: RowState): void {
    if (!this.stored.insert(state.rowid, this.storedValues(state.row))) {
      throw this.rowDisagreement(state.rowid, "is taken, though it was free");
    }
  }

  /** Deletes a row that was read. */
  private deleteRow(rowid: number): void {
    if (!this.stored.delete(rowid)) {
      throw this.rowDisagreement(rowid, "is gone, though it was read");
    }
  }

  private rowDisagreement(rowid: number, problem: string): CorruptError {
    return new CorruptError(
      `table ${this.table.name}: row ${String(rowid)} ${problem}`,
    );
  }

  private insertEntries(state: RowState): void {
    for (const indexed of state.indexed) insertEntry(indexed, state.rowid);
  }

  /** Replaces each index entry whose key or rowid changed. */
  private moveEntries(old: RowState, state: RowState): void {
    for (const [position, after] of state.indexed.entries()) {
      const before = indexedAt(old, position);
      if (compareBytes(before.entry, after.entry) === 0) continue;
      deleteEntry(before, old.rowid);
      insertEntry(after, state.rowid);
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
      if (holder !== null) return this.conflict(constraint, holder, state);
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
    // By name: ON CONFLICT's index may come from another load of the schema.
    const isFirst = (constraint: ConflictConstraint): boolean =>
      isSameConstraint(constraint, first);
    const named = all.filter((item) => isFirst(item));
    return [...named, ...all.filter((item) => !isFirst(item))];
  }

  /** The row other than `replacing` that holds what the row needs under `constraint`. */
  private holderOf(
    constraint: ConflictConstraint,
    state: RowState,
    replacing: number | null,
  ): number | null {
    if (constraint === "rowid") return this.rowidHolder(state, replacing);
    const indexed = state.indexed.find(
      ({ target }) => target.keyed.index === constraint,
    );
    if (indexed === undefined) return null;
    for (const holder of rowidsWithKey(indexed.target.writer, indexed.key)) {
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

  private conflict(
    constraint: ConflictConstraint,
    rowid: number,
    refused: RowState,
  ): Conflict {
    const error = this.violation(constraint);
    return { rowid, constraint, error, refused: refused.row };
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
  { keyed, writer }: IndexTarget,
  key: readonly Value[],
  rowid: number,
): Uint8Array {
  const entry = encodeKey([...key, rowid]);
  if (entry.length > writer.maxKeySize) {
    const { name } = keyed.index;
    throw new IndexKeyTooLargeError(name, entry.length, writer.maxKeySize);
  }
  return entry;
}

/**
 * Adds row `rowid`'s entry for a key the table's row did not have: an entry
 * already there means the index disagrees with its table.
 */
function insertEntry({ target, entry }: IndexedKey, rowid: number): void {
  if (target.writer.insert(entry)) return;
  throw indexDisagreement(
    target,
    `it already has the entry for row ${String(rowid)}`,
  );
}

/**
 * Removes row `rowid`'s entry for the key read from the table's row: no
 * entry means the index disagrees with its table.
 */
function deleteEntry({ target, entry }: IndexedKey, rowid: number): void {
  if (target.writer.delete(entry)) return;
  throw indexDisagreement(target, `it has no entry for row ${String(rowid)}`);
}

function indexDisagreement(target: IndexTarget, problem: string): CorruptError {
  const { name } = target.keyed.index;
  return new CorruptError(`index ${name} disagrees with its table: ${problem}`);
}

/** Old and new states of a row list the same indexes in the same order. */
function indexedAt(state: RowState, position: number): IndexedKey {
  const indexed = state.indexed[position];
  if (indexed === undefined) {
    throw new InternalError(`no index key at ${String(position)}`);
  }
  return indexed;
}

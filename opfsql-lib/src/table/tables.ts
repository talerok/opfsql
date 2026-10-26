import { InternalError } from "../errors.js";
import type { Index, Schema, Table } from "../schema/index.js";
import { nameKey } from "../schema/index.js";
import type { PageReader, PageWriter } from "../storage/index.js";
import { TableEditor } from "./editor.js";
import { IndexRows } from "./index-rows.js";
import { type CompileExpression, type KeyedIndex, keyedIndex } from "./keys.js";
import { TableRows } from "./rows.js";
import { SEQUENCE_TABLE_NAME, Sequences } from "./sequences.js";

// ---------------------------------------------------------------------------
// The tables of one transaction as a statement reads and writes them: a
// table's rows, a table through one of its indexes, and in a write
// transaction the editor that keeps a table's indexes and AUTOINCREMENT
// counter with its rows. Index keys are compiled once per schema version,
// in IndexKeys, which the engine keeps while that version is current.
// ---------------------------------------------------------------------------

/** How to compute rows' keys in one schema version's indexes, each compiled when first needed. */
export class IndexKeys {
  private readonly keyedByName = new Map<string, KeyedIndex>();

  constructor(
    readonly schema: Schema,
    private readonly compile: CompileExpression,
  ) {}

  /** Every index of `table`, with how to compute a row's key in it. */
  of(table: Table): KeyedIndex[] {
    return this.schema
      .indexesOf(table.name)
      .map((index) => this.keyed(index, table));
  }

  private keyed(index: Index, table: Table): KeyedIndex {
    const name = nameKey(index.name);
    const known = this.keyedByName.get(name);
    if (known !== undefined) return known;
    const keyed = keyedIndex(index, table, this.compile);
    this.keyedByName.set(name, keyed);
    return keyed;
  }
}

/** One transaction's tables, read at its snapshot. */
export class Tables {
  constructor(
    protected readonly pages: PageReader,
    readonly schema: Schema,
  ) {}

  rows(table: Table): TableRows {
    return new TableRows(this.pages, table);
  }

  /** A table's rows through one of its indexes. */
  index(index: Index): IndexRows {
    return new IndexRows(this.pages, index);
  }
}

/** One write transaction's tables: read, and changed through their editors. */
export class WritableTables extends Tables {
  // The reader's pages, typed for writing.
  declare protected readonly pages: PageWriter;

  constructor(
    pages: PageWriter,
    private readonly keys: IndexKeys,
  ) {
    super(pages, keys.schema);
  }

  /** The one write path for `table`'s rows, its indexes and its AUTOINCREMENT counter. */
  editor(table: Table): TableEditor {
    const indexes = this.keys.of(table);
    return new TableEditor(this.pages, table, indexes, this.sequencesOf(table));
  }

  /** opfsql_sequence, which a table with AUTOINCREMENT needs. */
  private sequencesOf(table: Table): Sequences | null {
    if (!table.isAutoincrement) return null;
    const sequences = this.schema.table(SEQUENCE_TABLE_NAME);
    if (sequences === null) {
      throw new InternalError(
        `${table.name} has AUTOINCREMENT, but there is no ${SEQUENCE_TABLE_NAME}`,
      );
    }
    return new Sequences(this.pages, sequences.rootPage);
  }
}

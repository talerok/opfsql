import { CorruptError } from "../errors.js";
import type { TableShape } from "../schema/index.js";
import {
  createTableSql,
  nameKey,
  SEQUENCE_TABLE_NAME,
} from "../schema/index.js";
import type {
  PageReader,
  PageWriter,
  SchemaPages,
  StoredRow,
} from "../storage/index.js";
import {
  SchemaTableWriter,
  TableReader,
  TableWriter,
} from "../storage/index.js";

// ---------------------------------------------------------------------------
// AUTOINCREMENT counters (docs/file-format.md, System tables): opfsql_sequence
// has one row per table, (name, seq), seq being the largest rowid the table
// ever issued, so a deleted row's rowid is never issued again.
// ---------------------------------------------------------------------------

const SEQUENCE_TABLE: TableShape = {
  name: SEQUENCE_TABLE_NAME,
  columns: [
    { name: "name", type: "TEXT", isNotNull: false, defaultValue: null },
    { name: "seq", type: "INTEGER", isNotNull: false, defaultValue: null },
  ],
  rowidColumn: null,
  isAutoincrement: false,
};

const NAME = 0;
const SEQ = 1;

interface SequenceRow {
  readonly rowid: number;
  readonly name: string;
  readonly seq: number;
}

export class Sequences {
  private readonly rows: TableWriter;

  constructor(pages: PageWriter, root: number) {
    this.rows = new TableWriter(pages, root, SEQUENCE_TABLE_NAME);
  }

  /** Creates the table and its schema entry, and returns its root page. */
  static create(pages: SchemaPages): number {
    const root = TableWriter.create(pages);
    new SchemaTableWriter(pages).add({
      type: "table",
      name: SEQUENCE_TABLE_NAME,
      tableName: SEQUENCE_TABLE_NAME,
      rootPage: root,
      sql: createTableSql(SEQUENCE_TABLE, []),
    });
    return root;
  }

  /** The largest rowid `table` has issued; 0 if none. */
  last(table: string): number {
    return issuedIn(this.rows, table);
  }

  /** Records that `table` issued rowid `issued`, if it is the largest so far. */
  record(table: string, issued: number): void {
    const found = findSequence(this.rows, table);
    if (found === undefined) {
      const sequenceRowid = (this.rows.lastRowid() ?? 0) + 1;
      this.rows.insert(sequenceRowid, [table, issued]);
      return;
    }
    if (issued > found.seq) this.rows.replace(found.rowid, [table, issued]);
  }

  /** Forgets `table`, when it is dropped. */
  remove(table: string): void {
    const found = findSequence(this.rows, table);
    if (found !== undefined) this.rows.delete(found.rowid);
  }
}

/** The largest rowid `table` has issued, read from opfsql_sequence at `root`. */
export function lastIssued(
  pages: PageReader,
  root: number,
  table: string,
): number {
  return issuedIn(new TableReader(pages, root), table);
}

function issuedIn(rows: TableReader, table: string): number {
  return findSequence(rows, table)?.seq ?? 0;
}

/** The table's row. A scan: there is one row per AUTOINCREMENT table. */
function findSequence(
  rows: TableReader,
  table: string,
): SequenceRow | undefined {
  const key = nameKey(table);
  for (const row of rows.scan()) {
    const sequence = sequenceRowOf(row);
    if (nameKey(sequence.name) === key) return sequence;
  }
  return undefined;
}

function sequenceRowOf(row: StoredRow): SequenceRow {
  const name = row.value(NAME);
  const seq = row.value(SEQ);
  if (typeof name !== "string" || typeof seq !== "number") {
    throw new CorruptError(
      `${SEQUENCE_TABLE_NAME} row ${String(row.rowid)} is malformed`,
    );
  }
  return { rowid: row.rowid, name, seq };
}

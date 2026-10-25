import { CorruptError } from "../errors.js";
import { nameKey } from "../schema/index.js";
import type { PageWriter, SchemaPages, StoredRow } from "../storage/index.js";
import { SchemaTableWriter, TableWriter } from "../storage/index.js";

// ---------------------------------------------------------------------------
// AUTOINCREMENT counters (docs/file-format.md, System tables): opfsql_sequence
// has one row per table, (name, seq), seq being the largest rowid the table
// ever issued, so a deleted row's rowid is never issued again.
// ---------------------------------------------------------------------------

export const SEQUENCE_TABLE_NAME = "opfsql_sequence";
const SEQUENCE_TABLE_SQL = `CREATE TABLE ${SEQUENCE_TABLE_NAME} (name TEXT, seq INTEGER)`;

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
      sql: SEQUENCE_TABLE_SQL,
    });
    return root;
  }

  /** The largest rowid `table` has issued; 0 if none. */
  last(table: string): number {
    return this.find(table)?.seq ?? 0;
  }

  /** Records that `table` issued `rowid`, if it is the largest so far. */
  record(table: string, rowid: number): void {
    const found = this.find(table);
    if (found === undefined) {
      const next = (this.rows.lastRowid() ?? 0) + 1;
      this.rows.insert(next, [table, rowid]);
      return;
    }
    if (rowid > found.seq) this.rows.replace(found.rowid, [table, rowid]);
  }

  /** Forgets `table`, when it is dropped. */
  remove(table: string): void {
    const found = this.find(table);
    if (found !== undefined) this.rows.delete(found.rowid);
  }

  /** The table's row. A scan: there is one row per AUTOINCREMENT table. */
  private find(table: string): SequenceRow | undefined {
    const key = nameKey(table);
    for (const row of this.rows.scan()) {
      const sequence = sequenceRowOf(row);
      if (nameKey(sequence.name) === key) return sequence;
    }
    return undefined;
  }
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

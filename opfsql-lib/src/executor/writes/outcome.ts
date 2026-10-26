/** What an INSERT, UPDATE or DELETE did. */
export interface WriteOutcome {
  /** Rows inserted, updated or deleted. */
  readonly changes: number;
  /** The rowid of the last row inserted, or null if none was. */
  readonly lastInsertRowid: number | null;
}

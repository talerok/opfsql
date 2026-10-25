import { SyncTableManager } from "../store/table-manager.js";
import type { Row, RowId } from "../store/types.js";

/** The table manager of one statement, noting the last rowid it inserted. */
export class RecordingTableManager extends SyncTableManager {
  private lastInserted: RowId | null = null;

  get lastInsertRowid(): RowId | null {
    return this.lastInserted;
  }

  override prepareInsert(tableId: string, row: Row): RowId {
    const rowId = super.prepareInsert(tableId, row);
    this.lastInserted = rowId;
    return rowId;
  }
}

import { buildIndexKey } from "../executor/dml/utils/index-maintenance.js";
import { SyncBTree } from "../store/index-btree/index-btree.js";
import { SyncTableManager } from "../store/table-manager.js";
import type { ICatalog, SyncIPageStore } from "../store/types.js";

/**
 * Checks that every row has its entry in every index of its table, using the
 * same key derivation as index maintenance. Together with the store check
 * (entry count == row count, stored keys unique) this makes each index an
 * exact mirror of its table.
 */
export function checkIndexConsistency(
  pages: SyncIPageStore,
  catalog: ICatalog,
): string[] {
  const problems: string[] = [];
  const rows = new SyncTableManager(pages, () => catalog);

  for (const table of catalog.getAllTables()) {
    for (const index of catalog.getTableIndexes(table.name)) {
      const tree = new SyncBTree(pages, index.rootPageNo!, index);
      for (const { rowId, row } of rows.scanTable(table.name)) {
        const key = buildIndexKey(row, table, index);
        if (!tree.lookup(key).includes(rowId)) {
          problems.push(
            `index "${index.name}" has no entry ${JSON.stringify(key)} for row ${rowId} of "${table.name}"`,
          );
        }
      }
    }
  }
  return problems;
}

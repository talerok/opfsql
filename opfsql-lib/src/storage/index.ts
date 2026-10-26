// The storage layer's public surface (docs/architecture.md, Modules): opening
// a database, transactions over its pages, and the rows, index entries and
// schema entries stored in them.

export type { PageReader, PageWriter } from "./btree/page-store.js";
export { checkStorage, type SnapshotPages } from "./check.js";
export { decodeKey, encodeKey, keyRowid, prefixEnd } from "./format/key.js";
export {
  openOpfsFiles,
  openPager,
  type StorageFiles,
  type StorageOptions,
} from "./open.js";
export { DEFAULT_PAGE_SIZE } from "./pager/database-file.js";
export type { Pager } from "./pager/pager.js";
export { ReadTransaction, WriteTransaction } from "./pager/transaction.js";
export { IndexReader, IndexWriter } from "./rows/index-store.js";
export {
  readSchemaTable,
  SCHEMA_TABLE_NAME,
  type SchemaEntry,
  type SchemaPages,
  SchemaTableWriter,
  type StoredSchemaEntry,
} from "./rows/schema-table.js";
export {
  ROWID_FIELD,
  type ScanOrder,
  StoredRow,
  TableReader,
  TableWriter,
  type TupleLayout,
  tupleLayout,
} from "./rows/table-store.js";

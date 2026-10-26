import { CorruptError } from "../errors.js";
import type { Schema } from "../schema/index.js";
import { checkStorage, type ReadTransaction } from "../storage/index.js";
import { checkTables } from "../table/index.js";
import { compileIndexKey } from "./index-keys.js";

// The integrity check (docs/file-format.md, Integrity): storage first —
// every B-tree well formed, every page used once or free — then what rows
// mean to their tables, which only means something on sound trees.

/**
 * The problems found in the snapshot `pages` reads; none if it is sound. A
 * schema that does not load, or a record that does not decode, is one.
 */
export function checkIntegrity(
  pages: ReadTransaction,
  loadSchema: (pages: ReadTransaction) => Schema,
): string[] {
  const problems = checkStorage(pages);
  if (problems.length > 0) return problems;
  try {
    return checkTables(pages, loadSchema(pages), compileIndexKey);
  } catch (error) {
    if (error instanceof CorruptError) return [error.message];
    throw error;
  }
}

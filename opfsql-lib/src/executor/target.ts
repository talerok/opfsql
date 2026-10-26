import { InternalError } from "../errors.js";
import type { Tuple } from "../expressions/index.js";
import type { Schema } from "../schema/index.js";
import type { PageReader, PageWriter } from "../storage/index.js";
import type { CompileExpression } from "../table/index.js";

/** Where a statement runs. */
export interface Target {
  /**
   * The transaction's pages: a read transaction's for a query, a write
   * transaction's for INSERT, UPDATE and DELETE.
   */
  readonly pages: PageReader;
  /**
   * The schema as of the statement: each table's indexes, which a write
   * keeps, and opfsql_sequence, which an AUTOINCREMENT table's counter is in.
   */
  readonly schema: Schema;
  /**
   * Compiles index key expressions from their SQL text. That takes the
   * parser and the binder, which only the engine has.
   */
  readonly compileExpression: CompileExpression;
}

/** A target whose pages can be written. */
export interface WriteTarget extends Target {
  readonly pages: PageWriter;
}

/** What a statement did. */
export interface Outcome {
  /** A query's rows, each in its output columns' order; none for a write. */
  readonly rows: readonly Tuple[];
  /** Rows inserted, updated or deleted. */
  readonly changes: number;
  /** The rowid of the last row inserted, or null if none was. */
  readonly lastInsertRowid: number | null;
}

/** `target`, for a statement that writes. */
export function writeTargetOf(target: Target): WriteTarget {
  const { pages } = target;
  if (!isWriter(pages)) {
    throw new InternalError(
      "INSERT, UPDATE and DELETE need a write transaction",
    );
  }
  return { ...target, pages };
}

function isWriter(pages: PageReader): pages is PageWriter {
  return "write" in pages && "allocate" in pages && "free" in pages;
}

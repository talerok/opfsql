import { InternalError } from "../errors.js";
import { loadSchema } from "../sql/binder/index.js";
import { type ReadTransaction, readSchemaTable } from "../storage/index.js";
import type { IndexKeys } from "../table/index.js";
import { indexKeysOf } from "./index-keys.js";

// ---------------------------------------------------------------------------
// Schemas by version. The header of every snapshot holds its schema version,
// which every schema change increments. Changes are made under the write
// lock on top of the latest commit, so a committed version stands for one
// schema in every tab; a transaction's uncommitted one may be rolled back
// and its number reused, so it stays private until it commits.
//
// A schema is kept as its IndexKeys — the schema, and its index keys
// compiled once for the table layer. Neither changes once built, so a
// prepared statement can tell by identity whether its plan still fits.
// ---------------------------------------------------------------------------

/** A loaded schema: the schema, with its index keys compiled once. */
export type LoadedSchema = IndexKeys;

/** Versions kept loaded: the current one, and some that snapshots still read. */
const KEPT_VERSIONS = 8;

export class SchemaCache {
  /** In order of last use, the most recent last. */
  private readonly byVersion = new Map<number, LoadedSchema>();

  /** The version used last: a hit on it leaves the order as it is. */
  private lastUsed: number | null = null;

  /**
   * The schema of the committed snapshot `pages` reads. A transaction with
   * changes of its own is refused: its version may be rolled back and its
   * number reused for another schema, which the cache would then serve.
   */
  load(pages: ReadTransaction): LoadedSchema {
    if (pages.hasChanges()) {
      throw new InternalError(
        "the schema cache takes committed snapshots only, and this transaction has changes of its own",
      );
    }
    const version = pages.schemaVersion;
    const known = this.byVersion.get(version);
    // Nearly every load is of the version used last, already in its place.
    if (known !== undefined && version === this.lastUsed) return known;
    const keys = known ?? readSchema(pages);
    this.remember(keys);
    return keys;
  }

  /** Keeps a schema a commit has just stored, so no one loads it again. */
  remember(keys: LoadedSchema): void {
    const { version } = keys.schema;
    // Deleting first moves it to the end: the map is in order of last use.
    this.byVersion.delete(version);
    this.byVersion.set(version, keys);
    this.lastUsed = version;
    for (const kept of this.byVersion.keys()) {
      if (this.byVersion.size <= KEPT_VERSIONS) return;
      this.byVersion.delete(kept);
    }
  }
}

/** The schema `pages` hold, read afresh: a write transaction's after its DDL. */
export function readSchema(pages: ReadTransaction): LoadedSchema {
  const schema = loadSchema(pages.schemaVersion, readSchemaTable(pages));
  return indexKeysOf(schema);
}

import type { CatalogChange } from "../executor/types.js";
import { Catalog } from "../store/catalog.js";
import type { SessionStore } from "../store/session-store.js";
import type { Storage } from "../store/storage.js";

/**
 * A schema as one snapshot sees it. Never changed once built: a transaction
 * that runs DDL works on a new catalog, so any catalog object stands for one
 * schema and a prepared statement can tell by identity whether its plan
 * still fits.
 */
export interface VersionedSchema {
  /**
   * The schema version of the commit the schema was read at: the number of
   * schema-changing commits before it. Commits that change the schema are
   * made under the write lock on top of the latest schema, so each takes the
   * next number and two different schemas never share one. A transaction's
   * uncommitted schema keeps the version it started from.
   */
  readonly version: number;
  readonly catalog: Catalog;
}

const KEPT_SCHEMAS = 8;

// ---------------------------------------------------------------------------
// The seam: the only code that knows where the schema is stored. Format 5
// moves it to the opfsql_schema table, its version to the header.
// ---------------------------------------------------------------------------

/**
 * Loads schemas from snapshots — `load` is the one way the engine reads a
 * schema — and keeps the recent ones decoded, by version.
 */
export class SchemaLoader {
  private readonly byVersion = new Map<number, VersionedSchema>();
  private last: { position: number; schema: VersionedSchema } | null = null;

  /**
   * The schema as of the snapshot `store` has pinned, at commit `position`.
   * Position 0 means the backend keeps no versions, so nothing is memoized
   * by position.
   */
  load(store: SessionStore, position: number): VersionedSchema {
    if (position > 0 && this.last?.position === position) {
      return this.last.schema;
    }
    const data = store.readCatalog();
    const version = data?.version ?? 0;
    const schema = this.byVersion.get(version) ?? {
      version,
      catalog: data === null ? new Catalog() : Catalog.deserialize(data),
    };
    this.remember(schema);
    this.last = { position, schema };
    return schema;
  }

  /** Keeps a schema a commit has just stored, so no one decodes it again. */
  remember(schema: VersionedSchema): void {
    this.byVersion.delete(schema.version);
    this.byVersion.set(schema.version, schema);
    for (const version of this.byVersion.keys()) {
      if (this.byVersion.size <= KEPT_SCHEMAS) break;
      this.byVersion.delete(version);
    }
  }
}

/** The version of the latest committed schema. */
export function latestSchemaVersion(storage: Storage): number {
  return storage.readCatalog()?.version ?? 0;
}

/** Stages `catalog` as the schema the commit `store` makes next stores. */
export function stageSchema(
  store: SessionStore,
  catalog: Catalog,
  version: number,
): void {
  store.writeCatalog({ ...catalog.serialize(), version });
}

// ---------------------------------------------------------------------------

/** A new catalog: `catalog` with the DDL changes of a statement applied. */
export function withChanges(
  catalog: Catalog,
  changes: readonly CatalogChange[],
): Catalog {
  const next = catalog.clone();
  for (const change of changes) apply(next, change);
  return next;
}

function apply(catalog: Catalog, change: CatalogChange): void {
  switch (change.type) {
    case "CREATE_TABLE": {
      catalog.addTable(change.schema);
      return;
    }
    case "DROP_TABLE": {
      catalog.removeTable(change.name);
      return;
    }
    case "ALTER_TABLE": {
      catalog.updateTable(change.after);
      return;
    }
    case "CREATE_INDEX": {
      catalog.addIndex(change.index);
      return;
    }
    case "DROP_INDEX": {
      catalog.removeIndex(change.name);
      return;
    }
  }
}

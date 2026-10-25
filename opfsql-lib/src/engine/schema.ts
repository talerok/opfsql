import type { CatalogChange } from "../executor/types.js";
import { Catalog } from "../store/catalog.js";
import type { SessionStore } from "../store/session-store.js";

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
// The seam: the only code that knows where the schema is stored. Format 4
// keeps the catalog, version included, in one page. Format 5 keeps the
// version in the database header and the schema as rows of opfsql_schema,
// and DDL writes those rows.
// ---------------------------------------------------------------------------

/**
 * The schema version of the snapshot `store` reads. Format 5 reads it from
 * the header without decoding anything; format 4 has to decode the catalog
 * page for it, which the WAL keeps decoded while the page is in it.
 */
export function readSchemaVersion(store: SessionStore): number {
  return store.readCatalog()?.version ?? 0;
}

/** The schema of the snapshot `store` reads. */
export function readSchema(store: SessionStore): Catalog {
  const data = store.readCatalog();
  return data === null ? new Catalog() : Catalog.deserialize(data);
}

/**
 * Stages a transaction's DDL, statement by statement, for its commit to
 * store as schema `version`.
 */
export function stageSchemaChanges(
  store: SessionStore,
  changes: readonly CatalogChange[],
  version: number,
): void {
  const catalog = withChanges(readSchema(store), changes);
  store.writeCatalog({ ...catalog.serialize(), version });
}

// ---------------------------------------------------------------------------

/**
 * Loads schemas from snapshots — `load` is the one way the engine reads a
 * schema — and keeps the recent ones decoded, by version, so a snapshot
 * whose version is known costs a version read.
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
    const last = this.last;
    const isSameSnapshot = position > 0 && last?.position === position;
    if (isSameSnapshot) return last.schema;
    const version = readSchemaVersion(store);
    const schema = this.byVersion.get(version) ?? {
      version,
      catalog: readSchema(store),
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

/** A new catalog: `catalog` with DDL changes applied in order. */
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

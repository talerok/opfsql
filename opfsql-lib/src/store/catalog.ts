import type { CatalogData, ICatalog, IndexDef, TableSchema, SyncIKVStore } from './types.js';

const CATALOG_KEY = 'meta:tables';

export class Catalog implements ICatalog {
  private tables = new Map<string, TableSchema>();
  private indexes = new Map<string, IndexDef>();

  private key(name: string): string { return name.toLowerCase(); }

  hasTable(name: string): boolean { return this.tables.has(this.key(name)); }
  getTable(name: string): TableSchema | undefined { return this.tables.get(this.key(name)); }
  addTable(schema: TableSchema): void { this.tables.set(this.key(schema.name), schema); }
  removeTable(name: string): void {
    const k = this.key(name);
    this.tables.delete(k);
    for (const [ik, idx] of this.indexes) {
      if (this.key(idx.tableName) === k) this.indexes.delete(ik);
    }
  }
  updateTable(schema: TableSchema): void { this.tables.set(this.key(schema.name), schema); }
  getAllTables(): TableSchema[] { return [...this.tables.values()]; }

  hasIndex(name: string): boolean { return this.indexes.has(this.key(name)); }
  getIndex(name: string): IndexDef | undefined { return this.indexes.get(this.key(name)); }
  getTableIndexes(tableName: string): IndexDef[] {
    const k = this.key(tableName);
    return [...this.indexes.values()].filter((idx) => this.key(idx.tableName) === k);
  }
  addIndex(index: IndexDef): void { this.indexes.set(this.key(index.name), index); }
  removeIndex(name: string): void { this.indexes.delete(this.key(name)); }

  serialize(): CatalogData {
    return { tables: [...this.tables.values()], indexes: [...this.indexes.values()] };
  }

  static deserialize(data: CatalogData): Catalog {
    const catalog = new Catalog();
    for (const table of data.tables) catalog.addTable(table);
    for (const index of data.indexes) catalog.addIndex(index);
    return catalog;
  }
}

export function initCatalog(kv: SyncIKVStore): Catalog {
  const data = kv.readKey<CatalogData>(CATALOG_KEY);
  if (!data) return new Catalog();
  return Catalog.deserialize(data);
}

export function serializeCatalogEntry(catalog: ICatalog): [string, CatalogData] {
  return [CATALOG_KEY, catalog.serialize()];
}

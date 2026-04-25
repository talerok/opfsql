import type {
  CatalogData,
  ICatalog,
  IndexDef,
  SyncIPageStore,
  TableSchema,
} from "./types.js";

const CATALOG_PAGE_NO = 1;

export class Catalog implements ICatalog {
  private _version = 0;
  private tables = new Map<string, TableSchema>();
  private indexes = new Map<string, IndexDef>();

  get version(): number {
    return this._version;
  }

  private key(name: string): string {
    return name.toLowerCase();
  }

  hasTable(name: string): boolean {
    return this.tables.has(this.key(name));
  }

  getTable(name: string): TableSchema | undefined {
    return this.tables.get(this.key(name));
  }

  addTable(schema: TableSchema): void {
    this.tables.set(this.key(schema.name), schema);
  }

  removeTable(name: string): void {
    const k = this.key(name);
    this.tables.delete(k);
    for (const [ik, idx] of this.indexes) {
      if (this.key(idx.tableName) === k) {
        this.indexes.delete(ik);
      }
    }
  }

  updateTable(schema: TableSchema): void {
    this.tables.set(this.key(schema.name), schema);
  }

  getAllTables(): TableSchema[] {
    return [...this.tables.values()];
  }

  hasIndex(name: string): boolean {
    return this.indexes.has(this.key(name));
  }

  getIndex(name: string): IndexDef | undefined {
    return this.indexes.get(this.key(name));
  }

  getTableIndexes(tableName: string): IndexDef[] {
    const k = this.key(tableName);
    return [...this.indexes.values()].filter(
      (idx) => this.key(idx.tableName) === k,
    );
  }

  addIndex(index: IndexDef): void {
    this.indexes.set(this.key(index.name), index);
  }

  removeIndex(name: string): void {
    this.indexes.delete(this.key(name));
  }

  writeTo(ps: SyncIPageStore): void {
    this._version++;
    const data = this.serialize();
    ps.writePage(CATALOG_PAGE_NO, data);
  }

  serialize(): CatalogData {
    return {
      version: this.version,
      tables: [...this.tables.values()],
      indexes: [...this.indexes.values()],
    };
  }

  snapshot(): CatalogData {
    const data = this.serialize();
    return structuredClone(data);
  }

  clone(): Catalog {
    return Catalog.deserialize(this.serialize());
  }

  static deserialize(data: CatalogData): Catalog {
    const catalog = new Catalog();
    for (const table of data.tables) catalog.addTable(table);
    for (const index of data.indexes) catalog.addIndex(index);
    catalog._version = data.version ?? 0;
    return catalog;
  }

  static fromStorage(ps: SyncIPageStore): Catalog {
    const data = ps.readPage<CatalogData>(CATALOG_PAGE_NO);
    if (!data) return new Catalog();
    return Catalog.deserialize(data);
  }
}


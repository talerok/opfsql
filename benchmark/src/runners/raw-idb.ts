import type { BenchmarkRunner, Row } from '../types.js';

const DB_NAME = 'bench-raw-idb';
const STORE = 'products';

export function createRawIdbRunner(): BenchmarkRunner {
  let db: IDBDatabase;

  function tx(
    mode: IDBTransactionMode,
    fn: (store: IDBObjectStore) => IDBRequest | void,
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      const t = db.transaction(STORE, mode);
      const store = t.objectStore(STORE);
      const req = fn(store);
      if (req) {
        req.onsuccess = () => resolve(req.result);
      }
      t.oncomplete = () => {
        if (!req) resolve(undefined);
      };
      t.onerror = () => reject(t.error);
    });
  }

  function cursorAll(
    source: IDBObjectStore | IDBIndex,
    range?: IDBKeyRange | null,
  ): Promise<Row[]> {
    return new Promise((resolve, reject) => {
      const rows: Row[] = [];
      const req = source.openCursor(range);
      req.onsuccess = () => {
        const cursor = req.result;
        if (cursor) {
          rows.push(cursor.value);
          cursor.continue();
        } else {
          resolve(rows);
        }
      };
      req.onerror = () => reject(req.error);
    });
  }

  return {
    name: 'Raw IndexedDB',
    storage: 'IndexedDB',

    async setup() {
      // Delete previous
      await new Promise<void>((resolve) => {
        const req = indexedDB.deleteDatabase(DB_NAME);
        req.onsuccess = () => resolve();
        req.onerror = () => resolve();
        req.onblocked = () => resolve();
      });

      db = await new Promise<IDBDatabase>((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, 1);
        req.onupgradeneeded = () => {
          const store = req.result.createObjectStore(STORE, { keyPath: 'id' });
          store.createIndex('price', 'price');
          store.createIndex('category', 'category');
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      });
    },

    async teardown() {
      db.close();
      await new Promise<void>((resolve) => {
        const req = indexedDB.deleteDatabase(DB_NAME);
        req.onsuccess = () => resolve();
        req.onerror = () => resolve();
        req.onblocked = () => resolve();
      });
    },

    async insertBatch(rows: Row[]) {
      return new Promise<void>((resolve, reject) => {
        const t = db.transaction(STORE, 'readwrite');
        const store = t.objectStore(STORE);
        for (const row of rows) {
          store.add(row);
        }
        t.oncomplete = () => resolve();
        t.onerror = () => reject(t.error);
      });
    },

    async selectAll() {
      return new Promise((resolve, reject) => {
        const t = db.transaction(STORE, 'readonly');
        const req = t.objectStore(STORE).getAll();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      });
    },

    async selectPoint(id: number) {
      return tx('readonly', (store) => store.get(id));
    },

    async selectRange(low: number, high: number) {
      return new Promise((resolve, reject) => {
        const t = db.transaction(STORE, 'readonly');
        const index = t.objectStore(STORE).index('price');
        const range = IDBKeyRange.bound(low, high);
        const rows: Row[] = [];
        const req = index.openCursor(range);
        req.onsuccess = () => {
          const cursor = req.result;
          if (cursor) {
            rows.push(cursor.value);
            cursor.continue();
          } else {
            resolve(rows);
          }
        };
        req.onerror = () => reject(req.error);
      });
    },

    async aggregate() {
      // Manual GROUP BY category → { count, sum }
      return new Promise((resolve, reject) => {
        const t = db.transaction(STORE, 'readonly');
        const req = t.objectStore(STORE).openCursor();
        const groups: Record<string, { cnt: number; sum: number }> = {};
        req.onsuccess = () => {
          const cursor = req.result;
          if (cursor) {
            const row = cursor.value as Row;
            const g = groups[row.category] ??= { cnt: 0, sum: 0 };
            g.cnt++;
            g.sum += row.price;
            cursor.continue();
          } else {
            resolve(
              Object.entries(groups).map(([category, g]) => ({
                category,
                cnt: g.cnt,
                avg_price: g.sum / g.cnt,
              })),
            );
          }
        };
        req.onerror = () => reject(req.error);
      });
    },
  };
}

import type { Session } from "../src/index.js";

// ---------------------------------------------------------------------------
// Benchmark scenarios. They use only the public SQL API, so the same file can
// measure any revision of the engine, in Node or in a browser worker.
// ---------------------------------------------------------------------------

export interface Scenario {
  name: string;
  /** Operations performed by one run, for per-operation timings. */
  ops: number;
  /** Runs that do not modify the database share one prepared copy. */
  readOnly?: boolean;
  /** Prepares the database; not timed. */
  setup(session: Session): Promise<void>;
  /** The measured work. */
  run(session: Session): Promise<void>;
}

const TABLE_ROWS = 10_000;
const CATEGORIES = ["Electronics", "Books", "Clothing", "Food", "Toys"];

/** Deterministic row contents, so every run and revision sees the same data. */
function product(id: number): [number, string, number, string] {
  return [
    id,
    `Product ${id}`,
    ((id * 7919) % 10_000) / 100,
    CATEGORIES[id % CATEGORIES.length],
  ];
}

/** Spreads lookups over the whole key range without randomness. */
function scatter(i: number, n: number): number {
  return (i * 7919) % n;
}

async function createProducts(session: Session, rows: number): Promise<void> {
  await session.execute(
    "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
  );
  await insertProducts(session, 0, rows);
}

async function insertProducts(
  session: Session,
  from: number,
  count: number,
): Promise<void> {
  const insert = session.prepare(
    "INSERT INTO products VALUES ($1, $2, $3, $4)",
  );
  await session.execute("BEGIN");
  for (let id = from; id < from + count; id++) await insert.run(product(id));
  await session.execute("COMMIT");
}

export const SCENARIOS: Scenario[] = [
  {
    name: "insert 1 000 rows in one transaction",
    ops: 1000,
    setup: (s) => createProducts(s, 0),
    run: (s) => insertProducts(s, 0, 1000),
  },
  {
    name: "insert 100 rows, one commit each",
    ops: 100,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      const insert = s.prepare("INSERT INTO products VALUES ($1, $2, $3, $4)");
      for (let id = TABLE_ROWS; id < TABLE_ROWS + 100; id++) {
        await insert.run(product(id));
      }
    },
  },
  {
    name: "select by primary key ×1 000",
    ops: 1000,
    readOnly: true,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      const select = s.prepare("SELECT * FROM products WHERE id = $1");
      for (let i = 0; i < 1000; i++) await select.run([scatter(i, TABLE_ROWS)]);
    },
  },
  {
    name: "select range (BETWEEN) ×100",
    ops: 100,
    readOnly: true,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      for (let i = 0; i < 100; i++) {
        const low = scatter(i, 9_000) / 100;
        await s.execute(
          "SELECT * FROM products WHERE price BETWEEN $1 AND $2",
          [low, low + 10],
        );
      }
    },
  },
  {
    name: "full scan ×10",
    ops: 10,
    readOnly: true,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      for (let i = 0; i < 10; i++) await s.execute("SELECT * FROM products");
    },
  },
  {
    name: "GROUP BY + COUNT + AVG ×10",
    ops: 10,
    readOnly: true,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      for (let i = 0; i < 10; i++) {
        await s.execute(
          "SELECT category, COUNT(*) AS n, AVG(price) AS avg_price FROM products GROUP BY category",
        );
      }
    },
  },
  {
    name: "update by primary key, one commit each ×100",
    ops: 100,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      const update = s.prepare("UPDATE products SET price = $1 WHERE id = $2");
      for (let i = 0; i < 100; i++) {
        await update.run([i, scatter(i, TABLE_ROWS)]);
      }
    },
  },
  {
    name: "delete by primary key, one commit each ×100",
    ops: 100,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      const remove = s.prepare("DELETE FROM products WHERE id = $1");
      for (let i = 0; i < 100; i++) await remove.run([scatter(i, TABLE_ROWS)]);
    },
  },
];

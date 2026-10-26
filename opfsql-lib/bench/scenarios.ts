import type { PreparedStatement, Session } from "../src/worker-entry.js";

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
  const insert = await session.prepare(
    "INSERT INTO products VALUES ($1, $2, $3, $4)",
  );
  await session.execute("BEGIN");
  for (let id = from; id < from + count; id++) await insert.query(product(id));
  await session.execute("COMMIT");
}

// --- Workloads: joins, secondary indexes, pagination, JSON ------------------

/** One row's parameters, as a statement takes them. */
type Row = NonNullable<Parameters<PreparedStatement["query"]>[0]>;

const USERS = 1000;
const ORDERS = 20_000;
const ACCOUNTS = 10_000;
const EVENTS = 10_000;
const DOCUMENTS = 1000;
const PAGE = 20;
const KINDS = ["note", "task", "event", "photo", "link"];

/** Runs each statement of `statements`, one at a time. */
async function executeAll(
  session: Session,
  statements: string[],
): Promise<void> {
  for (const statement of statements) await session.execute(statement);
}

/** `rows` through one prepared INSERT, in one transaction. */
async function insertRows(
  session: Session,
  sql: string,
  rows: Iterable<Row>,
): Promise<void> {
  const insert = await session.prepare(sql);
  await session.execute("BEGIN");
  for (const row of rows) await insert.query(row);
  await session.execute("COMMIT");
}

function* range(count: number, row: (id: number) => Row): Generator<Row> {
  for (let id = 0; id < count; id++) yield row(id);
}

/** products with TABLE_ROWS rows. */
async function createAllProducts(session: Session): Promise<void> {
  await createProducts(session, TABLE_ROWS);
}

/** users (1 000) and their orders (20 000, 20 each), orders indexed by user. */
async function createShop(session: Session): Promise<void> {
  await executeAll(session, [
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, created_at INTEGER)",
    "CREATE INDEX orders_user ON orders (user_id)",
  ]);
  await insertRows(
    session,
    "INSERT INTO users VALUES ($1, $2, $3)",
    range(USERS, (id) => [
      id,
      `User ${String(id)}`,
      `user${String(id)}@example.com`,
    ]),
  );
  await insertRows(
    session,
    "INSERT INTO orders VALUES ($1, $2, $3, $4)",
    range(ORDERS, (id) => [
      id,
      scatter(id, USERS),
      ((id * 7919) % 100_000) / 100,
      id,
    ]),
  );
}

/** accounts (10 000), found by an indexed email. */
async function createAccounts(session: Session): Promise<void> {
  await executeAll(session, [
    "CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT, name TEXT)",
    "CREATE INDEX accounts_email ON accounts (email)",
  ]);
  await insertRows(
    session,
    "INSERT INTO accounts VALUES ($1, $2, $3)",
    range(ACCOUNTS, (id) => [id, accountEmail(id), `Account ${String(id)}`]),
  );
}

function accountEmail(id: number): string {
  return `account${String(id)}@example.com`;
}

/** events (10 000) with distinct, shuffled creation times under an index. */
async function createEvents(session: Session): Promise<void> {
  await executeAll(session, [
    "CREATE TABLE events (id INTEGER PRIMARY KEY, created_at INTEGER, name TEXT)",
    "CREATE INDEX events_created ON events (created_at)",
  ]);
  // 10 007 is prime, so i ↦ i·7919 mod 10 007 never repeats below it.
  await insertRows(
    session,
    "INSERT INTO events VALUES ($1, $2, $3)",
    range(EVENTS, (id) => [id, (id * 7919) % 10_007, `Event ${String(id)}`]),
  );
}

/** A JSON document, its shape varying with `id`. */
function document(id: number): Row[number] {
  return {
    title: `Document ${String(id)}`,
    kind: KINDS[id % KINDS.length] ?? "note",
    tags: [`tag${String(id % 7)}`, `tag${String(id % 11)}`],
    meta: { words: (id * 37) % 1000, draft: id % 3 === 0 },
  };
}

async function createDocumentTable(session: Session): Promise<void> {
  await createDocuments(session, 0);
}

async function createAllDocuments(session: Session): Promise<void> {
  await createDocuments(session, DOCUMENTS);
}

async function createDocuments(
  session: Session,
  documents: number,
): Promise<void> {
  await session.execute(
    "CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSON)",
  );
  await insertRows(
    session,
    "INSERT INTO docs VALUES ($1, $2)",
    range(documents, (id) => [id, document(id)]),
  );
}

/**
 * Pages through a query that returns a page of rows before the cursor, each
 * page's last row giving the next cursor at `cursorColumn`; the pages read.
 */
async function pageThrough(
  session: Session,
  sql: string,
  start: number,
  cursorColumn: number,
): Promise<number> {
  const page = await session.prepare(sql);
  let cursor = start;
  let pages = 0;
  for (;;) {
    const { rows } = await page.query([cursor]);
    const last = rows.at(-1);
    if (last === undefined) return pages;
    cursor = Number(last[cursorColumn]);
    pages++;
  }
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
      const insert = await s.prepare(
        "INSERT INTO products VALUES ($1, $2, $3, $4)",
      );
      for (let id = TABLE_ROWS; id < TABLE_ROWS + 100; id++) {
        await insert.query(product(id));
      }
    },
  },
  {
    name: "select by primary key ×1 000",
    ops: 1000,
    readOnly: true,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      const select = await s.prepare("SELECT * FROM products WHERE id = $1");
      for (let i = 0; i < 1000; i++)
        await select.query([scatter(i, TABLE_ROWS)]);
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
        await s.query("SELECT * FROM products WHERE price BETWEEN $1 AND $2", [
          low,
          low + 10,
        ]);
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
      const update = await s.prepare(
        "UPDATE products SET price = $1 WHERE id = $2",
      );
      for (let i = 0; i < 100; i++) {
        await update.query([i, scatter(i, TABLE_ROWS)]);
      }
    },
  },
  {
    name: "delete by primary key, one commit each ×100",
    ops: 100,
    setup: (s) => createProducts(s, TABLE_ROWS),
    async run(s) {
      const remove = await s.prepare("DELETE FROM products WHERE id = $1");
      for (let i = 0; i < 100; i++)
        await remove.query([scatter(i, TABLE_ROWS)]);
    },
  },
  {
    name: "join: an order with its user ×200",
    ops: 200,
    readOnly: true,
    setup: createShop,
    async run(s) {
      const select = await s.prepare(
        "SELECT o.id, o.total, u.name FROM orders o JOIN users u ON u.id = o.user_id WHERE o.id = $1",
      );
      for (let count = 0; count < 200; count++)
        await select.query([scatter(count, ORDERS)]);
    },
  },
  {
    name: "join: a user's orders ×20",
    ops: 20,
    readOnly: true,
    setup: createShop,
    async run(s) {
      const select = await s.prepare(
        "SELECT o.id, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = $1",
      );
      for (let count = 0; count < 20; count++)
        await select.query([scatter(count, USERS)]);
    },
  },
  {
    name: "join + GROUP BY: order totals per user ×5",
    ops: 5,
    readOnly: true,
    setup: createShop,
    async run(s) {
      for (let count = 0; count < 5; count++) {
        await s.execute(
          "SELECT u.id, u.name, COUNT(*) AS orders, SUM(o.total) AS spent FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.id, u.name",
        );
      }
    },
  },
  {
    name: "secondary index: account by email ×1 000",
    ops: 1000,
    readOnly: true,
    setup: createAccounts,
    async run(s) {
      const select = await s.prepare("SELECT * FROM accounts WHERE email = $1");
      for (let count = 0; count < 1000; count++) {
        await select.query([accountEmail(scatter(count, ACCOUNTS))]);
      }
    },
  },
  {
    name: "keyset pages by primary key, 20 a page",
    ops: TABLE_ROWS / PAGE,
    readOnly: true,
    setup: createAllProducts,
    async run(s) {
      const sql =
        "SELECT * FROM products WHERE id < $1 ORDER BY id DESC LIMIT 20";
      await pageThrough(s, sql, TABLE_ROWS, 0);
    },
  },
  {
    name: "keyset pages by an indexed column, 20 a page",
    ops: EVENTS / PAGE,
    readOnly: true,
    setup: createEvents,
    async run(s) {
      const sql =
        "SELECT * FROM events WHERE created_at < $1 ORDER BY created_at DESC LIMIT 20";
      await pageThrough(s, sql, Number.MAX_SAFE_INTEGER, 1);
    },
  },
  {
    name: "prepared point query, prepared once ×10 000",
    ops: 10_000,
    readOnly: true,
    setup: createAllProducts,
    async run(s) {
      const select = await s.prepare("SELECT * FROM products WHERE id = $1");
      for (let count = 0; count < 10_000; count++) {
        await select.query([scatter(count, TABLE_ROWS)]);
      }
    },
  },
  {
    name: "update all 10 000 rows in one statement",
    ops: TABLE_ROWS,
    setup: createAllProducts,
    async run(s) {
      await s.execute("UPDATE products SET price = price * 1.1");
    },
  },
  {
    name: "JSON: insert 1 000 documents in one transaction",
    ops: DOCUMENTS,
    setup: createDocumentTable,
    async run(s) {
      const sql = "INSERT INTO docs VALUES ($1, $2)";
      await insertRows(
        s,
        sql,
        range(DOCUMENTS, (id) => [id, document(id)]),
      );
    },
  },
  {
    name: "JSON: read back 1 000 documents ×10",
    ops: 10,
    readOnly: true,
    setup: createAllDocuments,
    async run(s) {
      for (let count = 0; count < 10; count++)
        await s.execute("SELECT data FROM docs");
    },
  },
  {
    name: "JSON: filter on a path ×10",
    ops: 10,
    readOnly: true,
    setup: createAllDocuments,
    async run(s) {
      const select = await s.prepare(
        "SELECT id FROM docs WHERE data.kind = $1",
      );
      for (let count = 0; count < 10; count++) {
        await select.query([KINDS[count % KINDS.length]]);
      }
    },
  },
  {
    name: "insert then select, one commit each ×100",
    ops: 100,
    setup: createAllProducts,
    async run(s) {
      const insert = await s.prepare(
        "INSERT INTO products VALUES ($1, $2, $3, $4)",
      );
      const select = await s.prepare("SELECT * FROM products WHERE id = $1");
      for (let id = TABLE_ROWS; id < TABLE_ROWS + 100; id++) {
        await insert.query(product(id));
        await select.query([id]);
      }
    },
  },
];

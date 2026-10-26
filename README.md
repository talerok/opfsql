# opfsql

opfsql is a SQL database written in TypeScript. It runs in a dedicated
worker and stores its data in the browser's Origin Private File System
(OPFS). It uses no WebAssembly and has no runtime dependencies. It is meant
for web apps that keep their data on the user's device and want to query it
with SQL.

Try it in the [demo](https://talerok.github.io/opfsql/client/): a SQL
console on opfsql, built from [`client/`](https://github.com/talerok/opfsql/tree/main/client).

**Status:** version 0.1.0, before 1.0. The file format is version 5. The
API, the semantics and the file format may still change before 1.0. opfsql
cannot open databases written by 0.0.x: opening one fails with
`UNSUPPORTED_FORMAT`, and nothing converts it. See
[CHANGELOG.md](https://github.com/talerok/opfsql/blob/main/CHANGELOG.md).

## How it is verified

195,993 checks run against the code in this repository:

| Checks                                                                                                                                   | Count   | Reproduce                                                                                         |
| ---------------------------------------------------------------------------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------- |
| records from a subset of SQLite's [sqllogictest](https://www.sqlite.org/sqllogictest/) corpus, compared with SQLite's results            | 122,118 | `npx vitest run test/sqllogictest`; counts: `npx vite-node test/sqllogictest/triage.ts`           |
| opfsql's own tests: the 2,051 of `npx vitest run`, less the 30 that run the corpus, the 2 interleaving tests and the 8 metamorphic tests | 2,011   | `npx vitest run`                                                                                  |
| runs of the multi-tab interleaving test: 300 seeds, with and without torn writes                                                         | 600     | `INTERLEAVING_SEEDS=300 npx vitest run src/storage/pager/tests/interleaving.test.ts`              |
| metamorphic checks of queries on 400 random databases                                                                                    | 71,219  | `METAMORPHIC_SEED=5000 METAMORPHIC_SEEDS=400 npx vitest run test/metamorphic/metamorphic.test.ts` |
| end-to-end tests in Chromium, Firefox and WebKit                                                                                         | 45      | `npm run test:e2e`                                                                                |

All of them pass. The commands run from `opfsql-lib`.

- **sqllogictest.** The subset is 22 files of SQLite's corpus: SELECT,
  joins, aggregates, subqueries, NULL logic, IN, UPDATE, DROP, and the
  same queries over tables with and without indexes. Of the 122,118 records
  (8,627 statements and 113,491 queries), 121,248 give SQLite's result. The
  other 870 are listed in
  [`skips.txt`](https://github.com/talerok/opfsql/blob/main/opfsql-lib/test/sqllogictest/skips.txt) with a reason
  each, and the suite fails if one of them starts to pass:
  - 808 use features opfsql does not have: descending index keys (695),
    `x IN table` (72), parentheses around FROM items (29), `INDEXED BY`
    (6) and the `<<` operator (6);
  - 49 read a column that is neither grouped nor aggregated;
  - 13 hit another documented departure from SQLite.

  [test/sqllogictest/README.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/test/sqllogictest/README.md) says
  which files were chosen and why.

- **opfsql's own tests** cover each layer, from bytes on a page to the
  worker's messages. They include seeded property tests against a model of
  the data, an integrity check after every step of the storage fuzz test,
  and the engine's suites run a second time with 4 KB pages and frequent
  checkpoints.
- **The interleaving test** runs three tabs over shared files. At every
  file operation of one tab, another tab may run a whole step first. It
  checks every read against a model of what each commit wrote, and checks
  that a snapshot a tab holds never changes under it. Every `npx vitest run`
  runs 8 seeds of each kind; the figure above is a run of 300.
- **The metamorphic tests** generate random databases and random
  well-typed queries, and check each query against other forms of it that
  must return the same rows: the query split by its condition into the
  rows where it is true, false and NULL; a count against a sum; LIMIT and
  OFFSET against a slice of the full order; constants against parameters;
  and a copy of the database with random indexes against one without.
  Besides those checks, random UPDATE, DELETE and INSERT statements must
  leave both copies the same and pass the integrity check. They need no
  reference database, so they reach what the sqllogictest subset cannot:
  JSON, BOOLEAN, parameters and the edges of the safe integer range. The
  run above made 72,000 checks and skipped 781 whose forms overflowed the
  safe integer range, which the generator provokes on purpose. Every
  `npx vitest run` runs 4 databases, 720 checks.
- **The end-to-end tests** run in Playwright's Chromium 147, Firefox 148 and
  WebKit 26.4, several tabs included. They cover crashes with open
  transactions, recovery from the WAL, the write lock across tabs, and
  errors that cross the worker boundary.

## Supported

[docs/semantics.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/semantics.md) defines what each of
these does.

| Area         | Supported                                                                                                                                                                                                                                                                                                                                       |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Queries      | `SELECT` with `DISTINCT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY` (with `NULLS FIRST` and `NULLS LAST`), and `LIMIT` and `OFFSET` (constants or parameters). Also `SELECT` without `FROM`                                                                                                                                                     |
| Joins        | `INNER`, `LEFT`, `RIGHT` and `CROSS`, with `ON` or `USING`                                                                                                                                                                                                                                                                                      |
| Subqueries   | scalar, `IN`, `EXISTS`, `ANY` and `ALL`, correlated or not; subqueries in `FROM`                                                                                                                                                                                                                                                                |
| CTEs         | `WITH`, including `WITH RECURSIVE`                                                                                                                                                                                                                                                                                                              |
| Compound     | `UNION`, `UNION ALL`, `INTERSECT` and `EXCEPT`                                                                                                                                                                                                                                                                                                  |
| Writes       | `INSERT` (several rows, or `INSERT … SELECT`), `UPDATE`, `DELETE`, and `INSERT … ON CONFLICT DO NOTHING` or `DO UPDATE`                                                                                                                                                                                                                         |
| Aggregates   | `count`, `sum`, `total`, `avg`, `min`, `max` and `group_concat`, each also with `DISTINCT`                                                                                                                                                                                                                                                      |
| Functions    | text: `upper`, `lower`, `length`, `trim`, `ltrim`, `rtrim`, `substr`, `substring`, `replace`, `instr`, `concat`, `concat_ws`, `hex`. Numbers: `abs`, `sign`, `round`, `floor`, `ceil`, `ceiling`, `random`. JSON: `json_extract`, `json_type`, `json_array_length`, `json_valid`. Other: `coalesce`, `ifnull`, `nullif`, `iif`, `typeof`, `md5` |
| Expressions  | arithmetic, comparisons, `AND`, `OR`, `NOT`, `\|\|`, `LIKE`, `BETWEEN`, `IN`, `IS [NOT] NULL`, `CASE`, `CAST`, and JSON paths such as `data.items[0].title`                                                                                                                                                                                     |
| Tables       | `CREATE TABLE` and `DROP TABLE`, with `IF [NOT] EXISTS`; `ALTER TABLE … ADD COLUMN` and `DROP COLUMN`                                                                                                                                                                                                                                           |
| Indexes      | `CREATE INDEX` and `DROP INDEX`, unique or not, on several columns or on expressions such as `lower(email)`                                                                                                                                                                                                                                     |
| Constraints  | `PRIMARY KEY` (on one column or several), `UNIQUE` on a column, `NOT NULL`, `DEFAULT` and `AUTOINCREMENT`                                                                                                                                                                                                                                       |
| Types        | `INTEGER`, `REAL`, `TEXT`, `BLOB`, `BOOLEAN`, `JSON` (any JSON value, queried by path) and `ANY`. `INT`, `BIGINT`, `VARCHAR(n)` and other common names map onto these                                                                                                                                                                           |
| Transactions | `BEGIN` (deferred or `IMMEDIATE`), `COMMIT` and `ROLLBACK`, with snapshot isolation. A failed statement inside a transaction is rolled back alone, and the transaction goes on, as in SQLite                                                                                                                                                    |
| API          | prepared statements with `$1`, `$2` … parameters; connections; `transaction(work)`; `AbortSignal` on every call; `schema()`; `EXPLAIN`, which shows the plan a statement runs                                                                                                                                                                   |
| Tabs         | several tabs on one database, in Chromium (see [Browsers](#browsers))                                                                                                                                                                                                                                                                           |

## Not supported

Each of these fails with `NOT_SUPPORTED` before anything runs, unless the
list says otherwise. Nothing is parsed and then ignored. docs/semantics.md
has the list under
[Not supported](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/semantics.md#not-supported).

- **Foreign keys.** A `CREATE TABLE` with `REFERENCES` or `FOREIGN KEY`
  fails, and the table is not created. Nothing is enforced, because nothing
  is accepted.
- **Window functions:** `OVER`, `PARTITION BY`, `row_number()` and the rest.
- **Savepoints in SQL:** `SAVEPOINT`, `RELEASE` and `ROLLBACK TO`. Each
  statement inside a transaction still runs in a savepoint of its own; SQL
  cannot name one.
- **Interrupting a running statement.** An `AbortSignal` cancels a call only
  while it waits. A statement that has started runs to its end. `close()`
  waits for it, and terminates the worker if closing takes more than 10
  seconds.
- Joins: `FULL OUTER JOIN`, `NATURAL JOIN`, and parentheses around FROM
  items.
- Constraints: `CHECK`, a table-level `UNIQUE (a, b)` (use
  `CREATE UNIQUE INDEX`), and generated columns.
- Schema: views, triggers, `ALTER TABLE … RENAME`, `CREATE TABLE … AS`,
  temporary tables, `WITHOUT ROWID`, `STRICT`, descending index keys
  (`CREATE INDEX … (x DESC)`; an ascending index is read backward instead),
  and `REINDEX`.
- Writes: `REPLACE`, `INSERT OR …` and `UPDATE OR …`, `RETURNING`,
  `UPDATE … FROM`, and `DEFAULT VALUES`.
- Expressions: the bitwise operators `&`, `|`, `~`, `<<` and `>>`;
  `COLLATE`; `GLOB`; `LIKE … ESCAPE`; `IS` and `IS NOT` with anything but
  NULL; `IS [NOT] DISTINCT FROM`; `x IN table`; `FILTER` on aggregates; and
  `VALUES` as a query.
- `INDEXED BY` and `NOT INDEXED`, `ATTACH`, `PRAGMA` and `VACUUM`.
- Functions that are not in the list above, such as date and time
  functions and `printf`. These fail with `SEMANTIC_ERROR`: "no such
  function".

## Differences from SQLite

opfsql follows SQLite with STRICT tables, except where JavaScript makes
another choice clearly better. The
[Departures table](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/semantics.md#departures-from-sqlite) in
docs/semantics.md lists every difference. These are the ones that most
often surprise people who know SQLite:

- **BOOLEAN is its own type.** `true` is not `1`. Comparing a BOOLEAN
  column with `1` is a bind error. You get back `true` and `false`, not `1`
  and `0`.
- **Comparisons do not convert values.** `id = '5'` for an INTEGER `id` is
  a bind error; write `id = 5`. Only parameters are converted, so `id = $1`
  with `'5'` still finds id 5.
- **INTEGER means a safe JavaScript integer**, up to ±(2⁵³ − 1). A result
  outside that range fails with `INTEGER_OVERFLOW`.
- **Nothing is guessed.**
  - `CAST('12abc' AS INTEGER)` fails, where SQLite gives 12.
  - A scalar subquery that returns several rows fails.
  - Selecting a column that is neither grouped nor aggregated fails.
  - `WHERE` takes only a BOOLEAN.

## Browsers

| Browser         | Tabs on one database                                  |
| --------------- | ----------------------------------------------------- |
| Chromium        | up to 64 at once                                      |
| Firefox, Safari | one; another tab that opens it gets `DATABASE_IN_USE` |

The limit comes from the platform. opfsql reads and writes its files
through OPFS synchronous access handles (`createSyncAccessHandle`), the only
synchronous file API in the browser, and it exists only in workers:

- **Chromium** has had a locking mode since Chrome 121:
  `{ mode: "readwrite-unsafe" }` lets handles in several tabs hold the same
  file at once.
- **Firefox and Safari** implement only the default, exclusive mode: one
  handle per file in the whole origin. A second tab cannot open the
  database's files.

The locking modes are a proposal of the WHATWG File System standard, not yet
part of it.

When several tabs share a database, opfsql's own protocol keeps them
consistent. It uses a WAL, a write lock through Web Locks, and a small
third file, `name.opfsql-shm`. In that file each tab records the oldest
snapshot it is still reading, so another tab's checkpoint does not
overwrite pages that the snapshot needs.
[docs/concurrency.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/concurrency.md) has the details.

This is a deliberate choice. opfsql does not send other tabs' queries
through a "leader" tab: then every query would cross tabs as a message, and
closing the leader would interrupt the others. When Firefox and Safari
support the locking modes, several tabs will work there with no change to
the file format.

In every browser, a database is three files in OPFS: `name.opfsql`,
`name.opfsql-wal` and `name.opfsql-shm`.

The end-to-end tests run in Playwright's Chromium 147, Firefox 148 and
WebKit 26.4. No older version has been tested. Besides OPFS sync access
handles, opfsql needs module workers, Web Locks and
`Promise.withResolvers`.

## Size

`npm run build` writes three bundles to `opfsql-lib/dist`. Both entry
points import a chunk that they share, which holds the errors and `Result`:

| Entry point                      | Files                      | Raw      | Gzipped |
| -------------------------------- | -------------------------- | -------- | ------- |
| `"opfsql"`, loaded by the page   | `index.js` + shared chunk  | 19.4 kB  | 6.3 kB  |
| `"opfsql/worker"`, in the worker | `worker.js` + shared chunk | 274.9 kB | 82.9 kB |

An app loads both: the client on the page and the engine in the worker,
about 89 kB gzipped in all. The page never loads the engine.

- The figures are the bytes of the minified bundles, gzipped with
  `gzip -9`: for example,
  `cat dist/index.js dist/chunk-*.js | gzip -9 | wc -c`.
- An app's bundler minifies and splits the code its own way, so its figures
  will differ a little.
- The npm package also holds type declarations, which nothing loads at run
  time.

## Performance

These figures come from the scenarios in
[bench/scenarios.ts](https://github.com/talerok/opfsql/blob/main/opfsql-lib/bench/scenarios.ts), each the median of 15
runs after 5 warmup runs. They were taken on an Apple M4 (10 cores) on
2026-10-26, while other work ran on the machine (1-minute load average
13–25). In those conditions, the median of one scenario moved by up to 2×
from run to run, and more for bulk inserts. Read the figures as orders of
magnitude.

- **Node** runs the bundled, minified engine on memory files
  (`npm run bench`). It measures the CPU path: planning, executing and
  B-tree work.
- **Chromium** runs it in a worker on real OPFS, in a persistent profile
  (`npm run bench:browser`). [bench/README.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/bench/README.md)
  has the full table.

| Operation                                      | Node, memory files | Chromium, OPFS |
| ---------------------------------------------- | -----------------: | -------------: |
| point query by primary key, prepared           |             5.5 µs |        35.2 µs |
| lookup by an indexed TEXT column               |            12.9 µs |        33.4 µs |
| one page of 20 rows, keyset on the primary key |            21.9 µs |        40.8 µs |
| join one order to its user, by key             |            14.5 µs |        33.5 µs |
| insert one row and commit                      |             144 µs |       1,142 µs |
| insert 1,000 rows in one transaction, per row  |            12.1 µs |        15.5 µs |
| full scan of 10,000 rows                       |             3.3 ms |         4.4 ms |
| GROUP BY over 10,000 rows                      |             4.8 ms |         5.9 ms |
| read back 1,000 JSON documents                 |             1.1 ms |         1.3 ms |

- **Commits.** In Chromium a commit costs about 1 ms, against 0.14 ms in
  Node. On the same machine, flushing one 8 KB write took 0.2–0.3 ms by
  itself.
- **Private windows.** In private browsing, Chromium's OPFS is about 100
  times slower per file operation, so a point query takes about 0.7 ms.
  This was measured in a Playwright ephemeral context, which is a private
  profile.

What limits performance:

- **No statistics.** The planner counts every table as 10,000 rows. So it
  ranks access paths well (a rowid, then a unique key, then an equality,
  then a range, then a full read). But it cannot tell a small table from a
  large one when it orders joins.
- **No page cache across transactions, by design.** Each transaction reads
  pages from the file again. Another tab may have changed them, and a stale
  cache would return wrong rows. Reads cost a few microseconds each.
- **Some work happens in memory.**
  - Sorts, hash joins, aggregates, `DISTINCT` and compound selects hold
    their rows in memory.
  - A write transaction keeps the pages it changes in memory until it
    commits.
- **A running statement cannot be interrupted.**

## Quick start

```sh
npm install opfsql
```

```ts
// db-worker.ts — the worker's script
import { serveDatabase } from "opfsql/worker";
serveDatabase();
```

```ts
// main.ts
import { openDatabase } from "opfsql";

const db = await openDatabase("app", {
  worker: new Worker(new URL("./db-worker.ts", import.meta.url), {
    type: "module",
  }),
});

await db.execute(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  )
`);

await db.query("INSERT INTO users (name, email) VALUES ($1, $2)", [
  "Alice",
  "alice@example.com",
]);

const result = await db.query("SELECT id, name FROM users");
result.columns; // ["id", "name"]
result.rows; // [[1, "Alice"]]
result.objects(); // [{ id: 1, name: "Alice" }]

await db.close();
```

Each of the database's calls is described in
[docs/api.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/api.md):

| Call                  | Does                                                    |
| --------------------- | ------------------------------------------------------- |
| `query(sql, params?)` | runs one statement and returns its `Result`             |
| `execute(script)`     | runs a script of statements, one `Result` each          |
| `prepare(sql)`        | plans a statement once, to run with `query(params?)`    |
| `transaction(work)`   | runs `work` in a transaction on a connection of its own |
| `connect()`           | opens a connection, whose calls run in order            |
| `schema()`            | describes the tables, columns and indexes               |
| `close()`             | closes the database and terminates the worker           |

A statement takes exactly as many parameters as it uses. Every error is an
`OpfsqlError` with a stable `code`, such as `CONSTRAINT_VIOLATION` or
`BUSY`, and it keeps its class across the worker boundary. Code that
already runs in a worker can use the engine without messages:
`Engine.open(name)` from `"opfsql/worker"`.

## Documentation

- [docs/api.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/api.md): the API, transactions, errors and
  every export.
- [docs/semantics.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/semantics.md): values, types,
  comparisons, functions, what is not supported, and the departures from
  SQLite.
- [docs/concurrency.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/concurrency.md): how tabs share a
  database.
- [docs/layers.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/layers.md): what each layer of the code
  guarantees the one above it.
- [docs/architecture.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/architecture.md) and
  [docs/file-format.md](https://github.com/talerok/opfsql/blob/main/opfsql-lib/docs/file-format.md): the code and the
  files.
- [CHANGELOG.md](https://github.com/talerok/opfsql/blob/main/CHANGELOG.md): what changed since 0.0.2.

## License

MIT

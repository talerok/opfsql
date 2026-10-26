# opfsql

An embedded SQL database for the browser, written in TypeScript, with no
dependencies. It runs in a dedicated worker and stores its data in the
origin's private file system (OPFS); several tabs can share one database in
Chromium. The page's client is about 6 KB gzipped, the worker about 78 KB.

[Live Client](https://talerok.github.io/opfsql/client/) | [Benchmark](https://talerok.github.io/opfsql/benchmark/)

## Quick Start

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

## API

[docs/api.md](opfsql-lib/docs/api.md) is the full contract; in short:

| `Database` (from `openDatabase`) | Does                                                      |
| -------------------------------- | --------------------------------------------------------- |
| `query(sql, params?)`            | runs one statement and returns its `Result`               |
| `execute(script)`                | runs a script of statements, one `Result` each            |
| `prepare(sql)`                   | a `Statement` planned once, run with `query(params?)`     |
| `transaction(work)`              | runs `work` in a transaction on a connection of its own   |
| `connect()`                      | a `Connection`: calls in order, transactions across calls |
| `schema()`                       | the tables, columns and indexes                           |
| `close()`                        | closes the database and terminates the worker             |

A `Result` has `columns`, `rows` (arrays of values), `changes`,
`lastInsertRowid` and `objects()`. Parameters are `$1`, `$2`, …, and a
statement takes exactly as many as it uses. Errors are `OpfsqlError`s with
a stable `code` (`CONSTRAINT_VIOLATION`, `BUSY`, …), and keep their class
across the worker boundary.

Inside a worker, `Engine.open(name)` from `"opfsql/worker"` gives the same
API without messages.

Transactions read one snapshot; one writer at a time holds the write lock,
across tabs. A statement that fails inside a transaction is rolled back
alone, as in SQLite, and the transaction goes on.

## Supported SQL

### Data Types

`INTEGER` (`INT`, `BIGINT`, `SMALLINT`, `TINYINT`), `REAL` (`FLOAT`, `DOUBLE`), `TEXT` (`VARCHAR(n)`, `CHAR(n)`), `BLOB`, `BOOLEAN`, `JSON`, `ANY` — see [docs/semantics.md](opfsql-lib/docs/semantics.md)

### Statements

| Statement                       | Notes                                                              |
| ------------------------------- | ------------------------------------------------------------------ |
| `SELECT`                        | `DISTINCT`, column aliases, table aliases, `SELECT` without `FROM` |
| `INSERT`                        | Multi-row, `INSERT...SELECT`, `ON CONFLICT DO NOTHING/UPDATE`      |
| `UPDATE`                        | Expressions in `SET`                                               |
| `DELETE`                        | With or without `WHERE`                                            |
| `CREATE TABLE`                  | `IF NOT EXISTS`, column constraints, a table-level `PRIMARY KEY`   |
| `ALTER TABLE`                   | `ADD COLUMN`, `DROP COLUMN`                                        |
| `DROP TABLE`                    | `IF EXISTS`                                                        |
| `CREATE INDEX`                  | `UNIQUE`, multi-column, expression-based, `IF NOT EXISTS`          |
| `DROP INDEX`                    |                                                                    |
| `BEGIN` / `COMMIT` / `ROLLBACK` | Snapshot isolation, autocommit for standalone statements           |
| `EXPLAIN`                       | Shows the physical plan a statement runs                           |

### Clauses

| Clause                | Notes                                                  |
| --------------------- | ------------------------------------------------------ |
| `WHERE`               | All comparison, logical, and arithmetic operators      |
| `ORDER BY`            | Multi-column, `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST` |
| `GROUP BY`            | Multiple columns                                       |
| `HAVING`              |                                                        |
| `LIMIT` / `OFFSET`    |                                                        |
| `JOIN`                | `INNER`, `LEFT`, `RIGHT`, `CROSS`; `ON` and `USING`    |
| `UNION` / `UNION ALL` | Chainable; also `INTERSECT` and `EXCEPT`               |
| `WITH` (CTE)          | Multiple CTEs, `WITH RECURSIVE`                        |

### Expressions

| Expression  | Examples                                                 |
| ----------- | -------------------------------------------------------- |
| Arithmetic  | `+`, `-`, `*`, `/`, `%`                                  |
| Comparison  | `=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`                     |
| Logical     | `AND`, `OR`, `NOT`                                       |
| String      | `\|\|` (concatenation)                                   |
| Pattern     | `LIKE`, `NOT LIKE`                                       |
| Range       | `BETWEEN`, `NOT BETWEEN`                                 |
| Set         | `IN (...)`, `NOT IN (...)`                               |
| Null check  | `IS NULL`, `IS NOT NULL`                                 |
| Conditional | `CASE WHEN ... THEN ... ELSE ... END`                    |
| Cast        | `CAST(expr AS type)`                                     |
| Subquery    | Scalar, `EXISTS`, `NOT EXISTS`, `ANY`, `ALL`, correlated |
| JSON path   | `data.name`, `items[0]`, `data.items[0].title`           |

### Functions

**Aggregate:** `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX`

**String:** `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `LTRIM`, `RTRIM`, `SUBSTR`/`SUBSTRING`, `REPLACE`, `INSTR`, `CONCAT`, `CONCAT_WS`, `HEX`, `LIKE`

**Math:** `ABS`, `SIGN`, `ROUND`, `FLOOR`, `CEIL`/`CEILING`, `RANDOM`

**JSON:** `JSON_EXTRACT`, `JSON_TYPE`, `JSON_ARRAY_LENGTH`, `JSON_VALID`

**Hash:** `MD5`

**Utility:** `COALESCE`, `IFNULL`, `NULLIF`, `IIF`, `TYPEOF`

### Constraints

`PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `DEFAULT`, `AUTOINCREMENT`

### Other

- Comments: `-- single line`, `/* block */`
- Quoted identifiers: `"my column"`
- Blob literals: `x'DEADBEEF'`
- Boolean literals: `TRUE`, `FALSE`

## Architecture

```
text ─parse─▶ syntax tree ─bind─▶ bound statement ─optimize─▶ physical plan ─run─▶ rows
             sql/parser           sql/binder                  sql/optimizer         executor
                                                                                       │
                                                       table: rows, indexes, constraints
                                                                                       │
                                     storage: B-trees, WAL, snapshots, read marks — OPFS
```

- **sql/parser** — SQL text to syntax trees
- **sql/binder** — resolves names, scopes and types against the schema
- **sql/optimizer** — pushes filters down, chooses joins, access paths, sort order and MIN/MAX through indexes
- **executor** — pulls rows through operators in batches, no more than it needs
- **table** — one write path for rows, constraints, rowids, AUTOINCREMENT and index entries
- **storage** — pages as bytes (format 5), B+trees, a WAL with snapshot isolation, read marks shared across tabs
- **engine / worker** — sessions, transactions, the write lock, and the client the page talks to

[docs/architecture.md](opfsql-lib/docs/architecture.md) has the module map and rules,
[docs/file-format.md](opfsql-lib/docs/file-format.md) the files. Databases
written by opfsql 0.0.x (file format 4) are not read: opening one fails with
`UNSUPPORTED_FORMAT`.

## Not Supported (vs SQLite)

| Feature                                                      | Status                       |
| ------------------------------------------------------------ | ---------------------------- |
| Window functions (`OVER`, `PARTITION BY`, `ROW_NUMBER`, ...) | Not supported                |
| `FULL OUTER JOIN`, `NATURAL JOIN`                            | Not supported                |
| `FOREIGN KEY`, `REFERENCES`                                  | Not supported                |
| A table-level `UNIQUE` constraint                            | Not supported                |
| Views (`CREATE VIEW`)                                        | Not supported                |
| Triggers                                                     | Not supported                |
| `SAVEPOINT` / `RELEASE` / `ROLLBACK TO`                      | Not supported                |
| `CHECK` constraints                                          | Not supported                |
| `ATTACH` / `DETACH`                                          | Not supported                |
| `PRAGMA`                                                     | Not supported                |
| Collation                                                    | Not configurable             |
| Type affinity                                                | Strict logical types instead |

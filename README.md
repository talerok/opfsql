# opfsql

Lightweight SQL engine written in TypeScript. ~40 KB gzipped, zero native dependencies. Runs in browser workers (via OPFS) and Node.js.

## Quick Start

### Browser Worker

`Engine.open()` creates a database backed by OPFS with WAL. Must be called inside a Web Worker.

```ts
import { Engine } from "opfsql";

const engine = await Engine.open("my-db");

engine.execute(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  )
`);

engine.execute("INSERT INTO users (name, email) VALUES ($1, $2)", [
  "Alice",
  "alice@example.com",
]);

const [result] = engine.execute("SELECT * FROM users");
// result.type === "rows"
// result.rows === [{ id: 1, name: "Alice", email: "alice@example.com" }]

engine.close();
```

### Node.js

```ts
import { Engine } from "opfsql";
import { NodeSyncStorage } from "opfsql/store/backend/node-storage.js";
import { WalStorage } from "opfsql/store/wal/wal-storage.js";
import { NodeFileHandle } from "opfsql/store/backend/node-storage.js";

const main = new NodeSyncStorage("./data/my.db");
const walHandle = new NodeFileHandle("./data/my.db-wal");
const engine = await Engine.create(new WalStorage(main, walHandle));

engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
engine.close();
```

### Worker Bridge (main thread &rarr; worker)

For use from the main thread, `WorkerEngine` provides an async RPC wrapper:

```ts
import { WorkerEngine } from "opfsql/worker";

const engine = new WorkerEngine(new URL("./worker.ts", import.meta.url));
await engine.open("my-db");

const results = await engine.exec("SELECT 1 + 1 AS sum");
// results[0].rows === [{ sum: 2 }]

await engine.close();
```

## API

### `Engine`

| Method                         | Returns             | Description                                       |
| ------------------------------ | ------------------- | ------------------------------------------------- |
| `Engine.open(dbName)`          | `Promise<Engine>`   | Open OPFS database with WAL (worker only)         |
| `Engine.create(backend)`       | `Promise<Engine>`   | Create engine with custom storage backend         |
| `engine.execute(sql, params?)` | `Result[]`          | Execute one or more SQL statements                |
| `engine.prepare(sql)`          | `PreparedStatement` | Prepare a single statement for repeated execution |
| `engine.close()`               | `void`              | Close the database                                |

### `PreparedStatement`

| Method              | Returns  | Description                      |
| ------------------- | -------- | -------------------------------- |
| `stmt.run(params?)` | `Result` | Execute with optional parameters |

### `Result`

```ts
interface Result {
  type: "rows" | "ok";
  rows?: Row[]; // SELECT results
  rowsAffected?: number; // INSERT/UPDATE/DELETE count
}

type Row = Record<string, Value>;
type Value = string | number | boolean | null | JsonValue | Uint8Array;
```

### Parameters

Use `$1`, `$2`, etc. for parameterized queries:

```ts
engine.execute("SELECT * FROM users WHERE id = $1 AND name = $2", [1, "Alice"]);
```

## Supported SQL

### Data Types

`INTEGER`, `BIGINT`, `SMALLINT`, `FLOAT`/`REAL`, `DOUBLE`, `TEXT`/`VARCHAR`/`CHAR`, `BLOB`, `BOOLEAN`, `JSON`

### Statements

| Statement                       | Notes                                                              |
| ------------------------------- | ------------------------------------------------------------------ |
| `SELECT`                        | `DISTINCT`, column aliases, table aliases, `SELECT` without `FROM` |
| `INSERT`                        | Multi-row, `INSERT...SELECT`, `ON CONFLICT DO NOTHING/UPDATE`      |
| `UPDATE`                        | Expressions in `SET`                                               |
| `DELETE`                        | With or without `WHERE`                                            |
| `CREATE TABLE`                  | `IF NOT EXISTS`, column and table-level constraints                |
| `ALTER TABLE`                   | `ADD COLUMN`, `DROP COLUMN`                                        |
| `DROP TABLE`                    | `IF EXISTS`                                                        |
| `CREATE INDEX`                  | `UNIQUE`, multi-column, `IF NOT EXISTS`                            |
| `DROP INDEX`                    |                                                                    |
| `BEGIN` / `COMMIT` / `ROLLBACK` | Snapshot isolation, autocommit for standalone statements           |
| `EXPLAIN`                       | Shows optimized logical plan for any statement                     |

### Clauses

| Clause                | Notes                                                  |
| --------------------- | ------------------------------------------------------ |
| `WHERE`               | All comparison, logical, and arithmetic operators      |
| `ORDER BY`            | Multi-column, `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST` |
| `GROUP BY`            | Multiple columns                                       |
| `HAVING`              |                                                        |
| `LIMIT` / `OFFSET`    |                                                        |
| `JOIN`                | `INNER`, `LEFT`, `CROSS`; `ON` and `USING`             |
| `UNION` / `UNION ALL` | Chainable                                              |
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

**String:** `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `LTRIM`, `RTRIM`, `SUBSTR`/`SUBSTRING`, `REPLACE`, `CONCAT`

**Math:** `ABS`, `ROUND`, `FLOOR`, `CEIL`/`CEILING`

**Utility:** `COALESCE`, `NULLIF`, `TYPEOF`

### Constraints

`PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `DEFAULT`, `AUTOINCREMENT`, `FOREIGN KEY` (parsed, not enforced)

### Other

- Comments: `-- single line`, `/* block */`
- Quoted identifiers: `"my column"`
- Blob literals: `x'DEADBEEF'`
- Boolean literals: `TRUE`, `FALSE`

## Architecture

```
SQL string
  │
  ▼
┌────────┐   ┌────────┐   ┌───────────┐   ┌──────────┐
│ Lexer  │──▶│ Parser │──▶│  Binder   │──▶│Optimizer │
└────────┘   └────────┘   └───────────┘   └──────────┘
                                               │
                                               ▼
                                         ┌──────────┐
                                         │ Executor │
                                         └────┬─────┘
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                        ┌──────────┐   ┌───────────-┐   ┌───────────┐
                        │RowManager│   │IndexManager│   │  Catalog  │
                        └────┬─────┘   └─────┬─────-┘   └───────────┘
                             │               │
                             ▼               ▼
                       ┌──────────┐   ┌──────────┐
                       │TableBTree│   │IndexBTree│
                       └────┬─────┘   └─────┬────┘
                            │               │
                            ▼               ▼
                       ┌─────────────────────────┐
                       │       PageStore         │
                       │  (WAL + page cache)     │
                       └────────────┬────────────┘
                                    │
                          ┌─────────┴─────────┐
                          ▼                   ▼
                    ┌──────────┐        ┌──────────┐
                    │   OPFS   │        │ Node.js  │
                    │ (browser)│        │  (fs)    │
                    └──────────┘        └──────────┘
```

- **Lexer / Parser** — SQL text to AST
- **Binder** — resolves names, types, and constraints against the catalog
- **Optimizer** — rewrites logical plan (predicate pushdown, index selection)
- **Executor** — pull-based iterator model (volcano), executes physical operators
- **RowManager / IndexManager** — logical layer over B-trees, handles row CRUD and index maintenance
- **TableBTree / IndexBTree** — B+ trees for row storage (keyed by rowId) and secondary indexes (composite keys → rowId buckets)
- **PageStore** — page-level I/O with LRU cache and WAL for crash safety
- **Storage backends** — OPFS (synchronous access handle in workers) or Node.js filesystem

## Not Supported (vs SQLite)

| Feature                                                      | Status                       |
| ------------------------------------------------------------ | ---------------------------- |
| Window functions (`OVER`, `PARTITION BY`, `ROW_NUMBER`, ...) | Not supported                |
| `RIGHT JOIN` / `FULL OUTER JOIN`                             | Not supported                |
| Views (`CREATE VIEW`)                                        | Not supported                |
| Triggers                                                     | Not supported                |
| Savepoints                                                   | Not supported                |
| `CHECK` constraints                                          | Not supported                |
| `ATTACH` / `DETACH`                                          | Not supported                |
| `PRAGMA`                                                     | Not supported                |
| Collation                                                    | Not configurable             |
| Type affinity                                                | Strict logical types instead |
